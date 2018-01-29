/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.io.IOException
import java.util.concurrent.atomic.AtomicLong

import org.geotools.data._
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter.FlushableFeatureWriter
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.index.utils.ExplainLogging
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.CloseWithLogging
// noinspection ScalaDeprecation
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.conf.GeoMesaProperties
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Abstract base class for data store implementations on top of distributed databases
  *
  * @param config common datastore configuration options - subclasses can extend this
  * @tparam DS type of this data store
  * @tparam F wrapper around a simple feature - used for caching write calculations
  * @tparam W write result - feature writers will transform simple features into these
  */
abstract class GeoMesaDataStore[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W](val config: GeoMesaDataStoreConfig)
    extends MetadataBackedDataStore(config) with HasGeoMesaStats {

  this: DS =>

  private val projectVersionCheck = new AtomicLong(0)

  lazy val queryPlanner: QueryPlanner[DS, F, W] = createQueryPlanner()

  // abstract methods to be implemented by subclasses

  def manager: GeoMesaIndexManager[DS, F, W]

  /**
    * @see `GeoMesaFeatureWriter[DS, F, W, _]` for base implementation
    *
    * @param sft simple feature type
    * @param indices indices to write to
    * @return
    */
  protected def createFeatureWriterAppend(sft: SimpleFeatureType,
                                          indices: Option[Seq[GeoMesaFeatureIndex[DS, F, W]]]): FlushableFeatureWriter

  /**
    * See `GeoMesaFeatureWriter[DS, F, W, _]` for base implementation
    *
    * @param sft simple feature type
    * @param indices indices to write to
    * @param filter features to modify
    * @return
    */
  protected def createFeatureWriterModify(sft: SimpleFeatureType,
                                          indices: Option[Seq[GeoMesaFeatureIndex[DS, F, W]]],
                                          filter: Filter): FlushableFeatureWriter

  override protected def catalog: String = config.catalog

  /**
    * Optimized method to delete everything (all tables) associated with this datastore
    * (index tables and catalog table)
    * NB: We are *not* currently deleting the query table and/or query information.
    */
  def delete(): Unit

  /**
    * Returns all tables that may be created for the simple feature type. Note that some of these
    * tables may be shared with other simple feature types, and the tables may not all currently exist.
    *
    * @param typeName simple feature type name
    * @return
    */
  def getAllTableNames(typeName: String): Seq[String] = Seq(config.catalog) ++ getAllIndexTableNames(typeName)

  /**
    * Returns all index tables that may be created for the simple feature type. Note that some of these
    * tables may be shared with other simple feature types, and the tables may not all currently exist.
    *
    * @param typeName simple feature type name
    * @return
    */
  def getAllIndexTableNames(typeName: String): Seq[String] =
    Option(getSchema(typeName)).toSeq.flatMap(manager.indices(_, IndexMode.Any).map(_.getTableName(typeName, this)))

  // hooks to allow extended functionality

  protected def createQueryPlanner(): QueryPlanner[DS, F, W] = new QueryPlanner[DS, F, W](this)

  protected def createFeatureCollection(query: Query, source: GeoMesaFeatureSource): GeoMesaFeatureCollection =
    new GeoMesaFeatureCollection(source, query)

  /**
    * Gets iterator version as a string. Subclasses with distributed classpaths should override and implement.
    *
    * @return iterator version
    */
  protected def getIteratorVersion: Set[String] = Set.empty


  // set the enabled indices
  @throws(classOf[IllegalArgumentException])
  override protected def preSchemaCreate(sft: SimpleFeatureType): Unit = manager.setIndices(sft)

  @throws(classOf[IllegalArgumentException])
  override protected def preSchemaUpdate(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
    // update the configured indices if needed
    val previousIndices = previous.getIndices.map { case (name, version, _) => (name, version)}
    val newIndices = sft.getIndices.filterNot {
      // noinspection ExistsEquals
      case (name, version, _) => previousIndices.exists(_ == (name, version))
    }
    val validatedIndices = newIndices.map { case (name, version, _) =>
      manager.lookup.get(name, version) match {
        case Some(i) if i.supports(sft) => i
        case Some(i) => throw new IllegalArgumentException(s"Index ${i.identifier} does not support this feature type")
        case None => throw new IllegalArgumentException(s"Index $name:$version does not exist")
      }
    }
    // configure the new indices
    validatedIndices.foreach(_.configure(sft, this))
  }

  // create the index tables
  override protected def onSchemaCreated(sft: SimpleFeatureType): Unit =
    manager.indices(sft, IndexMode.Any).foreach(_.configure(sft, this))

  override protected def onSchemaUpdated(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {}

  // delete the index tables
  override protected def onSchemaDeleted(sft: SimpleFeatureType): Unit = {
    val shared = sft.isTableSharing && getTypeNames.filter(_ != sft.getTypeName).map(getSchema).exists(_.isTableSharing)
    manager.indices(sft, IndexMode.Any).par.foreach(_.delete(sft, this, shared))
    stats.clearStats(sft)
  }

  // methods from org.geotools.data.DataStore

  /**
   * @see org.geotools.data.DataStore#getSchema(java.lang.String)
   * @param typeName feature type name
   * @return feature type, or null if it does not exist
   */
  override def getSchema(typeName: String): SimpleFeatureType = {
    val sft = super.getSchema(typeName)
    if (sft != null) {
      checkProjectVersion()

      val missingIndices = sft.getIndices.filterNot { case (n, v, _) =>
        manager.AllIndices.exists(i => i.name == n && i.version == v)
      }

      if (missingIndices.nonEmpty) {
        val versions = missingIndices.map { case (n, v, _) => s"$n:$v" }.mkString(",")
        val available = manager.AllIndices.map(i => s"${i.name}:${i.version}").mkString(",")
        logger.error(s"Trying to access schema ${sft.getTypeName} with invalid index versions '$versions' - " +
            s"available indices are '$available'")
        throw new IllegalStateException(s"The schema ${sft.getTypeName} was written with a newer " +
            "version of GeoMesa than this client can handle. Please ensure that you are using the " +
            "same GeoMesa jar versions across your entire workflow. For more information, see " +
            "http://www.geomesa.org/documentation/user/installation_and_configuration.html#upgrading")
      }
    }

    sft
  }

  /**
   * @see org.geotools.data.DataStore#getFeatureSource(java.lang.String)
   * @param typeName simple feature type name
   * @return featureStore, suitable for reading and writing
   */
  override def getFeatureSource(typeName: String): GeoMesaFeatureStore = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    if (config.caching) {
      new GeoMesaFeatureStore(this, sft, queryPlanner, createFeatureCollection) with CachingFeatureSource
    } else {
      new GeoMesaFeatureStore(this, sft, queryPlanner, createFeatureCollection)
    }
  }

  /**
   * @see org.geotools.data.DataStore#getFeatureReader(org.geotools.data.Query, org.geotools.data.Transaction)
   * @param query query to execute
   * @param transaction transaction to use (currently ignored)
   * @return feature reader
   */
  override def getFeatureReader(query: Query, transaction: Transaction): GeoMesaFeatureReader = {
    require(query.getTypeName != null, "Type name is required in the query")
    val sft = getSchema(query.getTypeName)
    if (sft == null) {
      throw new IOException(s"Schema '${query.getTypeName}' has not been initialized. Please call 'createSchema' first.")
    }
    GeoMesaFeatureReader(sft, query, queryPlanner, config.queryTimeout, config.audit)
  }

  /**
   * Create a general purpose writer that is capable of updates and deletes.
   * Does <b>not</b> allow inserts.
   *
   * @see org.geotools.data.DataStore#getFeatureWriter(java.lang.String, org.opengis.filter.Filter,
   *        org.geotools.data.Transaction)
   * @param typeName feature type name
   * @param filter cql filter to select features for update/delete
   * @param transaction transaction (currently ignored)
   * @return feature writer
   */
  override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): FlushableFeatureWriter = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    if (transaction != Transaction.AUTO_COMMIT) {
      logger.warn("Ignoring transaction - not supported")
    }
    createFeatureWriterModify(sft, None, filter)
  }

  /**
   * Creates a feature writer only for writing - does not allow updates or deletes.
   *
   * @see org.geotools.data.DataStore#getFeatureWriterAppend(java.lang.String, org.geotools.data.Transaction)
   * @param typeName feature type name
   * @param transaction transaction (currently ignored)
   * @return feature writer
   */
  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): FlushableFeatureWriter = {
    if (transaction != Transaction.AUTO_COMMIT) {
      logger.warn("Ignoring transaction - not supported")
    }
    getIndexWriterAppend(typeName, null)
  }

  def getIndexWriterAppend(typeName: String,
                           indices: Seq[GeoMesaFeatureIndex[DS, F, W]]): FlushableFeatureWriter = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    createFeatureWriterAppend(sft, Option(indices))
  }

  /**
   * Cleanup any open connections, etc. Equivalent to java.io.Closeable.close()
   *
   * @see org.geotools.data.DataAccess#dispose()
   */
  override def dispose(): Unit = {
    CloseWithLogging(stats)
    config.audit.foreach { case (writer, _, _) => CloseWithLogging(writer) }
    super.dispose()
  }

  // end methods from org.geotools.data.DataStore

  // other public methods

  /**
   * Gets the query plan for a given query. The query plan consists of the tables, ranges, iterators etc
   * required to run a query against the data store.
   *
   * @param query query to execute
   * @param index hint on the index to use to satisfy the query
   * @return query plans
   */
  def getQueryPlan(query: Query,
                   index: Option[GeoMesaFeatureIndex[DS, F, W]] = None,
                   explainer: Explainer = new ExplainLogging): Seq[QueryPlan[DS, F, W]] = {
    require(query.getTypeName != null, "Type name is required in the query")
    val sft = getSchema(query.getTypeName)
    if (sft == null) {
      throw new IOException(s"Schema '${query.getTypeName}' has not been initialized. Please call 'createSchema' first.")
    }
    queryPlanner.planQuery(sft, query, index, explainer)
  }

  /**
    * Gets the geomesa version
    *
    * @return (client version, iterator version)
    */
  def getVersion: (String, Set[String]) = (GeoMesaProperties.ProjectVersion, getIteratorVersion)

  // end public methods

  /**
    * Checks that the distributed runtime jar matches the project version of this client. We cache
    * successful checks for 10 minutes.
    */
  private def checkProjectVersion(): Unit = {
    if (projectVersionCheck.get() > System.currentTimeMillis()) {
      projectVersionCheck.set(System.currentTimeMillis() + 3600000) // 1 hour
      val (clientVersion, iteratorVersions) = getVersion
      if (iteratorVersions.exists(_ != clientVersion)) {
        val versionMsg = "Configured server-side iterators do not match client version - " +
            s"client version: $clientVersion, server versions: ${iteratorVersions.mkString(", ")}"
        logger.warn(versionMsg)
      }
    }
  }
}
