/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.locationtech.geomesa.index.FlushableFeatureWriter
import org.locationtech.geomesa.index.api.{WrappedFeature, _}
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore.VersionKey
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter.FeatureWriterFactory
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.utils.conf.SemanticVersion.MinorOrdering
import org.locationtech.geomesa.utils.conf.{GeoMesaProperties, SemanticVersion}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.{Configs, InternalConfigs}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.CloseWithLogging
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

  lazy val queryPlanner: QueryPlanner[DS, F, W] = createQueryPlanner()

  // abstract methods to be implemented by subclasses

  def manager: GeoMesaIndexManager[DS, F, W]

  protected def featureWriterFactory: FeatureWriterFactory[DS, F, W]

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
    Option(getSchema(typeName)).toSeq.flatMap(sft => manager.indices(sft).flatMap(_.getTableNames(sft, this, None)))

  // hooks to allow extended functionality

  protected def createQueryPlanner(): QueryPlanner[DS, F, W] = new QueryPlanner[DS, F, W](this)

  protected def createFeatureCollection(query: Query, source: GeoMesaFeatureSource): GeoMesaFeatureCollection =
    new GeoMesaFeatureCollection(source, query)

  /**
    * Gets iterator versions as a string. Subclasses with distributed classpaths should override and implement.
    *
    * @return iterator versions
    */
  protected def loadIteratorVersions: Set[String] = Set.empty

  override protected def validateNewSchema(sft: SimpleFeatureType): Unit = {
    import Configs.{TABLE_SPLITTER, TABLE_SPLITTER_OPTS}
    import InternalConfigs.{PARTITION_SPLITTER, PARTITION_SPLITTER_OPTS}

    // for partitioned schemas, disable table sharing and persist the table partitioning keys
    if (TablePartition.partitioned(sft)) {
      Seq((TABLE_SPLITTER, PARTITION_SPLITTER), (TABLE_SPLITTER_OPTS, PARTITION_SPLITTER_OPTS)).foreach {
        case (from, to) => Option(sft.getUserData.get(from)).foreach(sft.getUserData.put(to, _))
      }
      sft.setTableSharing(false)
    }

    super.validateNewSchema(sft)
  }

  // set the enabled indices
  @throws(classOf[IllegalArgumentException])
  override protected def preSchemaCreate(sft: SimpleFeatureType): Unit = manager.setIndices(sft)

  @throws(classOf[IllegalArgumentException])
  override protected def preSchemaUpdate(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    import scala.collection.JavaConverters._

    // verify that attribute index is enabled if necessary
    if (!previous.getAttributeDescriptors.asScala.exists(_.isIndexed) &&
        sft.getAttributeDescriptors.asScala.exists(_.isIndexed) &&
        sft.getIndices.forall(_._1 != AttributeIndex.Name)) {
      val attr = manager.CurrentIndices.find(_.name == AttributeIndex.Name)
      sft.setIndices(sft.getIndices ++ attr.map(i => (i.name, i.version, IndexMode.ReadWrite)))
    }

    // update the configured indices if needed
    val previousIndices = previous.getIndices.map { case (name, version, _) => (name, version) }
    val newIndices = sft.getIndices.filterNot {
      case (name, version, _) => previousIndices.contains((name, version))
    }
    val validatedIndices = newIndices.map { case (name, version, _) =>
      manager.lookup.get(name, version) match {
        case Some(i) if i.supports(sft) => i
        case Some(i) => throw new IllegalArgumentException(s"Index ${i.identifier} does not support this feature type")
        case None => throw new IllegalArgumentException(s"Index $name:$version does not exist")
      }
    }
    // configure the new indices (if not using partitioned tables)
    if (!TablePartition.partitioned(sft)) {
      validatedIndices.foreach(_.configure(sft, this, None))
    }
  }

  // create the index tables (if not using partitioned tables)
  override protected def onSchemaCreated(sft: SimpleFeatureType): Unit = {
    if (!TablePartition.partitioned(sft)) {
      manager.indices(sft).foreach(_.configure(sft, this, None))
    }
  }

  override protected def onSchemaUpdated(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {}

  // delete the index tables
  override protected def onSchemaDeleted(sft: SimpleFeatureType): Unit = {
    manager.indices(sft).par.foreach(_.delete(sft, this, None))
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

      // get the remote version if it's available, but don't wait for it
      GeoMesaDataStore.versions.get(new VersionKey(this)).getNow(Right(None)) match {
        case Right(v) => v.foreach(sft.setRemoteVersion)
        case Left(e)  => throw e
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
    featureWriterFactory.createFeatureWriter(sft, manager.indices(sft, mode = IndexMode.Write), Some(filter))
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
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    if (transaction != Transaction.AUTO_COMMIT) {
      logger.warn("Ignoring transaction - not supported")
    }
    featureWriterFactory.createFeatureWriter(sft, manager.indices(sft, mode = IndexMode.Write), None)
  }

  /**
    * Writes to the specified indices
    *
    * @param typeName feature type name
    * @param indices indices to write
    * @return
    */
  def getIndexWriterAppend(typeName: String, indices: Seq[GeoMesaFeatureIndex[DS, F, W]]): FlushableFeatureWriter = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    featureWriterFactory.createFeatureWriter(sft, indices, None)
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
    * @return client version
    */
  def getClientVersion: SemanticVersion = SemanticVersion(GeoMesaProperties.ProjectVersion, lenient = true)

  /**
    * Gets the geomesa version
    *
    * @return iterator version, if data store has iterators
    */
  def getDistributedVersion: Option[SemanticVersion] = {
    GeoMesaDataStore.versions.get(new VersionKey(this)).get() match {
      case Right(v) => v
      case Left(e)  => throw e
    }
  }

  @deprecated("use getDistributedVersion")
  def getDistributeVersion: Option[SemanticVersion] = getDistributedVersion

  /**
    * Gets the geomesa version
    *
    * @return (client version, iterator version)
    */
  @deprecated("use getClientVersion and getDistributedVersion")
  def getVersion: (String, Set[String]) = (GeoMesaProperties.ProjectVersion, loadIteratorVersions)

  // end public methods
}

object GeoMesaDataStore extends LazyLogging {

  import org.locationtech.geomesa.index.conf.SchemaProperties.{CheckDistributedVersion, ValidateDistributedClasspath}

  private val loader = new CacheLoader[VersionKey[_, _, _], Either[Exception, Option[SemanticVersion]]]() {
    override def load(key: VersionKey[_, _, _]): Either[Exception, Option[SemanticVersion]] = {
      if (key.ds.getTypeNames.length == 0) {
        // short-circuit load - should try again next time cache is accessed
        throw new RuntimeException("Can't load remote versions if there are no feature types")
      }
      if (CheckDistributedVersion.toBoolean.contains(false)) { Right(None) } else {
        val clientVersion = key.ds.getClientVersion
        // use lenient parsing to account for versions like 1.3.5.1
        val iterVersions = key.ds.loadIteratorVersions.map(v => SemanticVersion(v, lenient = true))

        def message: String = "Classpath errors detected: configured server-side iterators do not match " +
            s"client version. Client version: $clientVersion, server versions: ${iterVersions.mkString(", ")}"

        // take the newest one if there are multiple - probably an update went partially awry, so it's
        // likely to match more tablet servers than the lower version
        val version = iterVersions.reduceLeftOption((left, right) => if (right > left) { right } else { left })

        // ensure matching versions
        // return an error if the user has enabled strict checking and it's not a patch/pre-release version mismatch
        // otherwise just log a warning
        if (iterVersions.forall(_ == clientVersion)) {
          Right(version)
        } else if (ValidateDistributedClasspath.toBoolean.contains(false) ||
            iterVersions.forall(MinorOrdering.compare(_, clientVersion) == 0)) {
          logger.warn(message)
          Right(version)
        } else {
          Left(new RuntimeException(s"$message. You may override this check by setting the system property " +
              s"'-D${ValidateDistributedClasspath.property}=false'"))
        }
      }
    }
  }

  private val versions = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.DAYS)
      .buildAsync[VersionKey[_, _, _], Either[Exception, Option[SemanticVersion]]](loader)

  /**
    * Kick off an asynchronous call to load remote iterator versions
    *
    * @param ds datastore
    */
  def initRemoteVersion[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W](ds: GeoMesaDataStore[DS, F, W]): Unit = {
    // can't get remote version if there aren't any tables
    if (ds.getTypeNames.length > 0) {
      versions.get(new VersionKey(ds))
    }
  }

  /**
    * Cache key that bases equality on data store class and catalog, but allows for loading remote version
    * from datastore
    *
    * @param ds data store
    */
  private class VersionKey[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W](val ds: GeoMesaDataStore[DS, F, W]) {

    override def equals(other: Any): Boolean = other match {
      case that: VersionKey[_, _, _] => ds.catalog == that.ds.catalog && ds.getClass == that.ds.getClass
      case _ => false
    }

    override def hashCode(): Int = Seq(ds.catalog, ds.getClass).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
