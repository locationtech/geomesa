/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.{List => jList}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature.{FeatureTypes, NameImpl}
import org.joda.time.DateTimeUtils
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.metadata.HasGeoMesaMetadata
import org.locationtech.geomesa.index.utils.{DistributedLocking, ExplainLogging, Releasable}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.CloseWithLogging
// noinspection ScalaDeprecation
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata._
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.conf.GeoMesaProperties
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.{GeoToolsDateFormat, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.GeoMesaSchemaValidator
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

/**
  * Abstract base class for data store implementations on top of distributed databases
  *
  * @param config common datastore configuration options - subclasses can extend this
  * @tparam DS type of this data store
  * @tparam F wrapper around a simple feature - used for caching write calculations
  * @tparam W write result - feature writers will transform simple features into these
  */
abstract class GeoMesaDataStore[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W](val config: GeoMesaDataStoreConfig)
    extends DataStore with HasGeoMesaMetadata[String] with HasGeoMesaStats with DistributedLocking with LazyLogging {

  this: DS =>

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  private val projectVersionCheck = new AtomicLong(0)

  lazy val queryPlanner = createQueryPlanner()

  // abstract methods to be implemented by subclasses

  def manager: GeoMesaIndexManager[DS, F, W]

  protected def createFeatureWriterAppend(sft: SimpleFeatureType,
                                          indices: Option[Seq[GeoMesaFeatureIndex[DS, F, W]]]): GeoMesaFeatureWriter[DS, F, W, _]

  protected def createFeatureWriterModify(sft: SimpleFeatureType,
                                          indices: Option[Seq[GeoMesaFeatureIndex[DS, F, W]]],
                                          filter: Filter): GeoMesaFeatureWriter[DS, F, W, _]

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

  protected def createQueryPlanner(): QueryPlanner[DS, F, W] = new QueryPlanner(this)

  protected def createFeatureCollection(query: Query, source: GeoMesaFeatureSource): GeoMesaFeatureCollection =
    new GeoMesaFeatureCollection(source, query)

  /**
    * Gets iterator version as a string. Subclasses with distributed classpaths should override and implement.
    *
    * @return iterator version
    */
  protected def getIteratorVersion: String = GeoMesaProperties.ProjectVersion

  // methods from org.geotools.data.DataStore

  /**
   * @see org.geotools.data.DataStore#getTypeNames()
   * @return existing simple feature type names
   */
  override def getTypeNames: Array[String] = metadata.getFeatureTypes

  /**
   * @see org.geotools.data.DataAccess#getNames()
   * @return existing simple feature type names
   */
  override def getNames: jList[Name] = getTypeNames.map(new NameImpl(_)).toList

  /**
   * Compute the GeoMesa SpatioTemporal Schema, create tables, and write metadata to catalog.
   * If the schema already exists, log a message and continue without error.
   * This method uses distributed locking to ensure a schema is only created once.
   *
   * @see org.geotools.data.DataAccess#createSchema(org.opengis.feature.type.FeatureType)
   * @param sft type to create
   */
  override def createSchema(sft: SimpleFeatureType): Unit = {
    if (getSchema(sft.getTypeName) == null) {
      val lock = acquireCatalogLock()
      try {
        // check a second time now that we have the lock
        if (getSchema(sft.getTypeName) == null) {
          // inspect and update the simple feature type for various components
          // do this before anything else so that any modifications will be in place
          GeoMesaSchemaValidator.validate(sft)

          // write out the metadata to the catalog table
          writeMetadata(sft)

          // reload the sft so that we have any default metadata,
          // then copy over any additional keys that were in the original sft
          val reloadedSft = getSchema(sft.getTypeName)
          (sft.getUserData.keySet -- reloadedSft.getUserData.keySet)
              .foreach(k => reloadedSft.getUserData.put(k, sft.getUserData.get(k)))

          // create the tables in accumulo
          manager.indices(reloadedSft, IndexMode.Any).foreach(_.configure(reloadedSft, this))
        }
      } finally {
        lock.release()
      }
    }
  }

  /**
    * @see org.geotools.data.DataAccess#getSchema(org.opengis.feature.type.Name)
    * @param name feature type name
    * @return feature type, or null if it does not exist
    */
  override def getSchema(name: Name): SimpleFeatureType = getSchema(name.getLocalPart)

  /**
   * @see org.geotools.data.DataStore#getSchema(java.lang.String)
   * @param typeName feature type name
   * @return feature type, or null if it does not exist
   */
  override def getSchema(typeName: String): SimpleFeatureType = {
    val sftOpt = metadata.read(typeName, ATTRIBUTES_KEY).map(SimpleFeatureTypes.createType(typeName, _))

    sftOpt.foreach { sft =>
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

    sftOpt.orNull
  }

  /**
    * Allows the following modifications to the schema:
    *   modifying keywords through user-data
    *   enabling/disabling indices through RichSimpleFeatureType.setIndexVersion (SimpleFeatureTypes.INDEX_VERSIONS)
    *   appending of new attributes
    *
    * Other modifications are not supported.
    *
    * @see org.geotools.data.DataStore#updateSchema(java.lang.String, org.opengis.feature.simple.SimpleFeatureType)
    * @param typeName simple feature type name
    * @param sft new simple feature type
    */
  override def updateSchema(typeName: String, sft: SimpleFeatureType): Unit =
    updateSchema(new NameImpl(typeName), sft)

  /**
    * Allows the following modifications to the schema:
    *   modifying keywords through user-data
    *   enabling/disabling indices through RichSimpleFeatureType.setIndexVersion (SimpleFeatureTypes.INDEX_VERSIONS)
    *   appending of new attributes
    *
    * Other modifications are not supported.
    *
    * @see org.geotools.data.DataAccess#updateSchema(org.opengis.feature.type.Name, org.opengis.feature.type.FeatureType)
    * @param typeName simple feature type name
    * @param sft new simple feature type
    */
  override def updateSchema(typeName: Name, sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs._
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.InternalConfigs._

    // validate type name has not changed
    if (typeName.getLocalPart != sft.getTypeName) {
      val msg = s"Updating the name of a schema is not allowed: '$typeName' changed to '${sft.getTypeName}'"
      throw new UnsupportedOperationException(msg)
    }

    val lock = acquireCatalogLock()
    try {
      // Get previous schema and user data
      val previousSft = getSchema(typeName)

      if (previousSft == null) {
        throw new IllegalArgumentException(s"Schema '$typeName' does not exist")
      }

      // validate that default geometry has not changed
      if (sft.getGeomField != previousSft.getGeomField) {
        throw new UnsupportedOperationException("Changing the default geometry is not supported")
      }

      // Check that unmodifiable user data has not changed
      val unmodifiableUserdataKeys =
        Set(SCHEMA_VERSION_KEY, TABLE_SHARING_KEY, SHARING_PREFIX_KEY, DEFAULT_DATE_KEY, ST_INDEX_SCHEMA_KEY)

      unmodifiableUserdataKeys.foreach { key =>
        if (sft.userData[Any](key) != previousSft.userData[Any](key)) {
          throw new UnsupportedOperationException(s"Updating '$key' is not supported")
        }
      }

      // Check that the rest of the schema has not changed (columns, types, etc)
      val previousColumns = previousSft.getAttributeDescriptors
      val currentColumns = sft.getAttributeDescriptors
      if (previousColumns.toSeq != currentColumns.take(previousColumns.length)) {
        throw new UnsupportedOperationException("Updating schema columns is not allowed")
      }

      // update the configured indices if needed
      val previousIndices = previousSft.getIndices.map { case (name, version, _) => (name, version)}
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

      // If all is well, update the metadata
      val attributesValue = SimpleFeatureTypes.encodeType(sft, includeUserData = true)
      metadata.insert(sft.getTypeName, ATTRIBUTES_KEY, attributesValue)
    } finally {
      lock.release()
    }
  }

  /**
   * Deletes all features from the accumulo index tables and deletes metadata from the catalog.
   * If the feature type shares tables with another, this is fairly expensive,
   * proportional to the number of features. Otherwise, it is fairly cheap.
   *
   * @see org.geotools.data.DataStore#removeSchema(java.lang.String)
   * @param typeName simple feature type name
   */
  override def removeSchema(typeName: String): Unit = {
    val lock = acquireCatalogLock()
    try {
      Option(getSchema(typeName)).foreach { sft =>
        val shared = sft.isTableSharing && getTypeNames.filter(_ != typeName).map(getSchema).exists(_.isTableSharing)
        manager.indices(sft, IndexMode.Any).par.foreach(_.delete(sft, this, shared))
        stats.clearStats(sft)
        metadata.delete(typeName)
      }
    } finally {
      lock.release()
    }
  }

  /**
   * @see org.geotools.data.DataAccess#removeSchema(org.opengis.feature.type.Name)
   * @param typeName simple feature type name
   */
  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  /**
   * @see org.geotools.data.DataStore#getFeatureSource(org.opengis.feature.type.Name)
   * @param typeName simple feature type name
   * @return featureStore, suitable for reading and writing
   */
  override def getFeatureSource(typeName: Name): GeoMesaFeatureStore = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    if (config.caching) {
      new GeoMesaFeatureStore(this, sft, createFeatureCollection) with CachingFeatureSource
    } else {
      new GeoMesaFeatureStore(this, sft, createFeatureCollection)
    }
  }

  /**
   * @see org.geotools.data.DataStore#getFeatureSource(java.lang.String)
   * @param typeName simple feature type name
   * @return featureStore, suitable for reading and writing
   */
  override def getFeatureSource(typeName: String): GeoMesaFeatureStore = getFeatureSource(new NameImpl(typeName))

  /**
   * @see org.geotools.data.DataStore#getFeatureReader(org.geotools.data.Query, org.geotools.data.Transaction)
   * @param query query to execute
   * @param transaction transaction to use (currently ignored)
   * @return feature reader
   */
  override def getFeatureReader(query: Query, transaction: Transaction): GeoMesaFeatureReader = {
    val sft = getSchema(query.getTypeName)
    if (sft == null) {
      throw new IOException(s"Schema '${query.getTypeName}' has not been initialized. Please call 'createSchema' first.")
    }
    GeoMesaFeatureReader(sft, query, queryPlanner, config.queryTimeout, config.audit)
  }

  /**
   * Create a general purpose writer that is capable of updates and deletes.
   * Does <b>not</b> allow inserts. Will return all existing features.
   *
   * @see org.geotools.data.DataStore#getFeatureWriter(java.lang.String, org.geotools.data.Transaction)
   * @param typeName feature type name
   * @param transaction transaction (currently ignored)
   * @return feature writer
   */
  override def getFeatureWriter(typeName: String, transaction: Transaction): GeoMesaFeatureWriter[DS, F, W, _] =
    getFeatureWriter(typeName, Filter.INCLUDE, transaction)

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
  override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): GeoMesaFeatureWriter[DS, F, W, _] = {
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
  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): GeoMesaFeatureWriter[DS, F, W, _] = {
    if (transaction != Transaction.AUTO_COMMIT) {
      logger.warn("Ignoring transaction - not supported")
    }
    getIndexWriterAppend(typeName, null)
  }

  def getIndexWriterAppend(typeName: String,
                           indices: Seq[GeoMesaFeatureIndex[DS, F, W]]): GeoMesaFeatureWriter[DS, F, W, _] = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    createFeatureWriterAppend(sft, Option(indices))
  }

  /**
   * @see org.geotools.data.DataAccess#getInfo()
   * @return service info
   */
  override def getInfo: ServiceInfo = {
    val info = new DefaultServiceInfo()
    info.setDescription(s"Features from ${getClass.getSimpleName}")
    info.setSchema(FeatureTypes.DEFAULT_NAMESPACE)
    info
  }

  /**
   * We always return null, which indicates that we are handling transactions ourselves.
   *
   * @see org.geotools.data.DataStore#getLockingManager()
   * @return locking manager - null
   */
  override def getLockingManager: LockingManager = null

  /**
   * Cleanup any open connections, etc. Equivalent to java.io.Closeable.close()
   *
   * @see org.geotools.data.DataAccess#dispose()
   */
  override def dispose(): Unit = {
    CloseWithLogging(metadata)
    CloseWithLogging(stats)
    config.audit.foreach { case (writer, _, _) => CloseWithLogging(writer) }
  }

  // end methods from org.geotools.data.DataStore

  // other public methods

  /**
   * Gets the query plan for a given query. The query plan consists of the tables, ranges, iterators etc
   * required to run a query against accumulo.
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
  def getVersion: (String, String) = (GeoMesaProperties.ProjectVersion, getIteratorVersion)

  // end public methods

  /**
    * Acquires a distributed lock for all accumulo data stores sharing this catalog table.
    * Make sure that you 'release' the lock in a finally block.
    */
  protected def acquireCatalogLock(): Releasable = {
    val path = s"/org.locationtech.geomesa/ds/${config.catalog}"
    acquireDistributedLock(path, 120000).getOrElse {
      throw new RuntimeException(s"Could not acquire distributed lock at '$path'")
    }
  }

  /**
   * Computes and writes the metadata for this feature type
   */
  private def writeMetadata(sft: SimpleFeatureType) {
    // determine the schema ID - ensure that it is unique in this catalog
    // IMPORTANT: this method needs to stay inside a zookeeper distributed locking block
    var schemaId = 1
    val existingSchemaIds = getTypeNames.flatMap(metadata.read(_, SCHEMA_ID_KEY, cache = false)
        .map(_.getBytes(StandardCharsets.UTF_8).head.toInt))
    // noinspection ExistsEquals
    while (existingSchemaIds.exists(_ == schemaId)) { schemaId += 1 }
    // We use a single byte for the row prefix to save space - if we exceed the single byte limit then
    // our ranges would start to overlap and we'd get errors
    require(schemaId <= Byte.MaxValue, s"No more than ${Byte.MaxValue} schemas may share a single catalog table")
    val schemaIdString = new String(Array(schemaId.asInstanceOf[Byte]), StandardCharsets.UTF_8)

    // set user data so that it gets persisted
    if (sft.isTableSharing) {
      sft.setTableSharing(true) // explicitly set it in case this was just the default
      sft.setTableSharingPrefix(schemaIdString)
    }

    // set the enabled indices
    manager.setIndices(sft)

    // compute the metadata values - IMPORTANT: encode type has to be called after all user data is set
    val attributesValue   = SimpleFeatureTypes.encodeType(sft, includeUserData = true)
    val statDateValue     = GeoToolsDateFormat.print(DateTimeUtils.currentTimeMillis())

    // store each metadata in the associated key
    val metadataMap = Map(
      ATTRIBUTES_KEY        -> attributesValue,
      STATS_GENERATION_KEY  -> statDateValue,
      SCHEMA_ID_KEY         -> schemaIdString
    )

    metadata.insert(sft.getTypeName, metadataMap)
  }

  /**
    * Checks that the distributed runtime jar matches the project version of this client. We cache
    * successful checks for 10 minutes.
    */
  private def checkProjectVersion(): Unit = {
    if (projectVersionCheck.get() < System.currentTimeMillis()) {
      val (clientVersion, iteratorVersion) = getVersion
      if (iteratorVersion != clientVersion) {
        val versionMsg = "Configured server-side iterators do not match client version - " +
            s"client version: $clientVersion, server version: $iteratorVersion"
        logger.warn(versionMsg)
      }
      projectVersionCheck.set(System.currentTimeMillis() + 3600000) // 1 hour
    }
  }
}
