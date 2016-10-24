/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.data

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.{List => jList}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature.{FeatureTypes, NameImpl}
import org.joda.time.DateTimeUtils
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties
import org.locationtech.geomesa.accumulo.data.GeoMesaMetadata._
import org.locationtech.geomesa.accumulo.data.stats._
import org.locationtech.geomesa.accumulo.data.stats.usage.{GeoMesaUsageStats, GeoMesaUsageStatsImpl, HasGeoMesaUsageStats}
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.index.attribute.{AttributeIndex, AttributeSplittable}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
// noinspection ScalaDeprecation
import org.locationtech.geomesa.accumulo.index.geohash.GeoHashIndex
import org.locationtech.geomesa.accumulo.index.id.RecordIndex
import org.locationtech.geomesa.accumulo.iterators.ProjectVersionIterator
import org.locationtech.geomesa.accumulo.util.{DistributedLocking, Releasable}
import org.locationtech.geomesa.features.SerializationType
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.security.{AuditProvider, AuthorizationsProvider}
import org.locationtech.geomesa.utils.conf.GeoMesaProperties
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.{GeoToolsDateFormat, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.GeoMesaSchemaValidator
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.util.control.NonFatal


/**
 * This class handles DataStores which are stored in Accumulo Tables. To be clear, one table may
 * contain multiple features addressed by their featureName.
 *
 * @param connector Accumulo connector
 * @param catalogTable Table name in Accumulo to store metadata about featureTypes. For pre-catalog
 *                     single-table stores this equates to the spatiotemporal table name
 * @param authProvider Provides the authorizations used to access data
 * @param auditProvider Provides access to the current user for auditing purposes
 * @param defaultVisibilities default visibilities applied to any data written
 * @param config configuration values
 */
class AccumuloDataStore(val connector: Connector,
                        val catalogTable: String,
                        val authProvider: AuthorizationsProvider,
                        val auditProvider: AuditProvider,
                        val defaultVisibilities: String,
                        val config: AccumuloDataStoreConfig)
    extends DataStore with AccumuloConnectorCreator with DistributedLocking
      with HasGeoMesaMetadata[String] with HasGeoMesaStats with HasGeoMesaUsageStats with LazyLogging {

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  private val queryTimeoutMillis: Option[Long] = config.queryTimeout
      .orElse(GeomesaSystemProperties.QueryProperties.QUERY_TIMEOUT_MILLIS.option.map(_.toLong))

  private val statsTable = GeoMesaTable.concatenateNameParts(catalogTable, "stats")
  private val usageStatsTable = GeoMesaTable.concatenateNameParts(catalogTable, "queries")

  private val projectVersionCheck = new AtomicLong(0)

  val tableOps = connector.tableOperations()

  override val metadata: GeoMesaMetadata[String] =
    new MultiRowAccumuloMetadata(connector, catalogTable, MetadataStringSerializer)
  private val oldMetadata = new SingleRowAccumuloMetadata(connector, catalogTable, MetadataStringSerializer)

  override val stats: GeoMesaStats = new GeoMesaMetadataStats(this, statsTable, config.generateStats)
  override val usageStats: GeoMesaUsageStats =
    new GeoMesaUsageStatsImpl(connector, usageStatsTable, config.collectUsageStats)


  // methods from org.geotools.data.DataStore

  /**
   * @see org.geotools.data.DataStore#getTypeNames()
   * @return existing simple feature type names
   */
  override def getTypeNames: Array[String] = metadata.getFeatureTypes ++ oldMetadata.getFeatureTypes

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

          // TODO GEOMESA-1322 support tilde in feature name
          if (sft.getTypeName.contains("~")) {
            throw new IllegalArgumentException("AccumuloDataStore does not currently support '~' in feature type names")
          }

          // write out the metadata to the catalog table
          writeMetadata(sft)

          // reload the sft so that we have any default metadata,
          // then copy over any additional keys that were in the original sft
          val reloadedSft = getSchema(sft.getTypeName)
          (sft.getUserData.keySet -- reloadedSft.getUserData.keySet)
              .foreach(k => reloadedSft.getUserData.put(k, sft.getUserData.get(k)))

          // create the tables in accumulo
          AccumuloFeatureIndex.indices(reloadedSft, IndexMode.Any).foreach(_.configure(reloadedSft, this))
        }
      } finally {
        lock.release()
      }
    }
  }

  /**
   * @see org.geotools.data.DataStore#getSchema(java.lang.String)
   * @param typeName feature type name
   * @return feature type, or null if it does not exist
   */
  override def getSchema(typeName: String): SimpleFeatureType = getSchema(new NameImpl(typeName))

  /**
   * @see org.geotools.data.DataAccess#getSchema(org.opengis.feature.type.Name)
   * @param name feature type name
   * @return feature type, or null if it does not exist
   */
  override def getSchema(name: Name): SimpleFeatureType = {
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.{ENABLED_INDEX_OPTS, ENABLED_INDICES}
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.InternalConfigs.{INDEX_VERSIONS, SCHEMA_VERSION_KEY}

    val typeName = name.getLocalPart
    val attributes = metadata.read(typeName, ATTRIBUTES_KEY).orElse {
      // check for old-style metadata and re-write it if necessary
      if (oldMetadata.read(typeName, ATTRIBUTES_KEY, cache = false).isDefined) {
        val lock = acquireFeatureLock(typeName)
        try {
          if (oldMetadata.read(typeName, ATTRIBUTES_KEY, cache = false).isDefined) {
            metadata.asInstanceOf[MultiRowAccumuloMetadata[String]].migrate(typeName)
            new MultiRowAccumuloMetadata[Any](connector, statsTable, null).migrate(typeName)
          }
        } finally {
          lock.release()
        }
        metadata.read(typeName, ATTRIBUTES_KEY)
      } else {
        None
      }
    }

    attributes.map { attributes =>
      val sft = SimpleFeatureTypes.createType(name.getURI, attributes)

      checkProjectVersion()

      // back compatible check for index versions
      if (!sft.getUserData.contains(INDEX_VERSIONS)) {
        // back compatible check if user data wasn't encoded with the sft
        if (!sft.getUserData.containsKey(SCHEMA_VERSION_KEY)) {
          metadata.read(typeName, "dtgfield").foreach(sft.setDtgField)
          sft.getUserData.put(SCHEMA_VERSION_KEY, metadata.readRequired(typeName, VERSION_KEY))

          // If no data is written, we default to 'false' in order to support old tables.
          if (metadata.read(typeName, "tables.sharing").exists(_.toBoolean)) {
            sft.setTableSharing(true)
            // use schema id if available or fall back to old type name for backwards compatibility
            val prefix = metadata.read(typeName, SCHEMA_ID_KEY).getOrElse(s"${sft.getTypeName}~")
            sft.setTableSharingPrefix(prefix)
          } else {
            sft.setTableSharing(false)
            sft.setTableSharingPrefix("")
          }
          ENABLED_INDEX_OPTS.foreach { i =>
            metadata.read(typeName, i).foreach(e => sft.getUserData.put(ENABLED_INDICES, e))
          }
          // old st_idx schema, kept around for back-compatibility
          metadata.read(typeName, "schema").foreach(sft.setStIndexSchema)
        }

        // set the enabled indices
        sft.setIndices(AccumuloDataStore.getEnabledIndices(sft))
      }

      val missingIndices = sft.getIndices.filterNot { case (n, v, _) =>
        AccumuloFeatureIndex.AllIndices.exists(i => i.name == n && i.version == v)
      }
      lazy val error = new IllegalStateException(s"The schema ${sft.getTypeName} was written with a newer " +
          "version of GeoMesa than this client can handle. Please ensure that you are using the " +
          "same GeoMesa jar versions across your entire workflow. For more information, see " +
          "http://www.geomesa.org/documentation/user/installation_and_configuration.html#upgrading")

      if (missingIndices.nonEmpty) {
        val versions = missingIndices.map { case (n, v, _) => s"$n:$v" }.mkString(",")
        val available = AccumuloFeatureIndex.AllIndices.map(i => s"${i.name}:${i.version}").mkString(",")
        logger.error(s"Trying to access schema ${sft.getTypeName} with invalid index versions '$versions' - " +
            s"available indices are '$available'")
        throw error
      }

      // back compatibility check for stat configuration
      if (config.generateStats && metadata.read(typeName, STATS_GENERATION_KEY).isEmpty) {
        // configure the stats combining iterator - we only use this key for older data stores
        val configuredKey = "stats-configured"
        if (!metadata.read(typeName, configuredKey).exists(_ == "true")) {
          val lock = acquireCatalogLock()
          try {
            if (!metadata.read(typeName, configuredKey, cache = false).exists(_ == "true")) {
              GeoMesaMetadataStats.configureStatCombiner(connector, statsTable, sft)
              metadata.insert(typeName, configuredKey, "true")
            }
          } finally {
            lock.release()
          }
        }
        // kick off asynchronous stats run for the existing data
        // this may get triggered more than once, but should only run one time
        val statsRunner = new StatsRunner(this)
        statsRunner.submit(sft)
        statsRunner.close()
      }

      sft
    }.orNull
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
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs._
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.InternalConfigs._

    // validate type name has not changed
    if (typeName.toString != sft.getTypeName) {
      val msg = s"Updating the name of a schema is not allowed: '$typeName' changed to '${sft.getTypeName}'"
      throw new UnsupportedOperationException(msg)
    }

    val lock = acquireFeatureLock(sft.getTypeName)
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
        case (name, version, _) => previousIndices.exists(_ == (name, version))
      }
      val validatedIndices = newIndices.map { case (name, version, _) =>
        AccumuloFeatureIndex.IndexLookup.get(name, version) match {
          case Some(i) if i.supports(sft) => i
          case Some(i) => throw new IllegalArgumentException(s"Index ${i.identifier} does not support this feature type")
          case None => throw new IllegalArgumentException(s"Index $name:$version does not exist")
        }
      }
      // configure the new indices
      validatedIndices.foreach(_.configure(sft, this))

      // check for newly indexed attributes and re-configure the splits
      val previousAttrIndices = previousSft.getAttributeDescriptors.collect {
        case d if d.isIndexed => d.getLocalName
      }
      if (sft.getAttributeDescriptors.exists(d => d.isIndexed && !previousAttrIndices.contains(d.getLocalName))) {
        AccumuloFeatureIndex.indices(sft, IndexMode.Any).foreach {
          case s: AttributeSplittable => s.configureSplits(sft, this)
          case _ => // no-op
        }
      }

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
  override def removeSchema(typeName: String) = {
    val typeLock = acquireFeatureLock(typeName)
    try {
      Option(getSchema(typeName)).foreach { sft =>
        if (sft.isTableSharing && getTypeNames.filter(_ != typeName).map(getSchema).exists(_.isTableSharing)) {
          deleteSharedTables(sft)
        } else {
          deleteStandAloneTables(sft)
        }
        stats.clearStats(sft)
      }
    } finally {
      typeLock.release()
    }
    val catalogLock = acquireCatalogLock()
    try {
      metadata.delete(typeName)
    } finally {
      catalogLock.release()
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
  override def getFeatureSource(typeName: Name): AccumuloFeatureStore = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    if (config.caching) {
      new AccumuloFeatureStore(this, sft) with CachingFeatureSource
    } else {
      new AccumuloFeatureStore(this, sft)
    }
  }

  /**
   * @see org.geotools.data.DataStore#getFeatureSource(java.lang.String)
   * @param typeName simple feature type name
   * @return featureStore, suitable for reading and writing
   */
  override def getFeatureSource(typeName: String): AccumuloFeatureStore =
    getFeatureSource(new NameImpl(typeName))

  /**
   * @see org.geotools.data.DataStore#getFeatureReader(org.geotools.data.Query, org.geotools.data.Transaction)
   * @param query query to execute
   * @param transaction transaction to use (currently ignored)
   * @return feature reader
   */
  override def getFeatureReader(query: Query, transaction: Transaction): AccumuloFeatureReader = {
    val qp = getQueryPlanner(query.getTypeName)
    val stats = if (config.collectUsageStats) Some((usageStats, auditProvider)) else None
    AccumuloFeatureReader(query, qp, queryTimeoutMillis, stats)
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
  override def getFeatureWriter(typeName: String, transaction: Transaction): AccumuloFeatureWriter =
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
  override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): AccumuloFeatureWriter = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    new ModifyAccumuloFeatureWriter(sft, this, defaultVisibilities, filter)
  }

  /**
   * Creates a feature writer only for writing - does not allow updates or deletes.
   *
   * @see org.geotools.data.DataStore#getFeatureWriterAppend(java.lang.String, org.geotools.data.Transaction)
   * @param typeName feature type name
   * @param transaction transaction (currently ignored)
   * @return feature writer
   */
  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): AccumuloFeatureWriter = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    new AppendAccumuloFeatureWriter(sft, this, defaultVisibilities)
  }

  /**
   * @see org.geotools.data.DataAccess#getInfo()
   * @return service info
   */
  override def getInfo: ServiceInfo = {
    val info = new DefaultServiceInfo()
    info.setDescription("Features from AccumuloDataStore")
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
    stats.close()
    usageStats.close()
  }

  // end methods from org.geotools.data.DataStore

  // methods from AccumuloConnectorCreator

  /**
   * @see org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator#getScanner(java.lang.String)
   * @param table table to scan
   * @return scanner
   */
  override def getScanner(table: String): Scanner =
    connector.createScanner(table, authProvider.getAuthorizations)

  /**
   * @see org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator#getBatchScanner(java.lang.String, int)
   * @param table table to scan
   * @param threads number of threads to use in scanning
   * @return batch scanner
   */
  override def getBatchScanner(table: String, threads: Int): BatchScanner =
    connector.createBatchScanner(table, authProvider.getAuthorizations, threads)

  /**
   * @see org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator#getTableName(java.lang.String,
   *          org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable)
   * @param featureName feature type name
   * @param index table
   * @return accumulo table name
   */
  // noinspection ScalaDeprecation
  override def getTableName(featureName: String, index: AccumuloWritableIndex): String = {
    lazy val oldKey = index match {
      case i if i.name == RecordIndex.name    => "tables.record.name"
      case i if i.name == AttributeIndex.name => "tables.idx.attr.name"
      case GeoHashIndex => "tables.idx.st.name"
      case i => s"tables.${i.name}.name"
    }
    metadata.read(featureName, index.tableNameKey).orElse(metadata.read(featureName, oldKey)).getOrElse {
      throw new RuntimeException(s"Could not read table name from metadata for index ${index.name}:${index.version}")
    }
  }

  /**
   * @see org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator#getSuggestedThreads(java.lang.String,
   *          org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable)
   * @param featureName feature type name
   * @param index table
   * @return threads to use in scanning
   */
  override def getSuggestedThreads(featureName: String, index: AccumuloFeatureIndex): Int = {
    index match {
      case RecordIndex    => config.recordThreads
      case _              => config.queryThreads
    }
  }

  // end methods from AccumuloConnectorCreator

  // other public methods

  /**
   * Optimized method to delete everything (all tables) associated with this datastore
   * (index tables and catalog table)
   * NB: We are *not* currently deleting the query table and/or query information.
   */
  def delete() = {
    val indexTables = getTypeNames.map(getSchema)
        .flatMap(sft => AccumuloFeatureIndex.indices(sft, IndexMode.Any).map(getTableName(sft.getTypeName, _)))
        .distinct
    val metadataTables = Seq(statsTable, catalogTable)
    // Delete index tables first then catalog table in case of error
    val allTables = indexTables ++ metadataTables
    allTables.filter(tableOps.exists).foreach(tableOps.delete)
  }

  /**
   * Reads the serialization type (feature encoding) from the metadata.
   *
   * @param sft simple feature type
   * @return serialization type
   * @throws RuntimeException if the feature encoding is invalid
   */
  @throws[RuntimeException]
  def getFeatureEncoding(sft: SimpleFeatureType): SerializationType =
    metadata.read(sft.getTypeName, "featureEncoding")
        .map(SerializationType.withName)
        .getOrElse(SerializationType.KRYO)

  /**
   * Gets the query plan for a given query. The query plan consists of the tables, ranges, iterators etc
   * required to run a query against accumulo.
   *
   * @param query query to execute
   * @param index hint on the index to use to satisfy the query
   * @return query plans
   */
  def getQueryPlan(query: Query,
                   index: Option[AccumuloFeatureIndex] = None,
                   explainer: Explainer = new ExplainLogging): Seq[QueryPlan] = {
    require(query.getTypeName != null, "Type name is required in the query")
    getQueryPlanner(query.getTypeName).planQuery(query, index, explainer)
  }

  /**
    * Gets iterator version as a string
    *
    * @return iterator version
    */
  def getIteratorVersion: String = {
    val scanner = connector.createScanner(catalogTable, new Authorizations())
    try {
      ProjectVersionIterator.scanProjectVersion(scanner)
    } catch {
      case NonFatal(e) => "unavailable"
    } finally {
      scanner.close()
    }
  }

  /**
   * Gets a query planner. Also has side-effect of setting transforms in the query.
   */
  protected [data] def getQueryPlanner(typeName: String): QueryPlanner = {
    val sft = getSchema(typeName)
    if (sft == null) {
      throw new IOException(s"Schema '$typeName' has not been initialized. Please call 'createSchema' first.")
    }
    new QueryPlanner(sft, this)
  }

  // end public methods

  /**
   * Computes and writes the metadata for this feature type
   */
  private def writeMetadata(sft: SimpleFeatureType) {
    // determine the schema ID - ensure that it is unique in this catalog
    // IMPORTANT: this method needs to stay inside a zookeeper distributed locking block
    var schemaId = 1
    val existingSchemaIds = getTypeNames.flatMap(metadata.read(_, SCHEMA_ID_KEY, cache = false)
        .map(_.getBytes(StandardCharsets.UTF_8).head.toInt))
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
    sft.setIndices(AccumuloDataStore.getEnabledIndices(sft))
    SimpleFeatureTypes.Configs.ENABLED_INDEX_OPTS.foreach(sft.getUserData.remove)

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

    // configure the stats combining iterator on the table for this sft
    GeoMesaMetadataStats.configureStatCombiner(connector, statsTable, sft)
  }

  private def deleteSharedTables(sft: SimpleFeatureType) =
    AccumuloFeatureIndex.indices(sft, IndexMode.Any).par.foreach(_.removeAll(sft, this))

  // NB: We are *not* currently deleting the query table and/or query information.
  private def deleteStandAloneTables(sft: SimpleFeatureType) = {
    val tables = AccumuloFeatureIndex.indices(sft, IndexMode.Any).map(getTableName(sft.getTypeName, _)).distinct
    tables.filter(tableOps.exists).foreach(tableOps.delete)
  }

  /**
    * Checks that the distributed runtime jar matches the project version of this client. We cache
    * successful checks for 10 minutes.
    */
  private def checkProjectVersion(): Unit = {
    if (projectVersionCheck.get() < System.currentTimeMillis()) {
      val clientVersion = GeoMesaProperties.ProjectVersion
      val iteratorVersion = getIteratorVersion
      if (iteratorVersion != clientVersion) {
        val versionMsg = "Configured server-side iterators do not match client version - " +
            s"client version: $clientVersion, server version: $iteratorVersion"
        logger.warn(versionMsg)
      }
      projectVersionCheck.set(System.currentTimeMillis() + 3600000) // 1 hour
    }
  }

  /**
   * Acquires a distributed lock for all accumulo data stores sharing this catalog table.
   * Make sure that you 'release' the lock in a finally block.
   */
  private def acquireCatalogLock(): Releasable = {
    val path = s"/org.locationtech.geomesa/accumulo/ds/$catalogTable"
    acquireDistributedLock(path, 120000).getOrElse {
      throw new RuntimeException(s"Could not acquire distributed lock at '$path'")
    }
  }

  /**
    * Acquires a distributed lock for a single feature type across all accumulo data stores sharing
    * this catalog table. Make sure that you 'release' the lock in a finally block.
    */
  private def acquireFeatureLock(typeName: String): Releasable = {
    val path = s"/org.locationtech.geomesa/accumulo/ds/$catalogTable-$typeName"
    acquireDistributedLock(path, 120000).getOrElse {
      throw new RuntimeException(s"Could not acquire distributed lock at '$path'")
    }
  }
}

object AccumuloDataStore {

  /**
    * Reads the indices configured using SimpleFeatureTypes.ENABLED_INDICES, or the
    * default indices for the schema version
    *
    * @param sft simple feature type
    * @return sequence of index (name, version)
    */
  private def getEnabledIndices(sft: SimpleFeatureType): Seq[(String, Int, IndexMode)] = {
    import SimpleFeatureTypes.Configs.ENABLED_INDEX_OPTS
    val marked: Seq[String] = ENABLED_INDEX_OPTS.map(sft.getUserData.get).find(_ != null) match {
      case None => AccumuloFeatureIndex.AllIndices.map(_.name).distinct
      case Some(enabled) =>
        val e = enabled.toString.split(",").map(_.trim).filter(_.length > 0)
        // check for old attribute index name
        if (e.contains("attr_idx")) {  e :+ AttributeIndex.name } else { e }
    }
    AccumuloFeatureIndex.getDefaultIndices(sft).collect {
      case i if marked.contains(i.name) => (i.name, i.version, IndexMode.ReadWrite)
    }
  }
}

// record scans are single-row ranges - increasing the threads too much actually causes performance to decrease
case class AccumuloDataStoreConfig(queryTimeout: Option[Long],
                                   queryThreads: Int,
                                   recordThreads: Int,
                                   writeThreads: Int,
                                   generateStats: Boolean,
                                   collectUsageStats: Boolean,
                                   caching: Boolean,
                                   looseBBox: Boolean)
