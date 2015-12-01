/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.data

import java.io.IOException
import java.util.NoSuchElementException
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.retry.ExponentialBackoffRetry
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.Hints
import org.geotools.feature.NameImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.Interval
import org.locationtech.geomesa.CURRENT_SCHEMA_VERSION
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore._
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType.StrategyType
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.accumulo.{AccumuloVersion, GeomesaSystemProperties}
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureSerializers}
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.{FeatureSpec, NonGeomAttributeSpec}
import org.locationtech.geomesa.utils.time.Time._
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.referencing.crs.CoordinateReferenceSystem

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Lock
import scala.util.{Failure, Success, Try}

/**
 *
 * @param connector Accumulo connector
 * @param catalogTable Table name in Accumulo to store metadata about featureTypes. For pre-catalog
 *                     single-table stores this equates to the spatiotemporal table name
 * @param authorizationsProvider Provides the authorizations used to access data
 * @param writeVisibilities   Visibilities applied to any data written by this store
 *
 *  This class handles DataStores which are stored in Accumulo Tables.  To be clear, one table may
 *  contain multiple features addressed by their featureName.
 */
class AccumuloDataStore(val connector: Connector,
                        val authToken: AuthenticationToken,
                        val catalogTable: String,
                        val authorizationsProvider: AuthorizationsProvider,
                        val writeVisibilities: String,
                        val queryTimeoutConfig: Option[Long] = None,
                        val queryThreadsConfig: Option[Int] = None,
                        val recordThreadsConfig: Option[Int] = None,
                        val writeThreadsConfig: Option[Int] = None,
                        val cachingConfig: Boolean = false)
    extends AbstractDataStore(true) with AccumuloConnectorCreator with StrategyHintsProvider with Logging {

  // having at least as many shards as tservers provides optimal parallelism in queries
  val DEFAULT_MAX_SHARD = connector.instanceOperations().getTabletServers.size()

  protected[data] val queryTimeoutMillis: Option[Long] = queryTimeoutConfig
      .orElse(GeomesaSystemProperties.QueryProperties.QUERY_TIMEOUT_MILLIS.option.map(_.toLong))

  // record scans are single-row ranges - increasing the threads too much actually causes performance to decrease
  private val recordScanThreads = recordThreadsConfig.getOrElse(10)

  private val writeThreads = writeThreadsConfig.getOrElse(10)

  // cap on the number of threads for any one query
  // if we let threads get too high, performance will suffer for simultaneous clients
  private val MAX_QUERY_THREADS = 15

  // floor on the number of query threads, even if the number of shards is 1
  private val MIN_QUERY_THREADS = 5

  // equivalent to: s"%~#s%$maxShard#r%${name}#cstr%0,3#gh%yyyyMMddHH#d::%~#s%3,2#gh::%~#s%#id"
  def buildDefaultSpatioTemporalSchema(name: String, maxShard: Int = DEFAULT_MAX_SHARD): String =
    new IndexSchemaBuilder("~")
      .randomNumber(maxShard)
      .indexOrDataFlag()
      .constant(name)
      .geoHash(0, 3)
      .date("yyyyMMddHH")
      .nextPart()
      .geoHash(3, 2)
      .nextPart()
      .id()
      .build()

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  private val validated = new mutable.HashMap[String, String]()
                              with mutable.SynchronizedMap[String, String]

  private[data] val metadata: GeoMesaMetadata =
    new AccumuloBackedMetadata(connector, catalogTable, writeVisibilities, authorizationsProvider)

  private val visibilityCheckCache = new mutable.HashMap[(String, String), Boolean]()
                                         with mutable.SynchronizedMap[(String, String), Boolean]

  private val defaultBWConfig =
    GeoMesaBatchWriterConfig().setMaxWriteThreads(writeThreads)

  private val tableOps = connector.tableOperations()

  AccumuloVersion.ensureTableExists(connector, catalogTable)

  /**
   * Computes and writes the metadata for this feature type
   *
   * @param sft
   * @param fe
   */
  private def writeMetadata(sft: SimpleFeatureType,
                            fe: SerializationType,
                            spatioTemporalSchemaValue: String) {

    // compute the metadata values
    val attributesValue             = SimpleFeatureTypes.encodeType(sft)
    val dtgValue: Option[String]    = sft.getDtgField // this will have already been checked and set
    val featureEncodingValue        = /*_*/fe.toString/*_*/
    val z3TableValue                = Z3Table.formatTableName(catalogTable, sft)
    val spatioTemporalIdxTableValue = SpatioTemporalTable.formatTableName(catalogTable, sft)
    val attrIdxTableValue           = AttributeTable.formatTableName(catalogTable, sft)
    val recordTableValue            = RecordTable.formatTableName(catalogTable, sft)
    val queriesTableValue           = formatQueriesTableName(catalogTable)
    val tableSharingValue           = sft.isTableSharing.toString
    val enabledTablesValue          = sft.getEnabledTables.toString
    val dataStoreVersion            = CURRENT_SCHEMA_VERSION.toString

    // store each metadata in the associated key
    val attributeMap =
      Map(
        ATTRIBUTES_KEY        -> attributesValue,
        SCHEMA_KEY            -> spatioTemporalSchemaValue,
        FEATURE_ENCODING_KEY  -> featureEncodingValue,
        VISIBILITIES_KEY      -> writeVisibilities,
        Z3_TABLE_KEY          -> z3TableValue,
        ST_IDX_TABLE_KEY      -> spatioTemporalIdxTableValue,
        ATTR_IDX_TABLE_KEY    -> attrIdxTableValue,
        RECORD_TABLE_KEY      -> recordTableValue,
        QUERIES_TABLE_KEY     -> queriesTableValue,
        SHARED_TABLES_KEY     -> tableSharingValue,
        TABLES_ENABLED_KEY    -> enabledTablesValue,
        VERSION_KEY           -> dataStoreVersion
      ) ++ (if (dtgValue.isDefined) Map(DTGFIELD_KEY -> dtgValue.get) else Map.empty)

    val featureName = sft.getTypeName
    metadata.insert(featureName, attributeMap)

    // write out a visibilities protected entry that we can use to validate that a user can see
    // data in this store
    if (!writeVisibilities.isEmpty) {
      metadata.insert(featureName, VISIBILITIES_CHECK_KEY, writeVisibilities, writeVisibilities)
    }

    // write a schema ID out - ensure that it is unique in this catalog
    // IMPORTANT: this method needs to stay inside a zookeeper distributed locking block
    var schemaId = 1
    val existingSchemaIds =
      getTypeNames.flatMap(metadata.readNoCache(_, SCHEMA_ID_KEY).map(_.getBytes("UTF-8").head.toInt))
    while (existingSchemaIds.contains(schemaId)) { schemaId += 1 }
    // We use a single byte for the row prefix to save space - if we exceed the single byte limit then
    // our ranges would start to overlap and we'd get errors
    require(schemaId <= Byte.MaxValue,
      s"No more than ${Byte.MaxValue} schemas may share a single catalog table")
    metadata.insert(featureName, SCHEMA_ID_KEY, new String(Array(schemaId.asInstanceOf[Byte]), "UTF-8"))
  }

  /**
   * Used to update the attributes that are marked as indexed
   *
   * @param featureName
   * @param attributes
   */
  def updateIndexedAttributes(featureName: String, attributes: String): Unit = {
    val FeatureSpec(existing, _) = SimpleFeatureTypes.parse(getAttributes(featureName).getOrElse(""))
    val FeatureSpec(updated, _)  = SimpleFeatureTypes.parse(attributes)
    // check that the only changes are to non-geometry index flags
    val ok = existing.length == updated.length &&
      existing.zip(updated).forall { case (e, u) => e == u ||
        (e.isInstanceOf[NonGeomAttributeSpec] &&
         u.isInstanceOf[NonGeomAttributeSpec] &&
         e.clazz == u.clazz &&
         e.name == u.name) }
    if (!ok) {
      throw new IllegalArgumentException("Attribute spec is not consistent with existing spec")
    }

    metadata.insert(featureName, ATTRIBUTES_KEY, attributes)

    // reconfigure the splits on the attribute table
    val sft = getSchema(featureName)
    val table = getTableName(featureName, AttributeTable)
    AttributeTable.configureTable(sft, table, tableOps)
  }

  /**
   * Read Queries table name from store metadata
   */
  def getQueriesTableName(featureName: String): String =
    Try(metadata.readRequired(featureName, QUERIES_TABLE_KEY)) match {
      case Success(queriesTableName) => queriesTableName
      // For backwards compatibility with existing tables that do not have queries table metadata
      case Failure(t) if t.getMessage.contains("Unable to find required metadata property") =>
        writeAndReturnMissingQueryTableMetadata(featureName)
      case Failure(t) => throw t
    }

  /**
   * Here just to write missing query metadata (for backwards compatibility with preexisting data).
   */
  private[this] def writeAndReturnMissingQueryTableMetadata(featureName: String): String = {
    val queriesTableValue = formatQueriesTableName(catalogTable)
    metadata.insert(featureName, QUERIES_TABLE_KEY, queriesTableValue)
    queriesTableValue
  }


  /**
   * Read SpatioTemporal Index table name from store metadata
   */
  def getSpatioTemporalMaxShard(sft: SimpleFeatureType): Int = {
    val indexSchemaFmt = metadata.read(sft.getTypeName, SCHEMA_KEY)
      .getOrElse(throw new RuntimeException(s"Unable to find required metadata property for $SCHEMA_KEY"))
    IndexSchema.maxShard(indexSchemaFmt)
  }

  def createTablesForType(sft: SimpleFeatureType): Unit = {
    GeoMesaTable.getTables(sft).foreach { table =>
      val name = table.formatTableName(catalogTable, sft)
      AccumuloVersion.ensureTableExists(connector, name)
      table.configureTable(sft, name, tableOps)
    }
  }

  // Retrieves or computes the indexSchema
  def computeSpatioTemporalSchema(sft: SimpleFeatureType): String = {
    Option(sft.getStIndexSchema) match {
      case None         => buildDefaultSpatioTemporalSchema(sft.getTypeName)
      case Some(schema) => schema
    }
  }

  /**
   * Compute the GeoMesa SpatioTemporal Schema, create tables, and write metadata to catalog.
   * If the schema already exists, log a message and continue without error.
   * This method uses distributed locking to ensure a schema is only created once.
   *
   * @param sft
   */
  override def createSchema(sft: SimpleFeatureType) =
    if (getSchema(sft.getTypeName) == null) {
      val lock = acquireDistributedLock()
      try {
        // check a second time now that we have the lock
        if (getSchema(sft.getTypeName) == null) {
          // inspect, warn and set SF_PROPERTY_START_TIME if appropriate
          // do this before anything else so appropriate tables will be created
          TemporalIndexCheck.validateDtgField(sft)
          // set the schema version, required for table checks
          sft.setSchemaVersion(CURRENT_SCHEMA_VERSION)
          val spatioTemporalSchema = computeSpatioTemporalSchema(sft)
          checkSchemaRequirements(sft, spatioTemporalSchema)
          writeMetadata(sft, SerializationType.KRYO, spatioTemporalSchema)

          // reload the SFT then copy over any additional keys that were in the original sft
          val reloadedSft = getSchema(sft.getTypeName)
          (sft.getUserData.keySet -- reloadedSft.getUserData.keySet)
            .foreach(k => reloadedSft.getUserData.put(k, sft.getUserData.get(k)))

          createTablesForType(reloadedSft)
        }
      } finally {
        lock.release()
      }
    }

  // This function enforces the shared ST schema requirements.
  //  For a shared ST table, the IndexSchema must start with a partition number and a constant string.
  //  TODO: This function should check if the constant is equal to the featureType.getTypeName
  def checkSchemaRequirements(featureType: SimpleFeatureType, schema: String) {
    if (featureType.isTableSharing) {
      val (rowf, _,_) = IndexSchema.parse(IndexSchema.formatter, schema).get
      rowf.lf match {
        case Seq(pf: PartitionTextFormatter, i: IndexOrDataTextFormatter, const: ConstantTextFormatter, r@_*) =>
        case _ => throw new RuntimeException(s"Failed to validate the schema requirements for " +
          s"the feature ${featureType.getTypeName} for catalog table : $catalogTable.  " +
          s"We require that features sharing a table have schema starting with a partition and a constant.")
      }
    }
  }

  /**
   * Deletes the tables from Accumulo created from the Geomesa SpatioTemporal Schema, and deletes
   * metadata from the catalog. If the table is an older 0.10.x table, we throw an exception.
   *
   * This version overrides the default geotools removeSchema function and uses 1 thread for
   * querying during metadata deletion.
   *
   * @param featureName the name of the feature
   */
  override def removeSchema(featureName: String) = removeSchema(featureName, 1)

  /**
   * Deletes the tables from Accumulo created from the Geomesa SpatioTemporal Schema, and deletes
   * metadata from the catalog.
   *
   * @param featureName the name of the feature
   * @param numThreads the number of concurrent threads to spawn for querying during metadata deletion
   */
  def removeSchema(featureName: String, numThreads: Int = 1) = {
    val lock = acquireDistributedLock()
    try {
      Option(getSchema(featureName)).foreach { sft =>
        if (sft.isTableSharing && getTypeNames.length > 1) {
          deleteSharedTables(sft)
        } else {
          deleteStandAloneTables(sft)
        }
      }
      metadata.delete(featureName, numThreads)
      metadata.expireCache(featureName)
    } finally {
      lock.release()
    }
  }

  private def deleteSharedTables(sft: SimpleFeatureType) = {
    val auths = authorizationsProvider.getAuthorizations
    val numThreads = queryThreadsConfig.getOrElse(Math.min(MAX_QUERY_THREADS,
      Math.max(MIN_QUERY_THREADS, getSpatioTemporalMaxShard(sft))))

    GeoMesaTable.getTables(sft).par.foreach { table =>
      val name = getTableName(sft.getTypeName, table)
      if (tableOps.exists(name)) {
        if (table == Z3Table) {
          tableOps.delete(name)
        } else {
          val deleter = connector.createBatchDeleter(name, auths, numThreads, defaultBWConfig)
          table.deleteFeaturesForType(sft, deleter)
          deleter.close()
        }
      }
    }
  }

  // NB: We are *not* currently deleting the query table and/or query information.
  private def deleteStandAloneTables(sft: SimpleFeatureType) =
    GeoMesaTable.getTableNames(sft, this).filter(tableOps.exists).foreach(tableOps.delete)

  /**
   * Delete everything (all tables) associated with this datastore (index tables and catalog table)
   * NB: We are *not* currently deleting the query table and/or query information.
   */
  def delete() = {
    val indexTables = getTypeNames.map(getSchema).flatMap(GeoMesaTable.getTableNames(_, this)).distinct
    // Delete index tables first then catalog table in case of error
    indexTables.filter(tableOps.exists).foreach(tableOps.delete)
    if (tableOps.exists(catalogTable)) {
      tableOps.delete(catalogTable)
    }
  }

  /**
   * Validates the configuration of this data store instance against the stored configuration in
   * Accumulo, if any. This is used to ensure that the visibilities (in particular) do not change
   * from one instance to the next. This will fill in any missing metadata that may occur in an
   * older (0.1.0) version of a table.
   */
  protected def validateMetadata(featureName: String): Unit = {
    metadata.read(featureName, ATTRIBUTES_KEY)
      .getOrElse(throw new IOException(s"Feature '$featureName' has not been initialized. Please call 'createSchema' first."))

    val ok = validated.getOrElseUpdate(featureName, checkMetadata(featureName))

    if (!ok.isEmpty) {
      throw new RuntimeException("Configuration of this DataStore does not match the schema values: " + ok)
    }
  }

  /**
   * Wraps the functionality of checking the metadata against this config
   *
   * @param featureName
   * @return string with errors, or empty string
   */
  private def checkMetadata(featureName: String): String = {

    // check the different metadata options
    val checks = List(checkVisibilitiesMetadata(featureName))

    val errors = checks.flatten.mkString(", ")

    // if no errors, check the feature encoding
    if (errors.isEmpty) {
      checkFeatureEncodingMetadata(featureName)
    }

    errors
  }

  /**
   * Checks the visibility stored in the metadata table against the configuration of this data store.
   *
   * @param featureName
   * @return
   */
  private def checkVisibilitiesMetadata(featureName: String): Option[String] = {
    // validate that visibilities have not changed
    val storedVisibilities = metadata.read(featureName, VISIBILITIES_KEY).getOrElse("")
    if (storedVisibilities != writeVisibilities) {
      Some(s"$VISIBILITIES_KEY = '$writeVisibilities', should be '$storedVisibilities'")
    } else {
      None
    }
  }

  /**
   * Checks the feature encoding in the metadata against the configuration of this data store.
   *
   * @param featureName
   */
  def checkFeatureEncodingMetadata(featureName: String): Unit = {
    // for feature encoding, we are more lenient - we will use whatever is stored in the table
    if (metadata.read(featureName, FEATURE_ENCODING_KEY).getOrElse("").isEmpty) {
      throw new RuntimeException(s"No '$FEATURE_ENCODING_KEY' found for feature '$featureName'.")
    }
  }

  /**
   * Checks whether the current user can write - based on whether they can read data in this
   * data store.
   *
   * @param featureName
   */
  protected def checkWritePermissions(featureName: String): Unit = {
    val visibilities = metadata.read(featureName, VISIBILITIES_KEY).getOrElse("")
    // if no visibilities set, no need to check
    if (!visibilities.isEmpty) {
      // create a key for the user's auths that we will use to check the cache
      val authString = authorizationsProvider.getAuthorizations.getAuthorizations
                      .map(a => new String(a)).sorted.mkString(",")
      if (!checkWritePermissions(featureName, authString)) {
        throw new RuntimeException(s"The current user does not have the required authorizations to " +
          s"write $featureName features. Required authorizations: '$visibilities', " +
          s"actual authorizations: '$authString'")
      }
    }
  }

  /**
   * Wraps logic for checking write permissions for a given set of auths
   *
   * @param featureName
   * @param authString
   * @return
   */
  private def checkWritePermissions(featureName: String, authString: String) = {
    // if cache contains an entry, use that
    visibilityCheckCache.getOrElse((featureName, authString), {
      // check the 'visibilities check' metadata - it has visibilities applied, so if the user
      // can read that row, then they can read any data in the data store
      val visCheck = metadata.readNoCache(featureName, VISIBILITIES_CHECK_KEY)
                      .isInstanceOf[Some[String]]
      visibilityCheckCache.put((featureName, authString), visCheck)
      visCheck
    })
  }

  /**
   * Implementation of AbstractDataStore getTypeNames
   *
   * @return
   */
  override def getTypeNames: Array[String] =
    if (tableOps.exists(catalogTable)) metadata.getFeatureTypes else Array.empty


  // NB:  By default, AbstractDataStore is "isWriteable".  This means that createFeatureSource returns
  // a featureStore
  override def getFeatureSource(typeName: Name): SimpleFeatureSource = {
    validateMetadata(typeName.getLocalPart)
    if (cachingConfig) {
      new AccumuloFeatureStore(this, typeName) with CachingFeatureSource
    } else {
      new AccumuloFeatureStore(this, typeName)
    }
  }

  override def getFeatureSource(typeName: String): SimpleFeatureSource =
    getFeatureSource(new NameImpl(typeName))

  /**
   * Reads the index schema format out of the metadata
   *
   * @param featureName
   * @return
   */
  def getIndexSchemaFmt(featureName: String) =
    metadata.read(featureName, SCHEMA_KEY).getOrElse(EMPTY_STRING)

  /**
   * Updates the index schema format - WARNING don't use this unless you know what you're doing.
   * @param sft
   * @param schema
   */
  def setIndexSchemaFmt(sft: String, schema: String) = {
    metadata.insert(sft, SCHEMA_KEY, schema)
    metadata.expireCache(sft)
  }

  /**
   * Update the geomesa version
   *
   * @param sft
   * @param version
   */
  def setGeomesaVersion(sft: String, version: Int): Unit = {
    metadata.insert(sft, VERSION_KEY, version.toString)
    metadata.expireCache(sft)
  }

  /**
   * Reads the attributes out of the metadata
   *
   * @param featureName
   * @return
   */
  private def getAttributes(featureName: String) = metadata.read(featureName, ATTRIBUTES_KEY)

  /**
   * Reads the feature encoding from the metadata.
   *
   * @throws RuntimeException if the feature encoding is missing or invalid
   */
  def getFeatureEncoding(sft: SimpleFeatureType): SerializationType = {
    val name = metadata.readRequired(sft.getTypeName, FEATURE_ENCODING_KEY)
    try {
      SerializationType.withName(name)
    } catch {
      case e: NoSuchElementException => throw new RuntimeException(s"Invalid Feature Encoding '$name'.")
    }
  }

  // We assume that they want the bounds for everything.
  override def getBounds(query: Query): ReferencedEnvelope = {
    val env = metadata.read(query.getTypeName, SPATIAL_BOUNDS_KEY).getOrElse(WHOLE_WORLD_BOUNDS)
    val minMaxXY = env.split(":")
    val curBounds = minMaxXY.size match {
      case 4 => env
      case _ => WHOLE_WORLD_BOUNDS
    }
    val sft = getSchema(query.getTypeName)
    val crs = sft.getCoordinateReferenceSystem
    stringToReferencedEnvelope(curBounds, crs)
  }

  def getTimeBounds(typeName: String): Interval = {
    metadata.read(typeName, TEMPORAL_BOUNDS_KEY)
      .map(stringToTimeBounds)
      .getOrElse(ALL_TIME_BOUNDS)
  }

  def getRecordTableSize(featureName: String): Long = {
    metadata.getTableSize(getTableName(featureName, RecordTable))
  }

  def stringToTimeBounds(value: String): Interval = {
    val longs = value.split(":").map(java.lang.Long.parseLong)
    require(longs(0) <= longs(1))
    require(longs.length == 2)
    new Interval(longs(0), longs(1))
  }

  private def stringToReferencedEnvelope(string: String,
                                         crs: CoordinateReferenceSystem): ReferencedEnvelope = {
    val minMaxXY = string.split(":")
    require(minMaxXY.size == 4)
    new ReferencedEnvelope(minMaxXY(0).toDouble, minMaxXY(1).toDouble, minMaxXY(2).toDouble,
                            minMaxXY(3).toDouble, crs)
  }

  /**
   * Writes spatial bounds for this feature
   *
   * @param featureName
   * @param bounds
   */
  def writeSpatialBounds(featureName: String, bounds: ReferencedEnvelope) {
    // prepare to write out properties to the Accumulo SHP-file table
    val newbounds = metadata.read(featureName, SPATIAL_BOUNDS_KEY) match {
      case Some(env) => getNewBounds(env, bounds)
      case None      => bounds
    }

    val minMaxXY = List(newbounds.getMinX, newbounds.getMaxX, newbounds.getMinY, newbounds.getMaxY)
    val encoded = minMaxXY.mkString(":")

    metadata.insert(featureName, SPATIAL_BOUNDS_KEY, encoded)
  }

  private def getNewBounds(env: String, bounds: ReferencedEnvelope) = {
    val oldBounds = stringToReferencedEnvelope(env, DefaultGeographicCRS.WGS84)
    oldBounds.expandToInclude(bounds)
    oldBounds
  }

  /**
   * Writes temporal bounds for this feature
   *
   * @param featureName
   * @param timeBounds
   */
  def writeTemporalBounds(featureName: String, timeBounds: Interval) {
    val newTimeBounds = metadata.read(featureName, TEMPORAL_BOUNDS_KEY) match {
      case Some(currentTimeBoundsString) => getNewTimeBounds(currentTimeBoundsString, timeBounds)
      case None                          => Some(timeBounds)
    }

    // Only write expanded bounds.
    newTimeBounds.foreach { newBounds =>
      val encoded = s"${newBounds.getStartMillis}:${newBounds.getEndMillis}"
      metadata.insert(featureName, TEMPORAL_BOUNDS_KEY, encoded)
    }
  }

  def getNewTimeBounds(current: String, newBounds: Interval): Option[Interval] = {
    val currentTimeBounds = stringToTimeBounds(current)
    val expandedTimeBounds = currentTimeBounds.expandByInterval(newBounds)
    if (!currentTimeBounds.equals(expandedTimeBounds)) {
      Some(expandedTimeBounds)
    } else {
      None
    }
  }

  /**
   * Implementation of abstract method
   *
   * @param featureName
   * @return the corresponding feature type (schema) for this feature name,
   *         or NULL if this feature name does not appear to exist
   */
  override def getSchema(featureName: String): SimpleFeatureType = getSchema(new NameImpl(featureName))

  override def getSchema(name: Name): SimpleFeatureType = {
    val featureName = name.getLocalPart
    getAttributes(featureName).map { attributes =>
      val sft = SimpleFeatureTypes.createType(name.getURI, attributes)
      // IMPORTANT: set data that we want to pass around with the sft
      metadata.read(featureName, DTGFIELD_KEY).foreach(sft.setDtgField)
      sft.setSchemaVersion(metadata.readRequired(featureName, VERSION_KEY).toInt)
      sft.setStIndexSchema(metadata.read(featureName, SCHEMA_KEY).orNull)
      // If no data is written, we default to 'false' in order to support old tables.
      if (metadata.read(featureName, SHARED_TABLES_KEY).exists(_.toBoolean)) {
        sft.setTableSharing(true)
        // use schema id if available or fall back to old type name for backwards compatibility
        val prefix = metadata.read(featureName, SCHEMA_ID_KEY).getOrElse(s"${sft.getTypeName}~")
        sft.setTableSharingPrefix(prefix)
      } else {
        sft.setTableSharing(false)
        sft.setTableSharingPrefix("")
      }

      metadata.read(featureName, TABLES_ENABLED_KEY).map { enabledTablesStr =>
        sft.setEnabledTables(enabledTablesStr)
      }

      sft
    }.orNull
  }

  // Implementation of Abstract method
  def getFeatureReader(featureName: String): AccumuloFeatureReader =
    getFeatureReader(featureName, new Query(featureName))

  // This override is important as it allows us to optimize and plan our search with the Query.
  override def getFeatureReader(featureName: String, query: Query): AccumuloFeatureReader = {
    val qp = getQueryPlanner(featureName, this)
    new AccumuloFeatureReader(qp, query, this)
  }

  // override the abstract data store method - we already handle all projections, transformations, etc
  override def getFeatureReader(query: Query, transaction: Transaction): AccumuloFeatureReader =
    getFeatureReader(query.getTypeName, query)

  /**
   * Gets the query plan for a given query. The query plan consists of the tables, ranges, iterators etc
   * required to run a query against accumulo.
   */
  def getQueryPlan(query: Query, strategy: Option[StrategyType] = None): Seq[QueryPlan] = {
    require(query.getTypeName != null, "Type name is required in the query")
    planQuery(query.getTypeName, query, strategy, ExplainNull)
  }

  /**
   * Prints the query plan for a given query to the provided output.
   */
  def explainQuery(query: Query, o: ExplainerOutputType = new ExplainPrintln): Unit = {
    require(query.getTypeName != null, "Type name is required in the query")
    planQuery(query.getTypeName, query, None, o)
  }

  /**
   * Plan the query, but don't execute it
   */
  private def planQuery(featureName: String,
                        query: Query,
                        strategy: Option[StrategyType],
                        o: ExplainerOutputType): Seq[QueryPlan] =
    getQueryPlanner(featureName, this).planQuery(query, None, o)

  /**
   * Gets a query planner. Also has side-effect of setting transforms in the query.
   */
  private def getQueryPlanner(featureName: String, cc: AccumuloConnectorCreator): QueryPlanner = {
    validateMetadata(featureName)
    val sft = getSchema(featureName)
    val indexSchemaFmt = getIndexSchemaFmt(featureName)
    val featureEncoding = getFeatureEncoding(sft)
    val hints = strategyHints(sft)
    new QueryPlanner(sft, featureEncoding, indexSchemaFmt, cc, hints)
  }

  /* create a general purpose writer that is capable of insert, deletes, and updates */
  override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): SFFeatureWriter = {
    validateMetadata(typeName)
    checkWritePermissions(typeName)
    val sft = getSchema(typeName)
    val fe = SimpleFeatureSerializers(sft, getFeatureEncoding(sft))
    val ive = IndexValueEncoder(sft)
    new ModifyAccumuloFeatureWriter(sft, fe, ive, this, writeVisibilities, filter)
  }

  /* optimized for GeoTools API to return writer ONLY for appending (aka don't scan table) */
  override def getFeatureWriterAppend(typeName: String,
                                      transaction: Transaction): SFFeatureWriter = {
    validateMetadata(typeName)
    checkWritePermissions(typeName)
    val sft = getSchema(typeName)
    val fe = SimpleFeatureSerializers(sft, getFeatureEncoding(sft))
    val ive = IndexValueEncoder(sft)
    new AppendAccumuloFeatureWriter(sft, fe, ive, this, writeVisibilities)
  }

  override def getUnsupportedFilter(featureName: String, filter: Filter): Filter = Filter.INCLUDE

  override def getScanner(table: String): Scanner =
    connector.createScanner(table, authorizationsProvider.getAuthorizations)

  override def getBatchScanner(table: String, threads: Int): BatchScanner =
    connector.createBatchScanner(table, authorizationsProvider.getAuthorizations, threads)

  override def getTableName(featureName: String, table: GeoMesaTable): String = {
    val key = table match {
      case RecordTable         => RECORD_TABLE_KEY
      case Z3Table             => Z3_TABLE_KEY
      case AttributeTable      => ATTR_IDX_TABLE_KEY
      case AttributeTableV5    => ATTR_IDX_TABLE_KEY
      case SpatioTemporalTable => ST_IDX_TABLE_KEY
      case _ => throw new NotImplementedError("Unknown table")
    }
    metadata.readRequired(featureName, key)
  }

  override def getSuggestedThreads(featureName: String, table: GeoMesaTable): Int = {
    table match {
      case RecordTable         => recordScanThreads
      case Z3Table             => queryThreadsConfig.getOrElse(8)
      case AttributeTable      => queryThreadsConfig.getOrElse(8)
      case AttributeTableV5    => 1
      case SpatioTemporalTable =>
        queryThreadsConfig.getOrElse {
          val shards = getSpatioTemporalMaxShard(getSchema(featureName))
          Math.min(MAX_QUERY_THREADS, Math.max(MIN_QUERY_THREADS, shards))
        }
      case _ => throw new NotImplementedError("Unknown table")
    }
  }

  /**
   * Read Queries table name from store metadata
   */
  def getQueriesTableName(featureType: SimpleFeatureType): String =
    getQueriesTableName(featureType.getTypeName)

  override def strategyHints(sft: SimpleFeatureType) = new UserDataStrategyHints()

  /**
   * Gets and acquires a distributed lock for all accumulo data stores sharing this catalog table.
   * Make sure that you 'release' the lock in a finally block.
   */
  private def acquireDistributedLock(): Releasable = {
    if (connector.isInstanceOf[MockConnector]) {
      // for mock connections use a jvm-level lock
      val lock = mockLocks.synchronized(mockLocks.getOrElseUpdate(catalogTable, new Lock))
      lock.acquire()
      new Releasable {
        override def release(): Unit = lock.release()
      }
    } else {
      val backoff = new ExponentialBackoffRetry(1000, 3)
      val client = CuratorFrameworkFactory.newClient(connector.getInstance().getZooKeepers, backoff)
      client.start()
      // unique path per catalog table - must start with a forward slash
      val lockPath =  s"/org.locationtech.geomesa/accumulo/ds/$catalogTable"
      val lock = new InterProcessSemaphoreMutex(client, lockPath)
      if (!lock.acquire(10000, TimeUnit.MILLISECONDS)) {
        throw new RuntimeException(s"Could not acquire distributed lock at '$lockPath'")
      }
      // delegate lock that will close the curator client upon release
      new Releasable {
        override def release(): Unit = try lock.release() finally client.close()
      }
    }
  }
}

object AccumuloDataStore {

  private lazy val mockLocks = scala.collection.mutable.Map.empty[String, Lock]

  trait Releasable {
    def release(): Unit
  }

  /**
   * Format queries table name for Accumulo...table name is stored in metadata for other usage
   * and provide compatibility moving forward if table names change
   * @param catalogTable
   * @return
   */
  def formatQueriesTableName(catalogTable: String): String =
    GeoMesaTable.concatenateNameParts(catalogTable, "queries")
}

