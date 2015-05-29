/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.locationtech.geomesa.accumulo.data

import java.io.IOException
import java.util.{Map => JMap, NoSuchElementException}

import com.google.common.collect.ImmutableSortedSet
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.file.keyfunctor.{ColumnFamilyFunctor, RowFunctor}
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.io.Text
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.Hints
import org.geotools.feature.NameImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.Interval
import org.locationtech.geomesa.accumulo
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore._
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.accumulo.index
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.util.{ExplainingConnectorCreator, GeoMesaBatchWriterConfig}
import org.locationtech.geomesa.accumulo.data.TableSplitter
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.{SimpleFeatureSerializers, SerializationType, SimpleFeatureSerializer}
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.{FeatureSpec, NonGeomAttributeSpec}
import org.locationtech.geomesa.utils.time.Time._
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.referencing.crs.CoordinateReferenceSystem

import scala.collection.JavaConversions._
import scala.collection.mutable
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
                        val queryThreadsConfig: Option[Int] = None,
                        val recordThreadsConfig: Option[Int] = None,
                        val writeThreadsConfig: Option[Int] = None,
                        val cachingConfig: Boolean = false,
                        val featureEncoding: SerializationType = DEFAULT_ENCODING)
    extends AbstractDataStore(true) with AccumuloConnectorCreator with StrategyHintsProvider with Logging {

  // having at least as many shards as tservers provides optimal parallelism in queries
  protected [accumulo] val DEFAULT_MAX_SHARD = connector.instanceOperations().getTabletServers.size()

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

  private val metadata: GeoMesaMetadata =
    new AccumuloBackedMetadata(connector, catalogTable, writeVisibilities, authorizationsProvider)

  private val visibilityCheckCache = new mutable.HashMap[(String, String), Boolean]()
                                         with mutable.SynchronizedMap[(String, String), Boolean]

  private val defaultBWConfig =
    GeoMesaBatchWriterConfig().setMaxWriteThreads(writeThreads)

  private val tableOps = connector.tableOperations()

  ensureTableExists(catalogTable)

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
    val attributesValue = SimpleFeatureTypes.encodeType(sft)
    val dtgValue: Option[String] = {
      val userData = sft.getUserData
      // inspect, warn and set SF_PROPERTY_START_TIME if appropriate
      TemporalIndexCheck.extractNewDTGFieldCandidate(sft)
        .foreach { name => userData.put(accumulo.index.SF_PROPERTY_START_TIME, name) }
      if (userData.containsKey(accumulo.index.SF_PROPERTY_START_TIME)) {
        Option(userData.get(accumulo.index.SF_PROPERTY_START_TIME).asInstanceOf[String])
      } else {
        None
      }
    }
    val featureEncodingValue        = /*_*/fe.toString/*_*/
    val z3TableValue                = formatZ3TableName(catalogTable, sft)
    val spatioTemporalIdxTableValue = formatSpatioTemporalIdxTableName(catalogTable, sft)
    val attrIdxTableValue           = formatAttrIdxTableName(catalogTable, sft)
    val recordTableValue            = formatRecordTableName(catalogTable, sft)
    val queriesTableValue           = formatQueriesTableName(catalogTable, sft)
    val dtgFieldValue               = dtgValue.getOrElse(accumulo.DEFAULT_DTG_PROPERTY_NAME)
    val tableSharingValue           = accumulo.index.getTableSharing(sft).toString
    val dataStoreVersion            = INTERNAL_GEOMESA_VERSION.toString

    // store each metadata in the associated key
    val attributeMap =
      Map(
        ATTRIBUTES_KEY        -> attributesValue,
        SCHEMA_KEY            -> spatioTemporalSchemaValue,
        DTGFIELD_KEY          -> dtgFieldValue,
        FEATURE_ENCODING_KEY  -> featureEncodingValue,
        VISIBILITIES_KEY      -> writeVisibilities,
        Z3_TABLE_KEY          -> z3TableValue,
        ST_IDX_TABLE_KEY      -> spatioTemporalIdxTableValue,
        ATTR_IDX_TABLE_KEY    -> attrIdxTableValue,
        RECORD_TABLE_KEY      -> recordTableValue,
        QUERIES_TABLE_KEY     -> queriesTableValue,
        SHARED_TABLES_KEY     -> tableSharingValue,
        VERSION_KEY           -> dataStoreVersion
      )

    val featureName = getFeatureName(sft)
    metadata.insert(featureName, attributeMap)

    // write out a visibilities protected entry that we can use to validate that a user can see
    // data in this store
    if (!writeVisibilities.isEmpty) {
      metadata.insert(featureName, VISIBILITIES_CHECK_KEY, writeVisibilities, writeVisibilities)
    }
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
    configureAttrIdxTable(getSchema(featureName), getAttributeTable(featureName))
  }

  type KVEntry = JMap.Entry[Key,Value]

  /**
   * Read Record table name from store metadata
   */
  def getRecordTable(featureType: SimpleFeatureType): String = getRecordTable(featureType.getTypeName)

  /**
   * Read Record table name from store metadata
   */
  def getRecordTable(featureName: String): String = metadata.readRequired(featureName, RECORD_TABLE_KEY)

  /**
   * Read SpatioTemporal Index table name from store metadata
   */
  override def getSpatioTemporalTable(featureType: SimpleFeatureType): String =
    getSpatioTemporalTable(featureType.getTypeName)

  override def getZ3Table(featureType: SimpleFeatureType): String = getZ3Table(featureType.getTypeName)

  def getZ3Table(featureName: String): String =
    metadata.readRequired(featureName, Z3_TABLE_KEY)
  /**
   * Read SpatioTemporal Index table name from store metadata
   */
  def getSpatioTemporalTable(featureName: String): String =
    metadata.readRequired(featureName, ST_IDX_TABLE_KEY)

  /**
   * Read Attribute Index table name from store metadata
   */
  override def getAttributeTable(featureType: SimpleFeatureType): String =
    getAttributeTable(featureType.getTypeName)

  /**
   * Read Attribute Index table name from store metadata
   */
  def getAttributeTable(featureName: String): String =
    metadata.readRequired(featureName, ATTR_IDX_TABLE_KEY)

  /**
   * Read Queries table name from store metadata
   * @param featureType
   * @return
   */
  def getQueriesTableName(featureType: SimpleFeatureType): String =
    Try(metadata.readRequired(featureType.getTypeName, QUERIES_TABLE_KEY)) match {
      case Success(queriesTableName) => queriesTableName
      // For backwards compatibility with existing tables that do not have queries table metadata
      case Failure(t) if t.getMessage.contains("Unable to find required metadata property") =>
        writeAndReturnMissingQueryTableMetadata(featureType)
      case Failure(t) => throw t
    }

  /**
   * Here just to write missing query metadata (for backwards compatibility with preexisting data).
   * @param sft
   */
  private[this] def writeAndReturnMissingQueryTableMetadata(sft: SimpleFeatureType): String = {
    val featureName = getFeatureName(sft)

    val queriesTableValue = formatQueriesTableName(catalogTable, sft)

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

  def createTablesForType(featureType: SimpleFeatureType, maxShard: Int) {
    val z3Table                = formatZ3TableName(catalogTable, featureType)
    val spatioTemporalIdxTable = formatSpatioTemporalIdxTableName(catalogTable, featureType)
    val attributeIndexTable    = formatAttrIdxTableName(catalogTable, featureType)
    val recordTable            = formatRecordTableName(catalogTable, featureType)

    List(z3Table, spatioTemporalIdxTable, attributeIndexTable, recordTable).foreach(ensureTableExists)

    configureZ3Table(featureType, z3Table)
    configureRecordTable(featureType, recordTable)
    configureAttrIdxTable(featureType, attributeIndexTable)
    configureSpatioTemporalIdxTable(maxShard, featureType, spatioTemporalIdxTable)
  }

  private def ensureTableExists(table: String) =
    if (!tableOps.exists(table)) {
      try {
        tableOps.create(table, true, TimeType.LOGICAL)
      } catch {
        case e: TableExistsException => // this can happen with multiple threads but shouldn't cause any issues
      }
    }

  def configureZ3Table(featureType: SimpleFeatureType, z3Table: String): Unit = Z3Table.configureTable(featureType, z3Table, tableOps)

  def configureRecordTable(featureType: SimpleFeatureType, recordTable: String): Unit = {
    val prefix = index.getTableSharingPrefix(featureType)
    val prefixFn = RecordTable.getRowKey(prefix, _: String)
    val splitterClazz = featureType.getUserData.getOrElse(SimpleFeatureTypes.TABLE_SPLITTER, classOf[HexSplitter].getCanonicalName).asInstanceOf[String]
    val clazz = Class.forName(splitterClazz)
    val splitter = clazz.newInstance().asInstanceOf[TableSplitter]
    val splitterOptions = featureType.getUserData.getOrElse(SimpleFeatureTypes.TABLE_SPLITTER_OPTIONS, Map.empty[String, String]).asInstanceOf[Map[String, String]]
    val splits = splitter.getSplits(splitterOptions)
    val sortedSplits = ImmutableSortedSet.copyOf(splits.map(_.toString).map(prefixFn).map(new Text(_)))
    tableOps.addSplits(recordTable, sortedSplits)
    // enable the row functor as the feature ID is stored in the Row ID
    tableOps.setProperty(recordTable, "table.bloom.key.functor", classOf[RowFunctor].getCanonicalName)
    tableOps.setProperty(recordTable, "table.bloom.enabled", "true")
  }

  // configure splits for each of the attribute names
  def configureAttrIdxTable(featureType: SimpleFeatureType, attributeIndexTable: String): Unit = {
    val indexedAttrs = SimpleFeatureTypes.getSecondaryIndexedAttributes(featureType)
    if (indexedAttrs.nonEmpty) {
      val prefix = index.getTableSharingPrefix(featureType)
      val prefixFn = AttributeTable.getAttributeIndexRowPrefix(prefix, _: AttributeDescriptor)
      val names = indexedAttrs.map(prefixFn).map(new Text(_))
      val splits = ImmutableSortedSet.copyOf(names.toArray)
      tableOps.addSplits(attributeIndexTable, splits)
    }
  }

  def configureSpatioTemporalIdxTable(maxShard: Int,
                                      featureType: SimpleFeatureType,
                                      tableName: String) {

    if (maxShard > 1) {
      val splits = (1 to maxShard - 1).map(i => new Text(s"%0${maxShard.toString.length}d".format(i)))
      tableOps.addSplits(tableName, new java.util.TreeSet(splits))
    }

    // enable the column-family functor
    tableOps.setProperty(tableName, "table.bloom.key.functor", classOf[ColumnFamilyFunctor].getCanonicalName)
    tableOps.setProperty(tableName, "table.bloom.enabled", "true")
  }

  // Retrieves or computes the indexSchema
  def computeSpatioTemporalSchema(sft: SimpleFeatureType, maxShard: Int): String = {
    val spatioTemporalIdxSchemaFmt: Option[String] = accumulo.index.getIndexSchema(sft)

    spatioTemporalIdxSchemaFmt match {
      case None => buildDefaultSpatioTemporalSchema(getFeatureName(sft), maxShard)
      case Some(schema) =>
        if (maxShard != DEFAULT_MAX_SHARD) {
          logger.warn("Calling create schema with a custom index format AND a custom shard number. " +
            "The custom index format will take precedence.")
        }
        schema
    }
  }

  /**
   * Compute the GeoMesa SpatioTemporal Schema, create tables, and write metadata to catalog.
   * If the schema already exists, log a message and continue without error.
   *
   * @param featureType
   * @param maxShard numerical id of the max shard (creates maxShard + 1 splits)
   */
  def createSchema(featureType: SimpleFeatureType, maxShard: Int) =
    if (getSchema(featureType.getTypeName) == null) {
      val spatioTemporalSchema = computeSpatioTemporalSchema(featureType, maxShard)
      checkSchemaRequirements(featureType, spatioTemporalSchema)
      createTablesForType(featureType, maxShard)
      writeMetadata(featureType, featureEncoding, spatioTemporalSchema)
    }

  // This function enforces the shared ST schema requirements.
  //  For a shared ST table, the IndexSchema must start with a partition number and a constant string.
  //  TODO: This function should check if the constant is equal to the featureType.getTypeName
  def checkSchemaRequirements(featureType: SimpleFeatureType, schema: String) {
    if(accumulo.index.getTableSharing(featureType)) {

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
   * metadata from the catalog. If the table is an older 0.10.x table, we throw an exception.
   *
   * @param featureName the name of the feature
   * @param numThreads the number of concurrent threads to spawn for querying during metadata deletion
   */
  def removeSchema(featureName: String, numThreads: Int = 1) =
    if (metadata.read(featureName, ST_IDX_TABLE_KEY).nonEmpty) {
      val featureType = getSchema(featureName)

      if (accumulo.index.getTableSharing(featureType)) {
        deleteSharedTables(featureType)
      } else {
        deleteStandAloneTables(featureType)
      }

      metadata.delete(featureName, numThreads)
      metadata.expireCache(featureName)
    } else {
      // TODO: Apply the SpatioTemporalTable.deleteFeaturesFromTable here?
      // https://geomesa.atlassian.net/browse/GEOMESA-360
      throw new RuntimeException("Cannot delete schema for this version of the data store")
    }

  private def deleteSharedTables(sft: SimpleFeatureType) = {
    val auths = authorizationsProvider.getAuthorizations
    val numThreads = queryThreadsConfig.getOrElse(Math.min(MAX_QUERY_THREADS,
      Math.max(MIN_QUERY_THREADS, getSpatioTemporalMaxShard(sft))))

    GeoMesaTable.getTablesAndNames(sft, this).foreach { case (table, name) =>
      val deleter = connector.createBatchDeleter(name, auths, numThreads, defaultBWConfig)
      table.deleteFeaturesForType(sft, deleter)
      deleter.close()
    }
  }

  // NB: We are *not* currently deleting the query table and/or query information.
  private def deleteStandAloneTables(sft: SimpleFeatureType) =
    Seq(
      getSpatioTemporalTable(sft),
      getAttributeTable(sft),
      getRecordTable(sft)
    ).filter(tableOps.exists).foreach(tableOps.delete)

  /**
   * Delete everything (all tables) associated with this datastore (index tables and catalog table)
   */
  def delete() = {
    val indexTables =
      getTypeNames.flatMap { t =>
        Seq(getSpatioTemporalTable(t),
            getAttributeTable(t),
            getRecordTable(t))
        }.distinct

    // Delete index tables first then catalog table in case of error
    indexTables.filter(tableOps.exists).foreach(tableOps.delete)
    tableOps.delete(catalogTable)
  }

  /**
   * GeoTools API createSchema() method for a featureType...creates tables with
   * ${numTabletServers} splits. To control the number of splits use the
   * createSchema(featureType, maxShard) method or a custom index schema format.
   *
   * @param featureType
   */
  override def createSchema(featureType: SimpleFeatureType) = createSchema(featureType, DEFAULT_MAX_SHARD)

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
      val visCheck = metadata.readRequiredNoCache(featureName, VISIBILITIES_CHECK_KEY)
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
    if (tableOps.exists(catalogTable)) {
      metadata.getFeatureTypes
    }
    else {
      Array()
    }


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
   * Gets the internal geomesa version number for a given feature type
   *
   * @param sft
   * @return
   */
  def getGeomesaVersion(sft: SimpleFeatureType): Int = getGeomesaVersion(sft.getTypeName)

  /**
   * Gets the internal geomesa version number for a given feature type
   *
   * @param sft
   * @return
   */
  def getGeomesaVersion(sft: String): Int =
    metadata.read(sft, VERSION_KEY).map(_.toInt).getOrElse {
      // back compatible checks for before we wrote the explicit version
      if (metadata.read(sft, ST_IDX_TABLE_KEY).isEmpty) {
        0 // version 0 corresponds to the old 'non-catalog' table format
      } else {
        1 // version 1 corresponds to the split tables with unsorted STIDX
      }
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
    metadata.getTableSize(getRecordTable(featureName))
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
      val dtgField = metadata.read(featureName, DTGFIELD_KEY).getOrElse(accumulo.DEFAULT_DTG_PROPERTY_NAME)
      val indexSchema = metadata.read(featureName, SCHEMA_KEY).orNull
      // If no data is written, we default to 'false' in order to support old tables.
      val sharingBoolean = metadata.read(featureName, SHARED_TABLES_KEY).getOrElse("false")

      sft.getUserData.put(accumulo.index.SF_PROPERTY_START_TIME, dtgField)
      sft.getUserData.put(accumulo.index.SF_PROPERTY_END_TIME, dtgField)
      sft.getUserData.put(accumulo.index.SFT_INDEX_SCHEMA, indexSchema)
      accumulo.index.setTableSharing(sft, new java.lang.Boolean(sharingBoolean))
      sft
    }.orNull
  }

  // Implementation of Abstract method
  def getFeatureReader(featureName: String): AccumuloFeatureReader = getFeatureReader(featureName, Query.ALL)

  // This override is important as it allows us to optimize and plan our search with the Query.
  override def getFeatureReader(featureName: String, query: Query) = {
    val qp = getQueryPlanner(featureName, query, this)
    new AccumuloFeatureReader(qp, query, this)
  }

  /**
   * Gets the query plan for a given query. The query plan consists of the tables, ranges, iterators etc
   * required to run a query against accumulo.
   */
  def getQueryPlan(featureName: String, query: Query): Seq[QueryPlan] =
    planQuery(featureName, query, ExplainNull)

  /**
   * Prints the query plan for a given query to the provided output.
   */
  def explainQuery(featureName: String, query: Query, o: ExplainerOutputType = ExplainPrintln): Unit =
    planQuery(featureName, query, o)

  /**
   *
   */
  private def planQuery(featureName: String, query: Query, o: ExplainerOutputType): Seq[QueryPlan] = {
    val cc = new ExplainingConnectorCreator(this, o)
    val qp = getQueryPlanner(featureName, query, cc)
    qp.planQuery(query, o)
  }

  /**
   * Gets a query planner. Also has side-effect of setting transforms in the query.
   */
  private def getQueryPlanner(featureName: String, query: Query, cc: AccumuloConnectorCreator): QueryPlanner = {
    validateMetadata(featureName)
    val sft = getSchema(featureName)
    val indexSchemaFmt = getIndexSchemaFmt(featureName)
    val featureEncoding = getFeatureEncoding(sft)
    val version = getGeomesaVersion(sft)
    val hints = strategyHints(sft)
    setQueryTransforms(query, sft)
    new QueryPlanner(sft, featureEncoding, indexSchemaFmt, cc, hints, version)
  }

  /* create a general purpose writer that is capable of insert, deletes, and updates */
  override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): SFFeatureWriter = {
    validateMetadata(typeName)
    checkWritePermissions(typeName)
    val sft = getSchema(typeName)
    val fe = SimpleFeatureSerializers(sft, getFeatureEncoding(sft))
    val ive = IndexValueEncoder(sft, getGeomesaVersion(sft))
    new ModifyAccumuloFeatureWriter(sft, fe, ive, this, writeVisibilities, filter)
  }

  /* optimized for GeoTools API to return writer ONLY for appending (aka don't scan table) */
  override def getFeatureWriterAppend(typeName: String,
                                      transaction: Transaction): SFFeatureWriter = {
    validateMetadata(typeName)
    checkWritePermissions(typeName)
    val sft = getSchema(typeName)
    val fe = SimpleFeatureSerializers(sft, getFeatureEncoding(sft))
    val ive = IndexValueEncoder(sft, getGeomesaVersion(sft))
    new AppendAccumuloFeatureWriter(sft, fe, ive, this, writeVisibilities)
  }

  override def getUnsupportedFilter(featureName: String, filter: Filter): Filter = Filter.INCLUDE

  override def getSuggestedSpatioTemporalThreads(sft: SimpleFeatureType): Int = queryThreadsConfig.getOrElse{
    val numShards = getSpatioTemporalMaxShard(sft)
    Math.min(MAX_QUERY_THREADS, Math.max(MIN_QUERY_THREADS, numShards))
  }

  override def getSuggestedAttributeThreads(sft: SimpleFeatureType): Int = 1

  override def getSuggestedRecordThreads(sft: SimpleFeatureType): Int = recordScanThreads

  override def getSuggestedZ3Threads(sft: SimpleFeatureType): Int = queryThreadsConfig.getOrElse(8)

  override def getBatchScanner(table: String, numThreads: Int): BatchScanner =
    connector.createBatchScanner(table, authorizationsProvider.getAuthorizations, numThreads)

  override def getScanner(table: String): Scanner =
    connector.createScanner(table, authorizationsProvider.getAuthorizations)

  // Accumulo assumes that the failures directory exists.  This function assumes that you have already created it.
  def importDirectory(tableName: String, dir: String, failureDir: String, disableGC: Boolean) {
    tableOps.importDirectory(tableName, dir, failureDir, disableGC)
  }

  /**
   * Gets the feature name from a feature type
   *
   * @param featureType
   * @return
   */
  private def getFeatureName(featureType: SimpleFeatureType) = featureType.getName.getLocalPart

  override def strategyHints(sft: SimpleFeatureType) = new UserDataStrategyHints()
}

object AccumuloDataStore {

  /**
   * Format record table name for Accumulo...table name is stored in metadata for other usage
   * and provide compatibility moving forward if table names change
   * @param catalogTable
   * @param featureType
   * @return
   */
  def formatRecordTableName(catalogTable: String, featureType: SimpleFeatureType) =
    formatTableName(catalogTable, featureType, RecordTable.suffix)

  /**
   * Format spatio-temoral index table name for Accumulo...table name is stored in metadata for other usage
   * and provide compatibility moving forward if table names change
   * @param catalogTable
   * @param featureType
   * @return
   */
  def formatSpatioTemporalIdxTableName(catalogTable: String, featureType: SimpleFeatureType) =
    formatTableName(catalogTable, featureType, SpatioTemporalTable.suffix)

  def formatZ3TableName(catalogTable: String, featureType: SimpleFeatureType) =
    formatTableName(catalogTable, featureType.getTypeName, Z3Table.suffix)

  /**
   * Format attribute index table name for Accumulo...table name is stored in metadata for other usage
   * and provide compatibility moving forward if table names change
   * @param catalogTable
   * @param featureType
   * @return
   */
  def formatAttrIdxTableName(catalogTable: String, featureType: SimpleFeatureType) =
    formatTableName(catalogTable, featureType, AttributeTable.suffix)

  /**
   * Format queries table name for Accumulo...table name is stored in metadata for other usage
   * and provide compatibility moving forward if table names change
   * @param catalogTable
   * @param featureType
   * @return
   */
  def formatQueriesTableName(catalogTable: String, featureType: SimpleFeatureType): String =
    s"${catalogTable}_queries"

  // only alphanumeric is safe
  val SAFE_FEATURE_NAME_PATTERN = "^[a-zA-Z0-9]+$"

  /**
   * Format a table name with a namespace. Non alpha-numeric characters present in
   * featureType names will be underscore hex encoded (e.g. _2a) including multibyte
   * UTF8 characters (e.g. _2a_f3_8c) to make them safe for accumulo table names
   * but still human readable.
   */
  def formatTableName(catalogTable: String, featureType: SimpleFeatureType, suffix: String): String =
    if (accumulo.index.getTableSharing(featureType))
      formatTableName(catalogTable, suffix)
    else
      formatTableName(catalogTable, featureType.getTypeName, suffix)

  /**
   * Format a table name for the shared tables
   */
  def formatTableName(catalogTable: String, suffix: String): String =
    s"${catalogTable}_$suffix"

  /**
   * Format a table name with a namespace. Non alpha-numeric characters present in
   * featureType names will be underscore hex encoded (e.g. _2a) including multibyte
   * UTF8 characters (e.g. _2a_f3_8c) to make them safe for accumulo table names
   * but still human readable.
   */
  def formatTableName(catalogTable: String, typeName: String, suffix: String): String = {
    val safeTypeName: String =
      if(typeName.matches(SAFE_FEATURE_NAME_PATTERN)){
        typeName
      } else {
        hexEncodeNonAlphaNumeric(typeName)
      }

    List(catalogTable, safeTypeName, suffix).mkString("_")
  }

  val alphaNumeric = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')

  /**
   * Encode non-alphanumeric characters in a string with
   * underscore plus hex digits representing the bytes. Note
   * that multibyte characters will be represented with multiple
   * underscores and bytes...e.g. _8a_2f_3b
   */
  def hexEncodeNonAlphaNumeric(input: String): String = {
    val sb = new StringBuilder
    input.toCharArray.foreach { c =>
      if (alphaNumeric.contains(c)) {
        sb.append(c)
      } else {
        val encoded =
          Hex.encodeHex(c.toString.getBytes("UTF8")).grouped(2)
            .map{ arr => "_" + arr(0) + arr(1) }.mkString.toLowerCase
        sb.append(encoded)
      }
    }
    sb.toString()
  }
}

