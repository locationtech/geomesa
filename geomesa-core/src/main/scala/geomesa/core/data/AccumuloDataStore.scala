/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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


package geomesa.core.data

import java.util.{Map => JMap}

import com.google.common.collect.ImmutableSortedSet
import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core
import geomesa.core.data.AccumuloDataStore._
import geomesa.core.data.FeatureEncoding.FeatureEncoding
import geomesa.core.index.{IndexSchema, IndexSchemaBuilder, TemporalIndexCheck}
import geomesa.core.security.AuthorizationsProvider
import geomesa.utils.geotools.SimpleFeatureTypes
import geomesa.utils.geotools.SimpleFeatureTypes.{AttributeSpec, NonGeomAttributeSpec}
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.admin.TimeType
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.accumulo.core.data.{Key, Mutation, Range, Value}
import org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.io.Text
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.referencing.crs.CoordinateReferenceSystem

import scala.collection.JavaConversions._

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
                        val spatioTemporalIdxSchemaFmt: Option[String] = None,
                        val queryThreadsConfig: Option[Int] = None,
                        val recordThreadsConfig: Option[Int] = None,
                        val writeThreadsConfig: Option[Int] = None,
                        val featureEncoding: FeatureEncoding = FeatureEncoding.AVRO)
    extends AbstractDataStore(true) with AccumuloConnectorCreator with Logging {

  // having at least as many shards as tservers provides optimal parallelism in queries
  private val DEFAULT_MAX_SHARD = connector.instanceOperations().getTabletServers.size()

  // record scans are single-row ranges - increasing the threads too much actually causes performance to decrease
  private val recordScanThreads = recordThreadsConfig.getOrElse(10)

  private val writeThreads = writeThreadsConfig.getOrElse(10)

  // cap on the number of threads for any one query
  // if we let threads get too high, performance will suffer for simultaneous clients
  private val MAX_QUERY_THREADS = 15

  // floor on the number of query threads, even if the number of shards is 1
  private val MIN_QUERY_THREADS = 5

  // equivalent to: s"%~#s%$maxShard#r%${name}#cstr%0,3#gh%yyyyMMdd#d::%~#s%3,2#gh::%~#s%#id"
  private def buildDefaultSpatioTemporalSchema(name: String, maxShard: Int) =
    new IndexSchemaBuilder("~")
      .randomNumber(maxShard)
      .constant(name)
      .geoHash(0, 3)
      .date("yyyyMMdd")
      .nextPart()
      .geoHash(3, 2)
      .nextPart()
      .id()
      .build()

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  private val validated = scala.collection.mutable.Map[String, String]()

  private val metaDataCache = scala.collection.mutable.HashMap[(String, Text), Option[String]]()

  private val visibilityCheckCache = scala.collection.mutable.Map[(String, String), Boolean]()

  // TODO memory should be configurable
  private val metadataBWConfig =
    new BatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(writeThreads)

  private val MetadataRowKeyRegex = (METADATA_TAG + """_(.*)""").r

  private val tableOps = connector.tableOperations()

  if (!tableOps.exists(catalogTable)) {
    tableOps.create(catalogTable, true, TimeType.LOGICAL)
  }

  /**
   * Computes and writes the metadata for this feature type
   *
   * @param sft
   * @param fe
   */
  private def writeMetadata(sft: SimpleFeatureType,
                            fe: FeatureEncoding,
                            spatioTemporalSchemaValue: String,
                            maxShard: Int) {

    val featureName = getFeatureName(sft)

    // the mutation we'll be writing to
    val mutation = getMetadataMutation(featureName)

    // compute the metadata values
    val attributesValue = SimpleFeatureTypes.encodeType(sft)
    val dtgValue: Option[String] = {
      val userData = sft.getUserData
      // inspect, warn and set SF_PROPERTY_START_TIME if appropriate
      TemporalIndexCheck.extractNewDTGFieldCandidate(sft)
        .foreach { name => userData.put(core.index.SF_PROPERTY_START_TIME, name) }
      if (userData.containsKey(core.index.SF_PROPERTY_START_TIME)) {
        Option(userData.get(core.index.SF_PROPERTY_START_TIME).asInstanceOf[String])
      } else {
        None
      }
    }
    val featureEncodingValue        = /*_*/fe.toString/*_*/
    val spatioTemporalIdxTableValue = formatSpatioTemporalIdxTableName(catalogTable, sft)
    val attrIdxTableValue           = formatAttrIdxTableName(catalogTable, sft)
    val recordTableValue            = formatRecordTableName(catalogTable, sft)
    val dtgFieldValue               = dtgValue.getOrElse(core.DEFAULT_DTG_PROPERTY_NAME)

    // store each metadata in the associated column family
    val attributeMap = Map(ATTRIBUTES_CF        -> attributesValue,
                           SCHEMA_CF            -> spatioTemporalSchemaValue,
                           DTGFIELD_CF          -> dtgFieldValue,
                           FEATURE_ENCODING_CF  -> featureEncodingValue,
                           VISIBILITIES_CF      -> writeVisibilities,
                           ST_IDX_TABLE_CF      -> spatioTemporalIdxTableValue,
                           ATTR_IDX_TABLE_CF    -> attrIdxTableValue,
                           RECORD_TABLE_CF      -> recordTableValue)

    attributeMap.foreach { case (cf, value) =>
      putMetadata(featureName, mutation, cf, value)
    }

    // write out a visibilities protected entry that we can use to validate that a user can see
    // data in this store
    if (!writeVisibilities.isEmpty) {
      mutation.put(VISIBILITIES_CHECK_CF, EMPTY_COLQ, new ColumnVisibility(writeVisibilities),
                    new Value(writeVisibilities.getBytes))
    }

    // write out the mutation
    writeMutations(mutation)
  }

  /**
   * Used to update the attributes that are marked as indexed
   *
   * @param featureName
   * @param attributes
   */
  def updateIndexedAttributes(featureName: String, attributes: String): Unit = {
    val existing = AttributeSpec.toAttributes(getAttributes(featureName))
    val updated = AttributeSpec.toAttributes(attributes)
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
    val mutation = getMetadataMutation(featureName)
    putMetadata(featureName, mutation, ATTRIBUTES_CF, attributes)
    writeMutations(mutation)
  }

  type KVEntry = JMap.Entry[Key,Value]

  /**
   * Read Record table name from store metadata
   */
  def getRecordTableForType(featureType: SimpleFeatureType): String =
    getRecordTableForType(featureType.getTypeName)

  /**
   * Read Record table name from store metadata
   */
  def getRecordTableForType(featureName: String): String =
    readRequiredMetadataItem(featureName, RECORD_TABLE_CF)

  /**
   * Read SpatioTemporal Index table name from store metadata
   */
  def getSpatioTemporalIdxTableName(featureType: SimpleFeatureType): String =
    getSpatioTemporalIdxTableName(featureType.getTypeName)

  /**
   * Read SpatioTemporal Index table name from store metadata
   */
  def getSpatioTemporalIdxTableName(featureName: String): String =
    if (catalogTableFormat(featureName)) {
      readRequiredMetadataItem(featureName, ST_IDX_TABLE_CF)
    } else {
      catalogTable
    }

  /**
   * Read Attribute Index table name from store metadata
   */
  def getAttrIdxTableName(featureType: SimpleFeatureType): String =
    getAttrIdxTableName(featureType.getTypeName)

  /**
   * Read Attribute Index table name from store metadata
   */
  def getAttrIdxTableName(featureName: String): String =
    readRequiredMetadataItem(featureName, ATTR_IDX_TABLE_CF)

  /**
   * Read SpatioTemporal Index table name from store metadata
   */
  def getSpatioTemporalMaxShard(featureType: SimpleFeatureType): Int = {
    val indexSchemaFmt = readMetadataItem(featureType.getTypeName, SCHEMA_CF)
      .getOrElse(throw new RuntimeException(s"Unable to find required metadata property for $SCHEMA_CF"))
    val featureEncoder = getFeatureEncoder(featureType.getTypeName)
    val indexSchema = IndexSchema(indexSchemaFmt, featureType, featureEncoder)
    indexSchema.maxShard
  }

  /**
   * Check if this featureType is stored with catalog table format (i.e. a catalog
   * table with attribute, spatiotemporal, and record tables) or the old style
   * single spatiotemporal table
   *
   * @param featureType
   * @return true if the storage is catalog-style, false if spatiotemporal table only
   */
  def catalogTableFormat(featureType: SimpleFeatureType): Boolean =
    catalogTableFormat(featureType.getTypeName)

  def catalogTableFormat(featureName: String): Boolean =
    readMetadataItem(featureName, ST_IDX_TABLE_CF).nonEmpty

  def createTablesForType(featureType: SimpleFeatureType, maxShard: Int) {
    val spatioTemporalIdxTable = formatSpatioTemporalIdxTableName(catalogTable, featureType)
    val attributeIndexTable    = formatAttrIdxTableName(catalogTable, featureType)
    val recordTable            = formatRecordTableName(catalogTable, featureType)
    
    List(spatioTemporalIdxTable, attributeIndexTable, recordTable).foreach { t =>
      if (!tableOps.exists(t)) {
        connector.tableOperations.create(t, true, TimeType.LOGICAL)
      }
    }

    if (!connector.isInstanceOf[MockConnector]) {
      configureRecordTable(featureType, recordTable)
      configureAttrIdxTable(featureType, attributeIndexTable)
      configureSpatioTemporalIdxTable(maxShard, featureType, spatioTemporalIdxTable)
    }
  }

  // if using UUID as FeatureID, configure splits with hex characters
  private val HEX_SPLITS = "0,1,2,3,4,5,6,7,8,9,A,a,B,b,C,c,D,d,E,e,F,f".split(",").map(s => new Text(s))
  private val RECORDS_SPLITS = ImmutableSortedSet.copyOf(HEX_SPLITS)
  def configureRecordTable(featureType: SimpleFeatureType, recordTable: String): Unit = {
    tableOps.addSplits(recordTable, RECORDS_SPLITS)
  }

  // configure splits for each of the attribute names
  def configureAttrIdxTable(featureType: SimpleFeatureType, attributeIndexTable: String): Unit = {
    val indexedAttrs = SimpleFeatureTypes.getIndexedAttributes(featureType)
    val names = indexedAttrs.map(_.getLocalName).map(new Text(_)).toArray
    val splits = ImmutableSortedSet.copyOf(names)
    tableOps.addSplits(attributeIndexTable, splits)
  }

  def configureSpatioTemporalIdxTable(maxShard: Int,
                                      featureType: SimpleFeatureType,
                                      tableName: String) {

    val splits = (1 to maxShard).map { i => s"%0${maxShard.toString.length}d".format(i) }.map(new Text(_))
    tableOps.addSplits(tableName, new java.util.TreeSet(splits))

    // enable the column-family functor
    tableOps.setProperty(tableName, "table.bloom.key.functor", classOf[ColumnFamilyFunctor].getCanonicalName)
    tableOps.setProperty(tableName, "table.bloom.enabled", "true")
  }

  // Computes the schema, checking for the "DEFAULT" flag
  def computeSpatioTemporalSchema(featureName: String, maxShard: Int): String = {
    spatioTemporalIdxSchemaFmt match {
      case None => buildDefaultSpatioTemporalSchema(featureName, maxShard)
      case Some(schema) => schema
    }
  }

  /**
   * Compute the GeoMesa SpatioTemporal Schema, create tables, and write metadata to catalog
   *
   * @param featureType
   * @param maxShard numerical id of the max shard (creates maxShard + 1 splits)
   */
  def createSchema(featureType: SimpleFeatureType, maxShard: Int) {
    if (maxShard != DEFAULT_MAX_SHARD && spatioTemporalIdxSchemaFmt.isDefined) {
      logger.warn("Calling create schema with a custom index format AND a custom shard number. " +
                  "The custom index format will take precedence.")
    }
    val spatioTemporalSchema = computeSpatioTemporalSchema(getFeatureName(featureType), maxShard)
    createTablesForType(featureType, maxShard)
    writeMetadata(featureType, featureEncoding, spatioTemporalSchema, maxShard)
  }

  /**
   * Deletes the tables from Accumulo created from the Geomesa SpatioTemporal Schema, and deletes
   * metadata from the catalog. If the table is an older 0.10.x table, we throw an exception.
   *
   * @param featureName the name of the feature
   * @param numThreads the number of concurrent threads to spawn for querying during metadata deletion
   */
  def deleteSchema(featureName: String, numThreads: Int = 1) = {
    if (readMetadataItem(featureName, ST_IDX_TABLE_CF).nonEmpty) {
      removeSchema(featureName)
      deleteMetadata(featureName, numThreads)
    } else {
      throw new RuntimeException("Cannot delete schema for this version of the data store")
    }
  }

  /**
   * Retrieves the Geotools SpatioTemporal Schema and deletes the three previously created tables
   *
   * @param featureName the name of the table to query and delete from
   */
  override def removeSchema(featureName: String) = {
    val featureType            = getSchema(featureName)
    val spatioTemporalIdxTable = formatSpatioTemporalIdxTableName(catalogTable, featureType)
    val attributeIndexTable    = formatAttrIdxTableName(catalogTable, featureType)
    val recordTable            = formatRecordTableName(catalogTable, featureType)

    List(spatioTemporalIdxTable, attributeIndexTable, recordTable).foreach { t => if (tableOps.exists(t)) tableOps.delete(t) }
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
   * Handles creating a mutation for writing metadata
   *
   * @param featureName
   * @return
   */
  private def getMetadataMutation(featureName: String) = new Mutation(getMetadataRowKey(featureName))

  /**
   * Handles encoding metadata into a mutation.
   *
   * @param featureName
   * @param mutation
   * @param columnFamily
   * @param value
   */
  private def putMetadata(featureName: String,
                          mutation: Mutation,
                          columnFamily: Text,
                          value: String) {
    mutation.put(columnFamily, EMPTY_COLQ, System.currentTimeMillis(), new Value(value.getBytes))
    // also pre-fetch into the cache
    if (!value.isEmpty) {
      metaDataCache.put((featureName, columnFamily), Some(value))
    }
  }

  /**
   * Handles writing mutations
   *
   * @param mutations
   */
  private def writeMutations(mutations: Mutation*): Unit = {
    val writer = connector.createBatchWriter(catalogTable, metadataBWConfig)
    for (mutation <- mutations) {
      writer.addMutation(mutation)
    }
    writer.flush()
    writer.close()
  }

  /**
   * Handles deleting metadata from the catalog by using the Range obtained from the METADATA_TAG and featureName
   * and setting that as the Range to be handled and deleted by Accumulo's BatchDeleter
   *
   * @param featureName the name of the table to query and delete from
   * @param numThreads the number of concurrent threads to spawn for querying
   */
  private def deleteMetadata(featureName: String, numThreads: Int): Unit = {
    val range = new Range(s"${METADATA_TAG}_$featureName")
    val deleter = connector.createBatchDeleter(catalogTable, authorizationsProvider.getAuthorizations, numThreads, metadataBWConfig)
    deleter.setRanges(List(range))
    deleter.delete()
    deleter.close()
  }

  /**
   * Validates the configuration of this data store instance against the stored configuration in
   * Accumulo, if any. This is used to ensure that the visibilities (in particular) do not change
   * from one instance to the next. This will fill in any missing metadata that may occur in an
   * older (0.1.0) version of a table.
   */
  protected def validateMetadata(featureName: String): Unit = {
    readMetadataItem(featureName, ATTRIBUTES_CF)
      .getOrElse(throw new RuntimeException(s"Feature '$featureName' has not been initialized. Please call 'createSchema' first."))

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
    val checks = List(checkVisibilitiesMetadata(featureName), checkSchemaMetadata(featureName))

    val errors = checks.flatten.mkString(", ")

    // if no errors, check the feature encoding and update if needed
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
    val storedVisibilities = readMetadataItem(featureName, VISIBILITIES_CF).getOrElse("")
    if (storedVisibilities != writeVisibilities) {
      Some(s"$VISIBILITIES_CF = '$writeVisibilities', should be '$storedVisibilities'")
    } else {
      None
    }
  }

  /**
   * Checks the schema stored in the metadata table against the configuration of this data store.
   *
   * @param featureName
   * @return
   */
  private def checkSchemaMetadata(featureName: String): Option[String] = {
    // validate the index schema
    val configuredSchema = computeSpatioTemporalSchema(featureName, DEFAULT_MAX_SHARD)
    val storedSchema = readMetadataItem(featureName, SCHEMA_CF).getOrElse("")
    // if they did not specify a custom schema (e.g. indexSchemaFormat == DEFAULT), just use the
    // stored metadata
    if (storedSchema != configuredSchema && spatioTemporalIdxSchemaFmt.isDefined) {
      Some(s"$SCHEMA_CF = '$configuredSchema', should be '$storedSchema'")
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
    // for feature encoding, we are more lenient - we will use whatever is stored in the table,
    // or default to 'text' for backwards compatibility
    if (readMetadataItem(featureName, FEATURE_ENCODING_CF).getOrElse("").isEmpty) {
      // if there is nothing in the table, it means the table was created with an older version of
      // geomesa - we'll update the data in the table to be 1.0 compliant
      val mutation = getMetadataMutation(featureName)
      putMetadata(featureName, mutation, FEATURE_ENCODING_CF, FeatureEncoding.TEXT.toString)
      writeMutations(mutation)
    }
  }

  /**
   * Checks whether the current user can write - based on whether they can read data in this
   * data store.
   *
   * @param featureName
   */
  protected def checkWritePermissions(featureName: String): Unit = {
    val visibilities = readMetadataItem(featureName, VISIBILITIES_CF).getOrElse("")
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
      val visCheck = readMetadataItemNoCache(featureName, VISIBILITIES_CHECK_CF)
                      .isInstanceOf[Some[String]]
      visibilityCheckCache.put((featureName, authString), visCheck)
      visCheck
    })
  }

  /**
   * Creates the row id for a metadata entry
   *
   * @param featureName
   * @return
   */
  private def getMetadataRowKey(featureName: String) = new Text(METADATA_TAG + "_" + featureName)

  /**
   * Reads metadata from cache or scans if not available
   *
   * @param featureName
   * @param colFam
   * @return
   */
  private def readMetadataItem(featureName: String, colFam: Text): Option[String] =
    metaDataCache.getOrElse((featureName, colFam), {
      val result = readMetadataItemNoCache(featureName, colFam)
      metaDataCache.put((featureName, colFam), result)
      result
    })

  private def readRequiredMetadataItem(featureName: String, colFam: Text): String =
    readMetadataItem(featureName, colFam)
      .getOrElse(throw new RuntimeException(s"Unable to find required metadata property for $colFam"))

  private def readRequiredMetadataItem(featureType: SimpleFeatureType, colFam: Text): String =
    readRequiredMetadataItem(featureType.getTypeName, colFam)

  /**
   * Create an Accumulo Scanner to the Catalog table to query Metadata for this store
   */
  def createCatalogScanner = connector.createScanner(catalogTable, authorizationsProvider.getAuthorizations)

  /**
   * Gets metadata by scanning the table, without the local cache
   *
   * Read metadata using scheme:  ~METADATA_featureName metadataFieldName: insertionTimestamp metadataValue
   *
   * @param featureName
   * @param colFam
   * @return
   */
  private def readMetadataItemNoCache(featureName: String, colFam: Text): Option[String] = {
    val scanner = createCatalogScanner
    scanner.setRange(new Range(s"${METADATA_TAG}_$featureName"))
    scanner.fetchColumn(colFam, EMPTY_COLQ)

    val name = "version-" + featureName + "-" + colFam.toString
    val cfg = new IteratorSetting(1, name, classOf[VersioningIterator])
    VersioningIterator.setMaxVersions(cfg, 1)
    scanner.addScanIterator(cfg)

    val iter = scanner.iterator
    val result =
      if (iter.hasNext) {
        Some(iter.next.getValue.toString)
      } else {
        None
      }

    scanner.close()
    result
  }

  /**
   * Implementation of AbstractDataStore getTypeNames
   *
   * @return
   */
  override def getTypeNames: Array[String] =
    if (tableOps.exists(catalogTable)) {
      readTypesFromMetadata
    }
    else {
      Array()
    }

  /**
   * Scans metadata rows and pulls out the different feature types in the table
   *
   * @return
   */
  private def readTypesFromMetadata: Array[String] = {
    val scanner = createCatalogScanner
    scanner.setRange(new Range(METADATA_TAG, METADATA_TAG_END))
    // restrict to just schema cf so we only get 1 hit per feature
    scanner.fetchColumnFamily(SCHEMA_CF)
    val resultItr = new Iterator[String] {
      val src = scanner.iterator()

      def hasNext = {
        val next = src.hasNext
        if (!next) {
          scanner.close()
        }
        next
      }

      def next() = src.next().getKey.getRow.toString
    }
    resultItr.toArray.map(getFeatureNameFromMetadataRowKey)
  }

  /**
   * Reads the feature name from a given metadata row key
   *
   * @param rowKey
   * @return
   */
  private def getFeatureNameFromMetadataRowKey(rowKey: String): String = {
    val MetadataRowKeyRegex(featureName) = rowKey
    featureName
  }

  // NB:  By default, AbstractDataStore is "isWriteable".  This means that createFeatureSource returns
  // a featureStore
  override def getFeatureSource(featureName: String): SimpleFeatureSource = {
    validateMetadata(featureName)
    new AccumuloFeatureStore(this, featureName)
  }

  /**
   * Reads the index schema format out of the metadata
   *
   * @param featureName
   * @return
   */
  def getIndexSchemaFmt(featureName: String) =
    readMetadataItem(featureName, SCHEMA_CF).getOrElse(EMPTY_STRING)

  /**
   * Reads the attributes out of the metadata
   *
   * @param featureName
   * @return
   */
  private def getAttributes(featureName: String) =
    readMetadataItem(featureName, ATTRIBUTES_CF).getOrElse(EMPTY_STRING)

  /**
   * Reads the feature encoding from the metadata. Defaults to TEXT if there is no metadata.
   *
   * @param featureName
   * @return
   */
  def getFeatureEncoder(featureName: String) = {
    val encodingString = readMetadataItem(featureName, FEATURE_ENCODING_CF)
                         .getOrElse(FeatureEncoding.TEXT.toString)
    SimpleFeatureEncoderFactory.createEncoder(encodingString)
  }

  // We assume that they want the bounds for everything.
  override def getBounds(query: Query): ReferencedEnvelope = {
    val env = readMetadataItem(query.getTypeName, BOUNDS_CF).getOrElse(WHOLE_WORLD_BOUNDS)
    val minMaxXY = env.split(":")
    val curBounds = minMaxXY.size match {
      case 4 => env
      case _ => WHOLE_WORLD_BOUNDS
    }
    val sft = getSchema(query.getTypeName)
    val crs = sft.getCoordinateReferenceSystem
    stringToReferencedEnvelope(curBounds, crs)
  }

  private def stringToReferencedEnvelope(string: String,
                                         crs: CoordinateReferenceSystem): ReferencedEnvelope = {
    val minMaxXY = string.split(":")
    require(minMaxXY.size == 4)
    new ReferencedEnvelope(minMaxXY(0).toDouble, minMaxXY(1).toDouble, minMaxXY(2).toDouble,
                            minMaxXY(3).toDouble, crs)
  }

  /**
   * Writes bounds for this feature
   *
   * @param featureName
   * @param bounds
   */
  def writeBounds(featureName: String, bounds: ReferencedEnvelope) {
    // prepare to write out properties to the Accumulo SHP-file table
    val newbounds = readMetadataItem(featureName, BOUNDS_CF) match {
      case Some(env) => getNewBounds(env, featureName, bounds)
      case None      => bounds
    }

    val minMaxXY = List(newbounds.getMinX, newbounds.getMaxX, newbounds.getMinY, newbounds.getMaxY)
    val encoded = minMaxXY.mkString(":")

    val mutation = getMetadataMutation(featureName)
    putMetadata(featureName, mutation, BOUNDS_CF, encoded)
    writeMutations(mutation)
  }

  private def getNewBounds(env: String, featureName: String, bounds: ReferencedEnvelope) = {
    val oldBounds = stringToReferencedEnvelope(env,
                                                getSchema(featureName).getCoordinateReferenceSystem)
    val projBounds = bounds.transform(oldBounds.getCoordinateReferenceSystem, true)
    projBounds.expandToInclude(oldBounds)
    projBounds
  }

  /**
   * Implementation of abstract method
   *
   * @param featureName
   * @return the corresponding feature type (schema) for this feature name,
   *         or NULL if this feature name does not appear to exist
   */
  override def getSchema(featureName: String): SimpleFeatureType =
    getAttributes(featureName) match {
      case attributes if attributes.isEmpty =>
        null
      case attributes                       =>
        val sft = SimpleFeatureTypes.createType(featureName, attributes)
        val dtgField = readMetadataItem(featureName, DTGFIELD_CF)
          .getOrElse(core.DEFAULT_DTG_PROPERTY_NAME)
        sft.getUserData.put(core.index.SF_PROPERTY_START_TIME, dtgField)
        sft.getUserData.put(core.index.SF_PROPERTY_END_TIME, dtgField)
        sft
    }

  // Implementation of Abstract method
  def getFeatureReader(featureName: String): AccumuloFeatureReader = getFeatureReader(featureName,
                                                                                       Query.ALL)

  // This override is important as it allows us to optimize and plan our search with the Query.
  override def getFeatureReader(featureName: String, query: Query) = {
    validateMetadata(featureName)
    val indexSchemaFmt = getIndexSchemaFmt(featureName)
    val sft = getSchema(featureName)
    val fe = getFeatureEncoder(featureName)
    new AccumuloFeatureReader(this, query, indexSchemaFmt, sft, fe)
  }

  /* create a general purpose writer that is capable of insert, deletes, and updates */
  override def createFeatureWriter(typeName: String, transaction: Transaction): SFFeatureWriter = {
    validateMetadata(typeName)
    checkWritePermissions(typeName)
    val featureType = getSchema(typeName)
    val indexSchemaFmt = getIndexSchemaFmt(typeName)
    val fe = getFeatureEncoder(typeName)
    val schema = IndexSchema(indexSchemaFmt, featureType, fe)
    new ModifyAccumuloFeatureWriter(featureType, schema, connector, fe, writeVisibilities, this)
  }

  /* optimized for GeoTools API to return writer ONLY for appending (aka don't scan table) */
  override def getFeatureWriterAppend(typeName: String,
                                      transaction: Transaction): SFFeatureWriter = {
    validateMetadata(typeName)
    checkWritePermissions(typeName)
    val featureType = getSchema(typeName)
    val indexSchemaFmt = getIndexSchemaFmt(typeName)
    val fe = getFeatureEncoder(typeName)
    val schema = IndexSchema(indexSchemaFmt, featureType, fe)
    new AppendAccumuloFeatureWriter(featureType, schema, connector, fe, writeVisibilities, this)
  }

  override def getUnsupportedFilter(featureName: String, filter: Filter): Filter = Filter.INCLUDE

  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   *
   * @param numThreads number of threads for the BatchScanner
   */
  def createSpatioTemporalIdxScanner(sft: SimpleFeatureType, numThreads: Int): BatchScanner = {
    logger.trace(s"Creating ST batch scanner with $numThreads threads")
    if (catalogTableFormat(sft)) {
      connector.createBatchScanner(getSpatioTemporalIdxTableName(sft), 
                                   authorizationsProvider.getAuthorizations, 
                                   numThreads)
    } else {
      connector.createBatchScanner(catalogTable, authorizationsProvider.getAuthorizations, numThreads)
    }
  }

  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   */
  def createSTIdxScanner(sft: SimpleFeatureType): BatchScanner = {
    // use provided thread count, or the number of shards (with min/max checks)
    val numThreads = queryThreadsConfig.getOrElse(Math.min(MAX_QUERY_THREADS,
                       Math.max(MIN_QUERY_THREADS, getSpatioTemporalMaxShard(sft))))
    createSpatioTemporalIdxScanner(sft, numThreads)
  }

  /**
   * Create a Scanner for the Attribute Table (Inverted Index Table)
   */
  def createAttrIdxScanner(sft: SimpleFeatureType) =
    if (catalogTableFormat(sft)) {
      connector.createScanner(getAttrIdxTableName(sft), authorizationsProvider.getAuthorizations)
    } else {
      throw new RuntimeException("Cannot create Attribute Index Scanner - " +
        "attribute index table does not exist for this version of the data store")
    }

  /**
   * Create a BatchScanner to retrieve only Records (SimpleFeatures)
   */
  def createRecordScanner(sft: SimpleFeatureType, numThreads: Int = recordScanThreads) = {
    logger.trace(s"Creating record scanne with $numThreads threads")
    if (catalogTableFormat(sft)) {
      connector.createBatchScanner(getRecordTableForType(sft), authorizationsProvider.getAuthorizations, numThreads)
    } else {
      throw new RuntimeException("Cannot create Record Scanner - record table does not exist for this version" +
        "of the datastore")
    }
  }

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
}

object AccumuloDataStore {

  // Format record table name for Accumulo...table name is stored in metadata for other usage
  // and provide compatibility moving forward if table names change
  def formatRecordTableName(catalogTable: String, featureType: SimpleFeatureType) =
    formatTableName(catalogTable, featureType, "records")

  // Format record table name for Accumulo...table name is stored in metadata for other usage
  // and provide compatibility moving forward if table names change
  def formatSpatioTemporalIdxTableName(catalogTable: String, featureType: SimpleFeatureType) =
    formatTableName(catalogTable, featureType, "st_idx")

  // Format record table name for Accumulo...table name is stored in metadata for other usage
  // and provide compatibility moving forward if table names change
  def formatAttrIdxTableName(catalogTable: String, featureType: SimpleFeatureType) =
    formatTableName(catalogTable, featureType, "attr_idx")

  // only alphanumeric is safe
  val SAFE_FEATURE_NAME_PATTERN = "^[a-zA-Z0-9]+$"

  /**
   * Format a table name with a namespace. Non alpha-numeric characters present in
   * featureType names will be underscore hex encoded (e.g. _2a) including multibyte
   * UTF8 characters (e.g. _2a_f3_8c) to make them safe for accumulo table names
   * but still human readable.
   */
  def formatTableName(catalogTable: String, featureType: SimpleFeatureType, suffix: String): String =
    formatTableName(catalogTable, featureType.getTypeName, suffix)

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
    sb.toString
  }


}

