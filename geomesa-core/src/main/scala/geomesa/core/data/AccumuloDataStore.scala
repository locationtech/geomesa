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

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core
import geomesa.core.data.AccumuloFeatureWriter.{LocalRecordDeleter, LocalRecordWriter, MapReduceRecordWriter}
import geomesa.core.data.FeatureEncoding.FeatureEncoding
import geomesa.core.index.{TemporalIndexCheck, Constants, IndexSchema}
import geomesa.core.security.AuthorizationsProvider
import java.io.{IOException, Serializable}
import java.util.{Map => JMap}
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.data.{Mutation, Value, Range}
import org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.referencing.crs.CoordinateReferenceSystem
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 *
 * @param connector        Accumulo connector
 * @param tableName        The name of the Accumulo table contains the various features
 * @param authorizationsProvider   Provides the authorizations used to access data
 * @param writeVisibilities   Visibilities applied to any data written by this store
 *
 *  This class handles DataStores which are stored in Accumulo Tables.  To be clear, one table may
 *  contain multiple features addressed by their featureName.
 */
class AccumuloDataStore(val connector: Connector, val tableName: String,
                        val authorizationsProvider: AuthorizationsProvider,
                        val writeVisibilities: String, val indexSchemaFormat: String = "DEFAULT",
                        val featureEncoding: FeatureEncoding = FeatureEncoding.AVRO)
    extends AbstractDataStore(true) with Logging {

  private def buildDefaultSchema(name: String) =
    s"%~#s%99#r%${name}#cstr%0,3#gh%yyyyMMdd#d::%~#s%3,2#gh::%~#s%#id"

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  private val validated = scala.collection.mutable.Map[String, String]()

  private val metaDataCache = scala.collection.mutable.HashMap[(String, Text), Option[String]]()

  private val visibilityCheckCache = scala.collection.mutable.Map[(String, String), Boolean]()

  // TODO config should be configurable...
  private val batchWriterConfig =
    new BatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(10)

  private val MetadataRowKeyRegex = (METADATA_TAG + """_(.*)""").r

  private val tableOps = connector.tableOperations()

  /**
   * Creates the schema for the feature type. This will create the table in accumulo, if it doesn't
   * exist. It will configure splits for the table based on the feature type. Note that if the table
   * has already been configured for a different feature type (i.e. multiple features in one table)
   * the splits from the previous feature will be used.
   *
   * @param featureType
   */
  override def createSchema(featureType: SimpleFeatureType) {
    val indexSchema = getIndexSchemaString(featureType.getTypeName)
    createAndConfigureTable(featureType, featureEncoding, indexSchema)
    writeMetadata(featureType, featureEncoding, indexSchema)
  }

  /**
   * Creates and configures the accumulo table for this feature. Note that if the table has already
   * been configured for a different feature type (i.e. multiple features in one table)
   * the splits from the previous feature will be used.
   *
   * If the schema already exists for this feature type, it will throw an exception.
   *
   * @param featureType
   * @param featureEncoding
   */
  private def createAndConfigureTable(featureType: SimpleFeatureType,
                                      featureEncoding: FeatureEncoding,
                                      indexSchemaString: String): Unit = {
    if (!tableOps.exists(tableName))
      connector.tableOperations.create(tableName)

    val featureName = getFeatureName(featureType)

    if (!getAttributes(featureName).isEmpty)
      throw new IOException(s"Schema already exists for feature type $featureName")

    // mock connector - skip configuration
    if (connector.isInstanceOf[MockConnector])
      return

    // configure table splits
    val existingSplits = tableOps.listSplits(tableName)
    if (existingSplits == null || existingSplits.isEmpty) {
      val encoder = SimpleFeatureEncoderFactory.createEncoder(featureEncoding)
      val indexSchema = IndexSchema(indexSchemaString, featureType, encoder)
      val maxShard = indexSchema.maxShard

      val splits = (1 to maxShard).map { i => s"%0${maxShard.toString.length }d".format(i) }
                   .map(new Text(_))
      tableOps.addSplits(tableName, new java.util.TreeSet(splits))
    } else
        logger.warn(s"Table $tableName has pre-existing splits which will be used: $existingSplits")

    // enable the column-family functor
    tableOps.setProperty(tableName, "table.bloom.key.functor",
                          classOf[ColumnFamilyFunctor].getCanonicalName)
    tableOps.setProperty(tableName, "table.bloom.enabled", "true")

    // isolate various metadata elements in locality groups
    tableOps.setLocalityGroups(tableName, Map(ATTRIBUTES_CF.toString -> Set(ATTRIBUTES_CF).asJava,
                                               SCHEMA_CF.toString -> Set(SCHEMA_CF).asJava,
                                               BOUNDS_CF.toString -> Set(BOUNDS_CF).asJava))

  }

  /**
   * Computes and writes the metadata for this feature type
   *
   * @param sft
   * @param fe
   */
  private def writeMetadata(sft: SimpleFeatureType, fe: FeatureEncoding,
                            indexSchemaString: String): Unit = {

    val featureName = getFeatureName(sft)

    // the mutation we'll be writing to
    val mutation = getMetadataMutation(featureName)

    // compute the metadata values
    val attributesValue = DataUtilities.encodeType(sft)
    val schemaValue = indexSchemaString
    val dtgValue: Option[String] = {
      val userData = sft.getUserData
      // inspect, warn and set SF_PROPERTY_START_TIME if appropriate
      TemporalIndexCheck.extractNewDTGFieldCandidate(sft)
        .foreach { name => userData.put(core.index.SF_PROPERTY_START_TIME, name) }
      if (userData.containsKey(core.index.SF_PROPERTY_START_TIME))
        Option(userData.get(core.index.SF_PROPERTY_START_TIME).asInstanceOf[String])
      else
        None
    }
    val featureEncodingValue = fe.toString

    // store each metadata in the associated column family
    val attributeMap = Map(ATTRIBUTES_CF          -> attributesValue,
                            SCHEMA_CF             -> schemaValue,
                            DTGFIELD_CF           -> dtgValue.getOrElse(core.DEFAULT_DTG_PROPERTY_NAME),
                            FEATURE_ENCODING_CF   -> featureEncodingValue,
                            VISIBILITIES_CF       -> writeVisibilities)

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
  private def putMetadata(featureName: String, mutation: Mutation, columnFamily: Text,
                          value: String): Unit = {
    mutation.put(columnFamily, EMPTY_COLQ, System.currentTimeMillis(), new Value(value.getBytes))
    // also pre-fetch into the cache
    if (!value.isEmpty)
      metaDataCache.put((featureName, columnFamily), Some(value))
  }

  /**
   * Handles writing mutations
   *
   * @param mutations
   */
  private def writeMutations(mutations: Mutation*): Unit = {
    val writer = connector.createBatchWriter(tableName, batchWriterConfig)
    for (mutation <- mutations) {
      writer.addMutation(mutation)
    }
    writer.flush()
    writer.close()
  }

  /**
   * Gets the index schema formatted string for this feature
   *
   * @param featureName
   * @return
   */
  private def getIndexSchemaString(featureName: String): String = {
    indexSchemaFormat match {
      case "DEFAULT" => buildDefaultSchema(featureName)
      case _         => indexSchemaFormat
    }
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

    if (!ok.isEmpty)
      throw new RuntimeException("Configuration of this DataStore does not match the schema values: " + ok)
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
    if (storedVisibilities != writeVisibilities)
      Some(s"$VISIBILITIES_CF = '$writeVisibilities', should be '$storedVisibilities'")
    else
      None
  }

  /**
   * Checks the schema stored in the metadata table against the configuration of this data store.
   *
   * @param featureName
   * @return
   */
  private def checkSchemaMetadata(featureName: String): Option[String] = {
    // validate the index schema
    val configuredSchema = getIndexSchemaString(featureName)
    val storedSchema = readMetadataItem(featureName, SCHEMA_CF).getOrElse("")
    // if they did not specify a custom schema (e.g. indexSchemaFormat == DEFAULT), just use the
    // stored metadata
    if (storedSchema != configuredSchema && indexSchemaFormat != "DEFAULT")
      Some(s"$SCHEMA_CF = '$configuredSchema', should be '$storedSchema'")
    else
      None
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
        throw new RuntimeException(s"The current user does not have the required authorizations to write $featureName features. Required authorizations: '$visibilities', actual authorizations: '$authString'")
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
    val scanner = createScanner
    scanner.setRange(new Range(s"${METADATA_TAG }_$featureName"))
    scanner.fetchColumn(colFam, EMPTY_COLQ)

    val name = "version-" + featureName + "-" + colFam.toString
    val cfg = new IteratorSetting(1, name, classOf[VersioningIterator])
    VersioningIterator.setMaxVersions(cfg, 1)
    scanner.addScanIterator(cfg)

    val iter = scanner.iterator
    val result =
      if (iter.hasNext) Some(iter.next.getValue.toString)
      else None

    scanner.close()
    result
  }

  /**
   * Implementation of AbstractDataStore getTypeNames
   *
   * @return
   */
  override def getTypeNames: Array[String] =
    if (tableOps.exists(tableName)) readTypesFromMetadata
    else Array()

  /**
   * Scans metadata rows and pulls out the different feature types in the table
   *
   * @return
   */
  private def readTypesFromMetadata: Array[String] = {
    val scanner = createScanner
    scanner.setRange(new Range(METADATA_TAG, METADATA_TAG_END))
    // restrict to just schema cf so we only get 1 hit per feature
    scanner.fetchColumnFamily(SCHEMA_CF)
    val resultItr = new Iterator[String] {
      val src = scanner.iterator()

      def hasNext = {
        val next = src.hasNext
        if (!next)
          scanner.close()
        next
      }

      def next() = src.next().getKey.getRow.toString
    }
    resultItr.toArray.map(getFeatureNameFromMetadataRowKey(_))
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
  protected def getIndexSchemaFmt(featureName: String) =
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
   * @return
   */
  override def getSchema(featureName: String): SimpleFeatureType = {
    val sft = DataUtilities.createType(featureName, getAttributes(featureName))
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
    val writer = new LocalRecordWriter(tableName, connector)
    val deleter = new LocalRecordDeleter(tableName, connector)
    new ModifyAccumuloFeatureWriter(featureType, schema, writer, writeVisibilities, deleter, this)
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
    val writer = new LocalRecordWriter(tableName, connector)
    new AppendAccumuloFeatureWriter(featureType, schema, writer, writeVisibilities)
  }

  override def getUnsupportedFilter(featureName: String, filter: Filter): Filter = Filter.INCLUDE

  /**
   * Creates a scanner for the table underlying this data store
   *
   * @return
   */
  def createBatchScanner(): BatchScanner = {
    connector.createBatchScanner(tableName, authorizationsProvider.getAuthorizations, 100)
  }

  /**
   * Creates a scanner for the table underlying this data store
   *
   * @return
   */
  def createScanner: Scanner = {
    connector.createScanner(tableName, authorizationsProvider.getAuthorizations)
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

/**
 *
 * @param connector        Accumulo connector
 * @param tableName        The name of the Accumulo table contains the various features
 * @param authorizationsProvider   Provides the authorizations used to access data
 * @param writeVisibilities visibilities to be applied to any data written by this store
 * @param params           The parameters used to create this datastore.
 *
 *                         This class provides an additional writer which can be accessed by
 *                         createMapReduceFeatureWriter(featureName, context)
 *
 *                         This writer is appropriate for use inside a MapReduce job.  We explicitly do not override the default
 *                         createFeatureWriter so that we have both available.
 */
class MapReduceAccumuloDataStore(connector: Connector, tableName: String,
                                 authorizationsProvider: AuthorizationsProvider,
                                 writeVisibilities: String, val params: JMap[String, Serializable],
                                 indexSchemaFormat: String = "DEFAULT",
                                 featureEncoding: FeatureEncoding = FeatureEncoding.AVRO)
    extends AccumuloDataStore(connector, tableName, authorizationsProvider, writeVisibilities,
                               indexSchemaFormat, featureEncoding) {

  override def getFeatureSource(featureName: String): SimpleFeatureSource = {
    validateMetadata(featureName)
    checkWritePermissions(featureName)
    new MapReduceAccumuloFeatureStore(this, featureName)
  }

  def createMapReduceFeatureWriter(featureName: String, context: TASKIOCTX): SFFeatureWriter = {
    validateMetadata(featureName)
    checkWritePermissions(featureName)
    val featureType = getSchema(featureName)
    val idxFmt = getIndexSchemaFmt(featureName)
    val fe = getFeatureEncoder(featureName)
    val idx = IndexSchema(idxFmt, featureType, fe)
    val writer = new MapReduceRecordWriter(context)
    // TODO allow deletes? modifications?
    new AppendAccumuloFeatureWriter(featureType, idx, writer, writeVisibilities)
  }

}
