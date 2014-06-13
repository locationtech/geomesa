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

import geomesa.core
import geomesa.core.data.AccumuloFeatureWriter.{LocalRecordDeleter, LocalRecordWriter, MapReduceRecordWriter}
import geomesa.core.data.FeatureEncoding.FeatureEncoding
import geomesa.core.index.{TemporalIndexCheck, Constants, IndexSchema}
import geomesa.core.security.AuthorizationsProvider
import java.io.Serializable
import java.util.{Map=>JMap}
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.client.{BatchWriterConfig, IteratorSetting, Connector}
import org.apache.accumulo.core.data.{Key, Mutation, Value, Range}
import org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.hadoop.io.Text
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.Hints
import org.geotools.feature.NameImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.`type`.Name
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
 * This class handles DataStores which are stored in Accumulo Tables.  To be clear, one table may contain multiple
 * features addressed by their featureName.
 */
class AccumuloDataStore(val connector: Connector,
                        val tableName: String,
                        val authorizationsProvider: AuthorizationsProvider,
                        val writeVisibilities: String,
                        val indexSchemaFormat: String = "DEFAULT",
                        val featureEncoding: FeatureEncoding = FeatureEncoding.AVRO)
  extends AbstractDataStore(true) {

  private def buildDefaultSchema(name: String) =
    s"%~#s%99#r%${name}#cstr%0,3#gh%yyyyMMdd#d::%~#s%3,2#gh::%~#s%#id"

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  val tableOps = connector.tableOperations()
  type KVEntry = JMap.Entry[Key,Value]

  def createTableIfNotExists(tableName: String,
                             featureType: SimpleFeatureType,
                             featureEncoding: FeatureEncoding) {
    if (!tableOps.exists(tableName))
      connector.tableOperations.create(tableName)

    if(!connector.isInstanceOf[MockConnector])
      configureNewTable(createIndexSchema(featureType), featureType, tableName, featureEncoding)
  }

  def configureNewTable(indexSchemaFormat: String, featureType: SimpleFeatureType, tableName: String, fe: FeatureEncoding) {
    val encoder = SimpleFeatureEncoderFactory.createEncoder(fe)
    val indexSchema = IndexSchema(indexSchemaFormat, featureType, encoder)
    val maxShard = indexSchema.maxShard

    val splits = (1 to maxShard).map { i => s"%0${maxShard.toString.length}d".format(i) }.map(new Text(_))
    tableOps.addSplits(tableName, new java.util.TreeSet(splits))

    // enable the column-family functor
    tableOps.setProperty(tableName, "table.bloom.key.functor", classOf[ColumnFamilyFunctor].getCanonicalName)
    tableOps.setProperty(tableName, "table.bloom.enabled", "true")

    // isolate various metadata elements in locality groups
    tableOps.setLocalityGroups(tableName,
      Map(
        ATTRIBUTES_CF.toString -> Set(ATTRIBUTES_CF).asJava,
        SCHEMA_CF.toString     -> Set(SCHEMA_CF).asJava,
        BOUNDS_CF.toString     -> Set(BOUNDS_CF).asJava))
  }

  override def createSchema(featureType: SimpleFeatureType) {
    // Attempt to determine the encoding before possibly creating the table
    val properEncoding = determineProperEncoding(featureType, featureEncoding)
    createTableIfNotExists(tableName, featureType, properEncoding)
    writeMetadata(featureType, properEncoding)
  }

  // If the table exists already use the feature encoding stored as metadata instead of
  // the user provided feature encoding...if table exists but doesn't have metadata for
  // feature encoding then default to TEXT for backwards compatability
  def determineProperEncoding(featureType: SimpleFeatureType, providedEncoding: FeatureEncoding) = {
    if (tableOps.exists(tableName)) getFeatureEncoder(featureType.getTypeName).getEncoding
    else providedEncoding
  }

  def writeMetadata(sft: SimpleFeatureType, fe: FeatureEncoding) {
    val featureName = sft.getName.getLocalPart
    val attributesValue = new Value(DataUtilities.encodeType(sft).getBytes)
    writeMetadataItem(featureName, ATTRIBUTES_CF, attributesValue)
    val schemaValue = createIndexSchema(sft)
    writeMetadataItem(featureName, SCHEMA_CF, new Value(schemaValue.getBytes))
    TemporalIndexCheck.checkForValidDtgField(sft)
    val userData = sft.getUserData
    if(userData.containsKey(core.index.SF_PROPERTY_START_TIME)) {
      val dtgField = userData.get(core.index.SF_PROPERTY_START_TIME)
      writeMetadataItem(featureName, DTGFIELD_CF, new Value(dtgField.asInstanceOf[String].getBytes))
    }
    writeMetadataItem(featureName, FEATURE_ENCODING_CF, new Value(fe.toString.getBytes()))
  }

  def createIndexSchema(sft: SimpleFeatureType) = indexSchemaFormat match {
    case "DEFAULT" => buildDefaultSchema(sft.getTypeName)
    case _         => indexSchemaFormat
  }

  def getMetadataRowID(featureName: String) = new Text(METADATA_TAG + "_" + featureName)

  val MetadataRowIDRegex = (METADATA_TAG + """_(.*)""").r

  def getFeatureNameFromMetadataRowID(rowID: String): String = {
    val MetadataRowIDRegex(featureName) = rowID
    featureName
  }

  private val metaDataCache = scala.collection.mutable.HashMap[(String, Text), Option[String]]()

  val batchWriterConfig =
    new BatchWriterConfig()
      .setMaxMemory(10000L)
      .setMaxWriteThreads(10)

  // record a single piece of metadata
  private def writeMetadataItem(featureName: String, colFam: Text, metadata: Value) {
    val writer = connector.createBatchWriter(tableName, batchWriterConfig)
    val mutation = new Mutation(getMetadataRowID(featureName))
    mutation.put(colFam, EMPTY_COLQ, System.currentTimeMillis(), metadata)
    writer.addMutation(mutation)
    writer.flush()
    writer.close()

    // pre-fetch this into cache
    metaDataCache.put((featureName, colFam), Option(metadata).map(_.toString))
  }

  // Read metadata using scheme:  ~METADATA_featureName metadataFieldName: insertionTimestamp metadataValue
  private def readMetadataItem(featureName: String, colFam: Text): Option[String] =
    metaDataCache.getOrElse((featureName, colFam), {
      val batchScanner = createBatchScanner
      batchScanner.setRanges(List(new org.apache.accumulo.core.data.Range(s"${METADATA_TAG}_$featureName")))
      batchScanner.fetchColumn(colFam, EMPTY_COLQ)

      val name = "version-" + featureName + "-" + colFam.toString
      val cfg = new IteratorSetting(1, name, classOf[VersioningIterator])
      VersioningIterator.setMaxVersions(cfg, 1)
      batchScanner.addScanIterator(cfg)

      val iter = batchScanner.iterator
      val result =
        if(iter.hasNext) Some(iter.next.getValue.toString)
        else None

      batchScanner.close()

      metaDataCache.put((featureName, colFam), result)
      result
    })

  // Returns a list of available layers.
  // This populates the list of layers which can be "published" by Geoserver.
  def createTypeNames(): java.util.List[Name] =
    if (!tableOps.exists(tableName)) List()
    else {
      readTypeNamesMatching.map { row =>
        val rowid = row.getKey.getRow.toString
        val attr = getFeatureNameFromMetadataRowID(rowid)
        new NameImpl(attr)
      }
    }

  def readTypeNamesMatching: Seq[KVEntry] = {
    val batchScanner = createBatchScanner
    batchScanner.setRanges(List[Range](new Range(METADATA_TAG, METADATA_TAG_END)))
    batchScanner.fetchColumnFamily(ATTRIBUTES_CF)
    val resultItr = new Iterator[KVEntry] {
      val src = batchScanner.iterator()
      def hasNext = src.hasNext
      def next() = src.next()
    }
    val result = resultItr.toList
    batchScanner.close()
    result
  }

  def getFeatureTypes = createTypeNames().map(_.toString)

  def getTypeNames: Array[String] = getFeatureTypes.toArray

  // NB:  By default, AbstractDataStore is "isWriteable".  This means that createFeatureSource returns
  // a featureStore
  def createFeatureSource(featureName: String): SimpleFeatureSource =
    new AccumuloFeatureStore(this, featureName)

  override def getFeatureSource(featureName: String): SimpleFeatureSource =
    createFeatureSource(featureName)

  def getIndexSchemaFmt(featureName: String) =
    readMetadataItem(featureName, SCHEMA_CF).getOrElse(EMPTY_STRING)

  def getAttributes(featureName: String) =
    readMetadataItem(featureName, ATTRIBUTES_CF).getOrElse(EMPTY_STRING)

  // Default to TEXT if no metadata is found in the table
  def getFeatureEncoder(featureName: String) = {
    val encodingString = readMetadataItem(featureName, FEATURE_ENCODING_CF).getOrElse(FeatureEncoding.TEXT.toString)
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

  def stringToReferencedEnvelope(string: String, crs: CoordinateReferenceSystem): ReferencedEnvelope = {
    val minMaxXY = string.split(":")
    require(minMaxXY.size == 4)
    new ReferencedEnvelope(minMaxXY(0).toDouble, minMaxXY(1).toDouble,
                           minMaxXY(2).toDouble, minMaxXY(3).toDouble, crs)
  }

  def writeBounds(featureName: String, bounds: ReferencedEnvelope) {
    // prepare to write out properties to the Accumulo SHP-file table
    val newbounds = readMetadataItem(featureName, BOUNDS_CF) match {
      case Some(env) => getNewBounds(env, featureName, bounds)
      case None      => bounds
    }

    val minMaxXY = List(newbounds.getMinX, newbounds.getMaxX, newbounds.getMinY, newbounds.getMaxY)
    val encoded = minMaxXY.mkString(":")
    writeMetadataItem(featureName, BOUNDS_CF, new Value(encoded.getBytes))
  }


  private def getNewBounds(env: String, featureName: String, bounds: ReferencedEnvelope) = {
    val oldBounds = stringToReferencedEnvelope(env, getSchema(featureName).getCoordinateReferenceSystem)
    val projBounds = bounds.transform(oldBounds.getCoordinateReferenceSystem, true)
    projBounds.expandToInclude(oldBounds)
    projBounds
  }

  // Implementation of Abstract method
  def getSchema(featureName: String) = {
    val sft = DataUtilities.createType(featureName, getAttributes(featureName))
    val dtgField = readMetadataItem(featureName, DTGFIELD_CF).getOrElse(Constants.SF_PROPERTY_START_TIME)
    sft.getUserData.put(core.index.SF_PROPERTY_START_TIME, dtgField)
    sft.getUserData.put(core.index.SF_PROPERTY_END_TIME,   dtgField)
    sft
  }

  // Implementation of Abstract method
  def getFeatureReader(featureName: String): AccumuloFeatureReader = getFeatureReader(featureName, Query.ALL)

  // This override is important as it allows us to optimize and plan our search with the Query.
  override def getFeatureReader(featureName: String, query: Query) = {
    val indexSchemaFmt = getIndexSchemaFmt(featureName)
    val sft = getSchema(featureName)
    val fe = getFeatureEncoder(featureName)
    new AccumuloFeatureReader(this, query, indexSchemaFmt, sft, fe)
  }

  /* create a general purpose writer that is capable of insert, deletes, and updates */
  override def createFeatureWriter(typeName: String, transaction: Transaction): SFFeatureWriter = {
    val featureType = getSchema(typeName)
    val indexSchemaFmt = getIndexSchemaFmt(typeName)
    val fe = getFeatureEncoder(typeName)
    val schema = IndexSchema(indexSchemaFmt, featureType, fe)
    val writer = new LocalRecordWriter(tableName, connector)
    val deleter = new LocalRecordDeleter(tableName, connector)
    new ModifyAccumuloFeatureWriter(featureType, schema, writer, writeVisibilities, deleter, this)
  }

  /* optimized for GeoTools API to return writer ONLY for appending (aka don't scan table) */
  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): SFFeatureWriter = {
    val featureType = getSchema(typeName)
    val indexSchemaFmt = getIndexSchemaFmt(typeName)
    val fe = getFeatureEncoder(typeName)
    val schema = IndexSchema(indexSchemaFmt, featureType, fe)
    val writer = new LocalRecordWriter(tableName, connector)
    new AppendAccumuloFeatureWriter(featureType, schema, writer, writeVisibilities)
  }

  override def getUnsupportedFilter(featureName: String, filter: Filter): Filter = Filter.INCLUDE

  def createBatchScanner = {
    connector.createBatchScanner(tableName, authorizationsProvider.getAuthorizations, 100)
  }

  // Accumulo assumes that the failures directory exists.  This function assumes that you have already created it.
  def importDirectory(tableName: String,
                      dir: String,
                      failureDir: String,
                      disableGC: Boolean) {
    connector.tableOperations().importDirectory(tableName, dir, failureDir, disableGC)
  }
}

/**
 *
 * @param connector        Accumulo connector
 * @param tableName        The name of the Accumulo table contains the various features
 * @param authorizationsProvider   Provides the authorizations used to access data
 * @param writeVisibilities visibilities to be applied to any data written by this store
 * @param params           The parameters used to create this datastore.
 *
 * This class provides an additional writer which can be accessed by
 *  createMapReduceFeatureWriter(featureName, context)
 *
 * This writer is appropriate for use inside a MapReduce job.  We explicitly do not override the default
 * createFeatureWriter so that we have both available.
 */
class MapReduceAccumuloDataStore(connector: Connector,
                                 tableName: String,
                                 authorizationsProvider: AuthorizationsProvider,
                                 writeVisibilities: String,
                                 val params: JMap[String, Serializable],
                                 indexSchemaFormat: String = "DEFAULT",
                                 featureEncoding: FeatureEncoding = FeatureEncoding.AVRO)
  extends AccumuloDataStore(connector, tableName, authorizationsProvider, writeVisibilities, indexSchemaFormat, featureEncoding) {

  override def createFeatureSource(featureName: String): SimpleFeatureSource =
    new MapReduceAccumuloFeatureStore(this, featureName)

  override def getFeatureSource(featureName: String): SimpleFeatureSource =
    createFeatureSource(featureName)

  def createMapReduceFeatureWriter(featureName: String, context: TASKIOCTX): SFFeatureWriter = {
    val featureType = getSchema(featureName)
    val idxFmt = getIndexSchemaFmt(featureName)
    val fe = getFeatureEncoder(featureName)
    val idx = IndexSchema(idxFmt, featureType, fe)
    val writer = new MapReduceRecordWriter(context)
    // TODO allow deletes? modifications?
    new AppendAccumuloFeatureWriter(featureType, idx, writer, writeVisibilities)
  }

}
