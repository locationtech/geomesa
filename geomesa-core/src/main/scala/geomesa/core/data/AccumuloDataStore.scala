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

import geomesa.core.data.AccumuloFeatureWriter.{LocalRecordWriter, MapReduceRecordWriter}
import geomesa.core.index.{SpatioTemporalIndexSchema, IndexEntryType}
import java.io.Serializable
import java.util.{Map=>JMap}
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.client.{BatchWriterConfig, IteratorSetting, TableExistsException, Connector}
import org.apache.accumulo.core.data.{Key, Mutation, Value, Range}
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.Hints
import org.geotools.feature.NameImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.referencing.crs.CoordinateReferenceSystem
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 *
 * @param connector        Accumulo connector
 * @param tableName        The name of the Accumulo table contains the various features
 * @param authorizations   The authorizations used to access data
 *
 * This class handles DataStores which are stored in Accumulo Tables.  To be clear, one table may contain multiple
 * features addressed by their featureName.
 */
class AccumuloDataStore(val connector: Connector,
                        val tableName: String,
                        val authorizations: Authorizations,
                        val indexSchemaFormat: String) extends AbstractDataStore {
  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  type KVEntry = JMap.Entry[Key,Value]

  def createTableIfNotExists(tableName: String, indexSchemaFormat: String,
                             featureType: SimpleFeatureType) {

    if (!connector.tableOperations().exists(tableName)) {
      try {
        connector.tableOperations.create(tableName)
      } catch {
        case tee: TableExistsException =>  // Swallow a race condition based exception...
      }
    }

    // If have a real Connector and fewer then the proper number of table splits,
    // we initialize our table structure.
    if(!connector.isInstanceOf[MockConnector]) {
      // extract sharding information (partition specified at the head of the RowID)
      val maxShard = SpatioTemporalIndexSchema(indexSchemaFormat, featureType).maxShard

      // if this table already has splits defined, suggesting that there is
      // already at least one feature present, yet those splits are not
      // consistent with what the current feature needs, then complain, and
      // refuse to add the current feature
      //@TODO revisit this constraint once dynamic re-keying is coded
      val numSplits = connector.tableOperations().listSplits(tableName).size
      if (numSplits > 0 && numSplits != maxShard)
        throw new Exception(s"Table $tableName already has $numSplits splits, " +
          s"but the new feature requires $maxShard.  These features cannot share" +
          "the same table.")

      // add a new table with a full set of splits;
      // NB:  "%99r" means that the shard numbers will range from 0 to 99 inclusive,
      // so the split points should be 1 to 99 inclusive (because the features
      // will all be of the form "00~...", up through "99~...")
      // For anyone who cares, this is how 99 partitions might be defined:
      //   TABLET RANGE  EXAMPLE KEYS
      //   (-inf, 01]    00~...
      //   (01,   02]    01~...
      //   ...
      //   (98,   99]    98~...
      //   (99, +inf)    99~...
      val splits = (1 to maxShard).map(s"%0${maxShard.toString.length}d".format(_)).map(new Text(_))
      connector.tableOperations.addSplits(tableName, new java.util.TreeSet(splits))

      // be sure to enable the column-family functor
      connector.tableOperations.setProperty(tableName, "table.bloom.key.functor",
                                            classOf[org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor].getCanonicalName)
      connector.tableOperations.setProperty(tableName, "table.bloom.enabled", "true")

      // isolate various metadata elements in locality groups
      connector.tableOperations().setLocalityGroups(tableName,
        Map(
          ATTRIBUTES_CF.toString -> Set(ATTRIBUTES_CF).asJava,
          SCHEMA_CF.toString -> Set(SCHEMA_CF).asJava,
          BOUNDS_CF.toString -> Set(BOUNDS_CF).asJava))
    }
  }

  override def createSchema(featureType: SimpleFeatureType) {
    val featureName = featureType.getName.getLocalPart
    val netSchemaFormat = getNetIndexSchemaFormat(featureName)

    createSchema(featureType, netSchemaFormat)
  }

  def createSchema(featureType: SimpleFeatureType,
                   indexSchemaFormat: String) {

    createTableIfNotExists(tableName, indexSchemaFormat, featureType)
    writeMetadata(featureType, connector, tableName, indexSchemaFormat)
  }

  def writeMetadata(sft: SimpleFeatureType,
                    indexSchemaFormat: String) {
    writeMetadata(sft, connector, tableName, indexSchemaFormat)
  }

  def getNetIndexSchemaFormat(featureName:String) : String = {
    // the sole purpose of having the ID tacked on to the end of the key is
    // to provide for differentiation among similar entries (same geometry,
    // date time) mapped to the same shard/partition
    val defaultIndexSchemaFormat = "%~#s%99#r%" + featureName +
                                   "#cstr%0,1#gh%yyyyMM#d::%~#s%1,3#gh::%~#s%4,3#gh%ddHH#d%#id"

    indexSchemaFormat match {
      case null => defaultIndexSchemaFormat
      case s:String => if (s.trim.length == 0) defaultIndexSchemaFormat else s
    }
  }

  def writeMetadata(sft: SimpleFeatureType,
                    connector: Connector,
                    tableName: String,
                    indexSchemaFormat: String) {

    val featureName = sft.getName.getLocalPart

    // blend the simple-feature type from the input SHP file with the index-service's (magic) type
    val netType: SimpleFeatureType = IndexEntryType.blendWithType(sft)

    // write out properties to the Accumulo SHP-file table
    if(getAttributes(featureName).length == 0) {
      // write out the simple-feature type
      val value = new Value(DataUtilities.encodeType(netType).getBytes)
      writeMetadataItem(featureName, ATTRIBUTES_CF, value)
    }
    if(getIndexSchemaFmt(featureName).length == 0) {
      // write out the index-schema for this feature
      val value = new Value(indexSchemaFormat.getBytes)
      writeMetadataItem(featureName, SCHEMA_CF, value)
    }
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

    // might as well pre-fetch this into cache
    metaDataCache.put((featureName, colFam), Option(metadata).map(_.toString))
  }

  // Read metadata using scheme:  ~METADATA_featureName metadataFieldName: insertionTimestamp metadataValue
  private def readMetadataItem(featureName: String, colFam: Text): Option[String] = metaDataCache.getOrElse((featureName, colFam), {
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
  def createTypeNames(): java.util.List[Name] = {
    if (!connector.tableOperations().exists(tableName)) List()
    else {
      def readTypeNamesMatching(lowRowID: String, highRowID: String): List[KVEntry] = {
        val batchScanner = createBatchScanner
        batchScanner.setRanges(List[Range](new Range(lowRowID, highRowID)))
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

      // extract the feature names
      for {
        row <- readTypeNamesMatching(METADATA_TAG, UNLIKELY_LAST_ROWID)
        rowid = row.getKey.getRow.toString
        attr = getFeatureNameFromMetadataRowID(rowid)
        name = new NameImpl(attr)
      } yield name
    }
  }

  def getFeatureTypes = createTypeNames().map(_.toString)

  // Implementation of Abstract method
  def getTypeNames: Array[String] = getFeatureTypes.toArray

  // NB:  By default, AbstractDataStore is "isWriteable".  This means that createFeatureSource returns
  //  a featureStore.  As such, we do never really use the AccumuloFeatureSource
  def createFeatureSource(featureName: String): SimpleFeatureSource =
    new AccumuloFeatureStore(this, featureName)

  override def getFeatureSource(featureName: String): SimpleFeatureSource =
    createFeatureSource(featureName)

  def getIndexSchemaFmt(featureName: String): String =
    readMetadataItem(featureName, SCHEMA_CF).getOrElse(EMPTY_STRING)

  def getAttributes(featureName: String): String =
    readMetadataItem(featureName, ATTRIBUTES_CF).getOrElse(EMPTY_STRING)

  // We assume that they want the bounds for everything.
  override def getBounds(query: Query): ReferencedEnvelope = {
    val env = readMetadataItem(query.getTypeName, BOUNDS_CF)
      .getOrElse(WHOLE_WORLD_BOUNDS)  // presumed EPSG:4326 inside

    val minMaxXY = env.split(":")

    stringToReferencedEnvelope(
      minMaxXY.size match {
        case 4 => env
        case _ => WHOLE_WORLD_BOUNDS
      },
      getSchema(query.getTypeName).getCoordinateReferenceSystem)
  }

  def stringToReferencedEnvelope(string: String, crs: CoordinateReferenceSystem): ReferencedEnvelope = {
    val minMaxXY = string.split(":")
    require(minMaxXY.size == 4)
    new ReferencedEnvelope(minMaxXY(0).toDouble, minMaxXY(1).toDouble,
                           minMaxXY(2).toDouble, minMaxXY(3).toDouble, crs)
  }

  def writeBounds(featureName: String, bounds: ReferencedEnvelope) {
    // prepare to write out properties to the Accumulo SHP-file table
    val env = readMetadataItem(featureName, BOUNDS_CF)
      .getOrElse(EMPTY_STRING)
    val newbounds = if(env != "" && env.split(":").size == 4) {
      val oldBounds = stringToReferencedEnvelope(env, getSchema(featureName).getCoordinateReferenceSystem)
      val projBounds = bounds.transform(oldBounds.getCoordinateReferenceSystem, true)
      projBounds.expandToInclude(oldBounds)
      projBounds
    } else bounds

    val minMaxXY = List(newbounds.getMinX, newbounds.getMaxX, newbounds.getMinY, newbounds.getMaxY)
    val encoded = minMaxXY.mkString(":")

    writeMetadataItem(featureName, BOUNDS_CF, new Value(encoded.getBytes))
  }

  // Implementation of Abstract method
  def getSchema(featureName: String): SimpleFeatureType =
    DataUtilities.createType(featureName, getAttributes(featureName))

  // Implementation of Abstract method
  def getFeatureReader(featureName: String): AccumuloFeatureReader = getFeatureReader(featureName, Query.ALL)

  // This override is important as it allows us to optimize and plan our search with the Query.
  override def getFeatureReader(featureName: String,
                                query: Query): AccumuloFeatureReader =
    new AccumuloFeatureReader(this, featureName, query, getIndexSchemaFmt(featureName),
                               getAttributes(featureName), getSchema(featureName))

  override def createFeatureWriter(featureName: String,
                                   transaction: Transaction): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    val featureType = getSchema(featureName)
    new AccumuloFeatureWriter(
      featureType,
      SpatioTemporalIndexSchema(getIndexSchemaFmt(featureName), featureType),
      new LocalRecordWriter(tableName, connector))
  }

  override def getUnsupportedFilter(featureName: String, filter: Filter): Filter = Filter.INCLUDE

  def createBatchScanner = connector.createBatchScanner(tableName, authorizations, 20)

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
 * @param authorizations   The authorizations used to access data
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
                                  authorizations: Authorizations,
                                  val params: JMap[String,Serializable],
                                  indexSchemaFormat: String)
    extends AccumuloDataStore(connector, tableName, authorizations, indexSchemaFormat) {

  override def createFeatureSource(featureName: String): SimpleFeatureSource =
    new MapReduceAccumuloFeatureStore(this, featureName)

  override def getFeatureSource(featureName: String): SimpleFeatureSource = createFeatureSource(featureName)

  def createMapReduceFeatureWriter(featureName: String, context: TaskInputOutputContext[_,_,Key,Value]):
  FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    val featureType = getSchema(featureName)
    new AccumuloFeatureWriter(
      featureType,
      SpatioTemporalIndexSchema(getIndexSchemaFmt(featureName), featureType),
      new MapReduceRecordWriter(context))
  }
}