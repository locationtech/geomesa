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

import java.nio.charset.StandardCharsets
import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.index._
import geomesa.feature.{AvroSimpleFeature, AvroSimpleFeatureFactory}
import geomesa.utils.geotools.SimpleFeatureTypes
import org.apache.accumulo.core.client.{BatchWriter, BatchWriterConfig, Connector}
import org.apache.accumulo.core.data.{Key, Mutation, PartialKey, Value, Range => ARange}
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{RecordWriter, Reporter}
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.geotools.data.DataUtilities
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object AccumuloFeatureWriter {

  type AccumuloRecordWriter = RecordWriter[Key, Value]

  val EMPTY_VALUE = new Value()

  class LocalRecordDeleter(tableName: String, connector: Connector) extends AccumuloRecordWriter {
    private val bw = connector.createBatchWriter(tableName, new BatchWriterConfig())

    def write(key: Key, value: Value) {
      val m = new Mutation(key.getRow)
      m.putDelete(key.getColumnFamily, key.getColumnQualifier, key.getColumnVisibilityParsed)
      bw.addMutation(m)
    }

    def close(reporter: Reporter) {
      bw.flush()
      bw.close()
    }
  }

  class MapReduceRecordWriter(context: TaskInputOutputContext[_,_,Key,Value]) extends AccumuloRecordWriter {
    def write(key: Key, value: Value) {
      context.write(key, value)
    }

    def close(reporter: Reporter) {}
  }
}

abstract class AccumuloFeatureWriter(featureType: SimpleFeatureType,
                                     indexer: IndexSchema,
                                     encoder: SimpleFeatureEncoder,
                                     ds: AccumuloDataStore,
                                     visibility: String)
  extends SimpleFeatureWriter
          with Logging {

  val indexedAttributes = SimpleFeatureTypes.getIndexedAttributes(featureType)
  val indexedAttributeNames = indexedAttributes.map(_.getLocalName.getBytes(StandardCharsets.UTF_8))
  val indexedAttributesWithNames = indexedAttributes.zip(indexedAttributeNames)

  val NULLBYTE = Array[Byte](0.toByte)
  val connector = ds.connector

  protected val multiBWWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig)

  // A "writer" is a function that takes a simple feature and writes
  // it to an index or table. This list is configured to match the
  // version of the datastore (i.e. single table vs catalog
  // table + index tables)
  protected val writers: List[SimpleFeature => Unit] = {
    val stTable = ds.getSpatioTemporalIdxTableName(featureType)
    val stWriter = List(spatioTemporalWriter(multiBWWriter.getBatchWriter(stTable)))

    val attrWriters: List[SimpleFeature => Unit] =
      if (ds.catalogTableFormat(featureType)) {
        val attrTable = ds.getAttrIdxTableName(featureType)
        val recTable = ds.getRecordTableForType(featureType)
        List(
          attrWriter(multiBWWriter.getBatchWriter(attrTable)),
          recordWriter(multiBWWriter.getBatchWriter(recTable)))
      } else {
        List.empty
      }

    stWriter ::: attrWriters
  }

  def getFeatureType: SimpleFeatureType = featureType

  /* Return a String representing nextId - use UUID.random for universal uniqueness across multiple ingest nodes */
  protected def nextFeatureId = UUID.randomUUID().toString

  protected val builder = AvroSimpleFeatureFactory.featureBuilder(featureType)

  protected def writeToAccumulo(feature: SimpleFeature): Unit = {
    // see if there's a suggested ID to use for this feature
    // (relevant when this insertion is wrapped inside a Transaction)
    val toWrite =
      if(feature.getUserData.containsKey(Hints.PROVIDED_FID)) {
        builder.init(feature)
        builder.buildFeature(feature.getUserData.get(Hints.PROVIDED_FID).toString)
      }
      else feature

    // require non-null geometry to write to geomesa (can't index null geo yo!)
    if (toWrite.getDefaultGeometry != null) {
      writers.foreach { w => w(toWrite) }
    } else {
      logger.warn("Invalid feature to write (no default geometry):  " + DataUtilities.encodeFeature(toWrite))
    }
  }

  /** Creates a function to write a feature to the Record Table **/
  private def recordWriter(bw: BatchWriter): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      val m = new Mutation(feature.getID)
      m.put(SFT_CF, EMPTY_COLQ, new ColumnVisibility(visibility), encoder.encode(feature))
      bw.addMutation(m)
    }

  /** Creates a function to write a feature to the spatio temporal index **/
  private def spatioTemporalWriter(bw: BatchWriter): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      val KVs = indexer.encode(feature)
      val m = KVs.groupBy { case (k, _) => k.getRow }.map { case (row, kvs) => kvsToMutations(row, kvs) }
      bw.addMutations(m.asJava)
    }

  /** Creates a function to write a feature to the attribute index **/
  private def attrWriter(bw: BatchWriter): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      val muts = getAttrIdxMutations(feature, new Text(feature.getID)).map {
        case PutOrDeleteMutation(row, cf, cq, v) =>
          val m = new Mutation(row)
          m.put(cf, cq, new ColumnVisibility(visibility), v)
          m
      }
      bw.addMutations(muts)
    }

  case class PutOrDeleteMutation(row: Array[Byte], cf: Text, cq: Text, v: Value)

  def getAttrIdxMutations(feature: SimpleFeature, cf: Text) = {
    val value = IndexSchema.encodeIndexValue(feature)
    indexedAttributesWithNames.map { case (attr, name) =>
      val attrValue = valOrNull(feature.getAttribute(attr.getName)).getBytes(StandardCharsets.UTF_8)
      val row = name ++ NULLBYTE ++ attrValue
      PutOrDeleteMutation(row, cf, EMPTY_COLQ, value)
    }
  }

  private val nullString = "<null>"
  private def valOrNull(o: AnyRef) = if(o == null) nullString else o.toString

  private def kvsToMutations(row: Text, kvs: Seq[(Key, Value)]): Mutation = {
    val m = new Mutation(row)
    kvs.foreach { case (k, v) =>
      m.put(k.getColumnFamily, k.getColumnQualifier, k.getColumnVisibilityParsed, v)
    }
    m
  }

  def close() = multiBWWriter.close()

  def remove() {}

  def hasNext: Boolean = false
}

class AppendAccumuloFeatureWriter(featureType: SimpleFeatureType,
                                  indexer: IndexSchema,
                                  connector: Connector,
                                  encoder: SimpleFeatureEncoder,
                                  visibility: String,
                                  ds: AccumuloDataStore)
  extends AccumuloFeatureWriter(featureType, indexer, encoder, ds, visibility) {

  var currentFeature: SimpleFeature = null


  def write() {
    if (currentFeature != null) writeToAccumulo(currentFeature)
    currentFeature = null
  }

  def next(): SimpleFeature = {
    currentFeature = new AvroSimpleFeature(new FeatureIdImpl(nextFeatureId), featureType)
    currentFeature
  }

}

class ModifyAccumuloFeatureWriter(featureType: SimpleFeatureType,
                                  indexer: IndexSchema,
                                  connector: Connector,
                                  encoder: SimpleFeatureEncoder,
                                  visibility: String,
                                  dataStore: AccumuloDataStore)
  extends AccumuloFeatureWriter(featureType, indexer, encoder, dataStore, visibility) {

  val reader = dataStore.getFeatureReader(featureType.getName.toString)
  var live: SimpleFeature = null      /* feature to let user modify   */
  var original: SimpleFeature = null  /* feature returned from reader */

  // A remover is a function that removes a feature from an
  // index or table. This list is configured to match the
  // version of the datastore (i.e. single table vs catalog
  // table + index tables)
  val removers: List[SimpleFeature => Unit] = {
    val stTable = dataStore.getSpatioTemporalIdxTableName(featureType)
    val stWriter = List(removeSpatioTemporalIdx(multiBWWriter.getBatchWriter(stTable)))

    val attrWriters: List[SimpleFeature => Unit] =
      if (dataStore.catalogTableFormat(featureType)) {
        val attrTable = dataStore.getAttrIdxTableName(featureType)
        val recTable = dataStore.getRecordTableForType(featureType)
        List(
          removeAttrIdx(multiBWWriter.getBatchWriter(attrTable)),
          removeRecord(multiBWWriter.getBatchWriter(recTable)))
      } else {
        List.empty
      }

    stWriter ::: attrWriters
  }

  /** Creates a function to remove a feature from the record table **/
  private def removeRecord(bw: BatchWriter): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      val row = new Text(feature.getID)
      val mutation = new Mutation(row)

      val scanner = dataStore.createRecordScanner(featureType)
      scanner.setRanges(List(new ARange(row, true, row, true)))
      scanner.iterator().foreach { entry =>
        val key = entry.getKey
        mutation.putDelete(key.getColumnFamily, key.getColumnQualifier, key.getColumnVisibilityParsed)
      }
      if (mutation.size() > 0) {
        bw.addMutation(mutation)
      }
    }

  /** Creates a function to remove spatio temporal index entries for a feature **/
  private def removeSpatioTemporalIdx(bw: BatchWriter): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      indexer.encode(original).foreach { case (key, _) =>
        val m = new Mutation(key.getRow)
        m.putDelete(key.getColumnFamily, key.getColumnQualifier, key.getColumnVisibilityParsed)
        bw.addMutation(m)
      }
    }

  val emptyVis = new ColumnVisibility()

  /** Creates a function to remove attribute index entries for a feature **/
  private def removeAttrIdx(bw: BatchWriter): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      getAttrIdxMutations(feature, new Text(feature.getID)).map {
        case PutOrDeleteMutation(row, cf, cq, _) =>
          val m = new Mutation(row)
          m.putDelete(cf, cq, emptyVis)
          bw.addMutation(m)
      }
    }

  override def remove() =
    if (original != null) {
      removers.foreach { r => r(original)}
      multiBWWriter.flush()
    }

  override def hasNext = reader.hasNext

  /* only write if non null and it hasn't changed...*/
  /* original should be null only when reader runs out */
  override def write() =
    if(!live.equals(original)) {  // This depends on having the same SimpleFeature concrete class
      if(original != null) remove()
      writeToAccumulo(live)
    }

  /* Delete keys from original index and data entries that are different from new keys */
  /* Return list of old keys that should be deleted */
  def keysToDelete = {
    val oldKeys = indexer.encode(original).map { case (k, v) => k }
    val newKeys = indexer.encode(live).map { case (k, v) => k }
    oldKeys.zip(newKeys).filter { case(o, n) =>
      !o.equals(n, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
    }.map { case (k1, _) => k1 }
  }

  override def next: SimpleFeature = {
    original = null
    live =
      if (hasNext) {
        original = reader.next()
        builder.init(original)
        builder.buildFeature(original.getID)
      } else {
        builder.buildFeature(nextFeatureId)
      }
    live
  }

  override def close() = {
    super.close() //closes writer
    reader.close()
  }

}
