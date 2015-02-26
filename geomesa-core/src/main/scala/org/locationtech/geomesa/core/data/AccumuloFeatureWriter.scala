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

package org.locationtech.geomesa.core.data

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{BatchWriterConfig, Connector}
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.mapred.{RecordWriter, Reporter}
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.Hints
import org.locationtech.geomesa.core.data.AccumuloFeatureWriter.FeatureWriterFn
import org.locationtech.geomesa.core.data.tables.{AttributeTable, RecordTable, SpatioTemporalTable}
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.security
import org.locationtech.geomesa.feature.{ScalaSimpleFeature, ScalaSimpleFeatureFactory, SimpleFeatureEncoder}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

object AccumuloFeatureWriter {

  type FeatureWriterFn = (SimpleFeature, String) => Unit
  type AccumuloRecordWriter = RecordWriter[Key, Value]

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
                                     indexEncoder: IndexEntryEncoder,
                                     encoder: SimpleFeatureEncoder,
                                     ds: AccumuloDataStore,
                                     visibility: String)
  extends SimpleFeatureWriter
          with Logging {

  val indexedAttributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(featureType)

  val connector = ds.connector

  protected val multiBWWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig)

  // A "writer" is a function that takes a simple feature and writes
  // it to an index or table. This list is configured to match the
  // version of the datastore (i.e. single table vs catalog
  // table + index tables)
  protected val writers: List[FeatureWriterFn] = {
    val stTable = ds.getSpatioTemporalIdxTableName(featureType)
    val stWriter = List(SpatioTemporalTable.spatioTemporalWriter(multiBWWriter.getBatchWriter(stTable), indexEncoder))

    val attrWriters =
      if (ds.getGeomesaVersion(featureType) < 1) {
        List.empty
      } else {
        val attrWriter = multiBWWriter.getBatchWriter(ds.getAttrIdxTableName(featureType))
        val recWriter = multiBWWriter.getBatchWriter(ds.getRecordTableForType(featureType))
        val rowIdPrefix = org.locationtech.geomesa.core.index.getTableSharingPrefix(featureType)
        val encoding = encoder.encoding
        List(AttributeTable.attrWriter(attrWriter, featureType, encoding, indexedAttributes, rowIdPrefix),
             RecordTable.recordWriter(recWriter, encoder, rowIdPrefix))
      }

    stWriter ::: attrWriters
  }

  def getFeatureType: SimpleFeatureType = featureType

  /* Return a String representing nextId - use UUID.random for universal uniqueness across multiple ingest nodes */
  protected def nextFeatureId = UUID.randomUUID().toString

  protected val builder = ScalaSimpleFeatureFactory.featureBuilder(featureType)

  protected def writeToAccumulo(feature: SimpleFeature): Unit = {
    import scala.collection.JavaConversions._
    // see if there's a suggested ID to use for this feature
    // (relevant when this insertion is wrapped inside a Transaction)
    val toWrite =
      if(feature.getUserData.containsKey(Hints.PROVIDED_FID)) {
        builder.init(feature)
        builder.buildFeature(feature.getUserData.get(Hints.PROVIDED_FID).toString)
      }
      else feature

    val perFeatureVisibility = feature.getUserData.getOrElse(security.SecurityUtils.FEATURE_VISIBILITY, visibility).asInstanceOf[String]

    // require non-null geometry to write to geomesa (can't index null geo)
    if (toWrite.getDefaultGeometry != null) {
      writers.foreach { w => w(toWrite, perFeatureVisibility) }
    } else {
      logger.warn("Invalid feature to write (no default geometry):  " + DataUtilities.encodeFeature(toWrite))
    }
  }

  def close() = multiBWWriter.close()

  def remove() {}

  def hasNext: Boolean = false
}

class AppendAccumuloFeatureWriter(featureType: SimpleFeatureType,
                                  indexEncoder: IndexEntryEncoder,
                                  connector: Connector,
                                  encoder: SimpleFeatureEncoder,
                                  visibility: String,
                                  ds: AccumuloDataStore)
  extends AccumuloFeatureWriter(featureType, indexEncoder, encoder, ds, visibility) {

  var currentFeature: SimpleFeature = null


  def write() {
    if (currentFeature != null) writeToAccumulo(currentFeature)
    currentFeature = null
  }

  def next(): SimpleFeature = {
    currentFeature = new ScalaSimpleFeature(nextFeatureId, featureType)
    currentFeature
  }

}

class ModifyAccumuloFeatureWriter(featureType: SimpleFeatureType,
                                  indexEncoder: IndexEntryEncoder,
                                  connector: Connector,
                                  encoder: SimpleFeatureEncoder,
                                  visibility: String,
                                  filter: Filter,
                                  dataStore: AccumuloDataStore)
  extends AccumuloFeatureWriter(featureType, indexEncoder, encoder, dataStore, visibility) {

  val reader = dataStore.getFeatureReader(featureType.getTypeName, new Query(featureType.getTypeName, filter))

  var live: SimpleFeature = null      /* feature to let user modify   */
  var original: SimpleFeature = null  /* feature returned from reader */

  // A remover is a function that removes a feature from an
  // index or table. This list is configured to match the
  // version of the datastore (i.e. single table vs catalog
  // table + index tables)
  val removers: List[FeatureWriterFn] = {
    val stTable = dataStore.getSpatioTemporalIdxTableName(featureType)
    val stWriter = List(SpatioTemporalTable.removeSpatioTemporalIdx(multiBWWriter.getBatchWriter(stTable), indexEncoder))

    val rowIdPrefix = org.locationtech.geomesa.core.index.getTableSharingPrefix(featureType)

    val attrWriters =
      if (dataStore.getGeomesaVersion(featureType) < 1) {
        List.empty
      } else {
        val attrWriter = multiBWWriter.getBatchWriter(dataStore.getAttrIdxTableName(featureType))
        val recWriter = multiBWWriter.getBatchWriter(dataStore.getRecordTableForType(featureType))
        List(AttributeTable.removeAttrIdx(attrWriter, featureType, indexedAttributes, rowIdPrefix),
             RecordTable.recordDeleter(recWriter, encoder, rowIdPrefix))
      }
    stWriter ::: attrWriters
  }

  import scala.collection.JavaConversions._
  override def remove() =
    if (original != null) {
      removers.foreach { r => r(original, original.getUserData.getOrElse(security.SecurityUtils.FEATURE_VISIBILITY, visibility).asInstanceOf[String]) }
    }

  override def hasNext = reader.hasNext

  /* only write if non null and it hasn't changed...*/
  /* original should be null only when reader runs out */
  override def write() =
    if(!live.equals(original)) {  // This depends on having the same SimpleFeature concrete class
      if(original != null) remove()
      writeToAccumulo(live)
    }

  override def next: SimpleFeature = {
    original = null
    live =
      if (hasNext) {
        original = reader.next()
        builder.init(original)
        val ret = builder.buildFeature(original.getID)
        ret.getUserData.putAll(original.getUserData)
        ret
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
