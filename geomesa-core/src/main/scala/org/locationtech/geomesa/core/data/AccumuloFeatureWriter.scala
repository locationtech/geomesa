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
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{RecordWriter, Reporter}
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.core.data.AccumuloFeatureWriter.{FeatureToWrite, FeatureWriterFn}
import org.locationtech.geomesa.core.data.tables.{AttributeTable, RecordTable, SpatioTemporalTable}
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.security.SecurityUtils.FEATURE_VISIBILITY
import org.locationtech.geomesa.feature.{ScalaSimpleFeature, ScalaSimpleFeatureFactory, SimpleFeatureEncoder}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

object AccumuloFeatureWriter {

  type FeatureWriterFn = (FeatureToWrite) => Unit
  type AccumuloRecordWriter = RecordWriter[Key, Value]

  class FeatureToWrite(val feature: SimpleFeature,
                       defaultVisibility: String,
                       encoder: SimpleFeatureEncoder,
                       indexValueEncoder: IndexValueEncoder) {
    val visibility =
      new Text(feature.getUserData.getOrElse(FEATURE_VISIBILITY, defaultVisibility).asInstanceOf[String])
    lazy val columnVisibility = new ColumnVisibility(visibility)
    // the index value is the encoded date/time/fid
    lazy val indexValue = new Value(indexValueEncoder.encode(feature))
    // the data value is the encoded SimpleFeature
    lazy val dataValue = new Value(encoder.encode(feature))
  }

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

abstract class AccumuloFeatureWriter(sft: SimpleFeatureType,
                                     encoder: SimpleFeatureEncoder,
                                     indexValueEncoder: IndexValueEncoder,
                                     stIndexEncoder: STIndexEncoder,
                                     ds: AccumuloDataStore,
                                     defaultVisibility: String) extends SimpleFeatureWriter with Logging {

  protected val rowIdPrefix = org.locationtech.geomesa.core.index.getTableSharingPrefix(sft)
  protected val indexedAttributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft)

  // TODO customizable batch writer config
  protected val multiBWWriter = ds.connector.createMultiTableBatchWriter(new BatchWriterConfig)

  // A "writer" is a function that takes a simple feature and writes
  // it to an index or table. This list is configured to match the
  // version of the datastore (i.e. single table vs catalog
  // table + index tables)
  protected val writers: List[FeatureWriterFn] = {
    val stBw = multiBWWriter.getBatchWriter(ds.getSpatioTemporalIdxTableName(sft))
    val stWriter = SpatioTemporalTable.spatioTemporalWriter(stBw, stIndexEncoder)

    val recBw = multiBWWriter.getBatchWriter(ds.getRecordTableForType(sft))
    val recWriter = RecordTable.recordWriter(recBw, rowIdPrefix)

    if (indexedAttributes.isEmpty) {
      List(stWriter, recWriter)
    } else {
      val attrBw = multiBWWriter.getBatchWriter(ds.getAttrIdxTableName(sft))
      val attrWriter = AttributeTable.attrWriter(attrBw, sft, indexedAttributes, rowIdPrefix)
      List(stWriter, recWriter, attrWriter)
    }
  }

  /* Return a String representing nextId - use UUID.random for universal uniqueness across multiple ingest nodes */
  protected def nextFeatureId = UUID.randomUUID().toString

  protected val builder = ScalaSimpleFeatureFactory.featureBuilder(sft)

  protected def writeToAccumulo(feature: SimpleFeature): Unit = {
    // require non-null geometry to write to geomesa (can't index null geo, yo)
    if (feature.getDefaultGeometry == null) {
      logger.warn(s"Invalid feature to write (no default geometry): ${DataUtilities.encodeFeature(feature)}")
      return
    }

    // see if there's a suggested ID to use for this feature
    val withFid = if (feature.getUserData.containsKey(Hints.PROVIDED_FID)) {
      val id = feature.getUserData.get(Hints.PROVIDED_FID).toString
      feature.getIdentifier match {
        case fid: FeatureIdImpl =>
          fid.setID(id)
          feature
        case _ =>
          builder.init(feature)
          builder.buildFeature(id)
      }
    } else {
      feature
    }

    val toWrite = new FeatureToWrite(withFid, defaultVisibility, encoder, indexValueEncoder)
    writers.foreach(write => write(toWrite))
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = multiBWWriter.close()

  override def remove(): Unit = {}

  override def hasNext: Boolean = false
}

class AppendAccumuloFeatureWriter(sft: SimpleFeatureType,
                                  encoder: SimpleFeatureEncoder,
                                  indexValueEncoder: IndexValueEncoder,
                                  stIndexEncoder: STIndexEncoder,
                                  ds: AccumuloDataStore,
                                  defaultVisibility: String)
  extends AccumuloFeatureWriter(sft, encoder, indexValueEncoder, stIndexEncoder, ds, defaultVisibility) {

  var currentFeature: SimpleFeature = null

  override def write(): Unit =
    if (currentFeature != null) {
      writeToAccumulo(currentFeature)
      currentFeature = null
    }

  override def next(): SimpleFeature = {
    currentFeature = new ScalaSimpleFeature(nextFeatureId, sft)
    currentFeature
  }
}

class ModifyAccumuloFeatureWriter(sft: SimpleFeatureType,
                                  encoder: SimpleFeatureEncoder,
                                  indexValueEncoder: IndexValueEncoder,
                                  stIndexEncoder: STIndexEncoder,
                                  ds: AccumuloDataStore,
                                  defaultVisibility: String,
                                  filter: Filter)
  extends AccumuloFeatureWriter(sft, encoder, indexValueEncoder, stIndexEncoder, ds, defaultVisibility) {

  val reader = ds.getFeatureReader(sft.getTypeName, new Query(sft.getTypeName, filter))

  var live: SimpleFeature = null      /* feature to let user modify   */
  var original: SimpleFeature = null  /* feature returned from reader */

  // A remover is a function that removes a feature from an
  // index or table. This list is configured to match the
  // version of the datastore (i.e. single table vs catalog
  // table + index tables)
  val removers: List[FeatureWriterFn] = {
    val stBw = multiBWWriter.getBatchWriter(ds.getSpatioTemporalIdxTableName(sft))
    val stWriter = SpatioTemporalTable.removeSpatioTemporalIdx(stBw, stIndexEncoder)

    val recBw = multiBWWriter.getBatchWriter(ds.getRecordTableForType(sft))
    val recWriter = RecordTable.recordDeleter(recBw, rowIdPrefix)

    if (indexedAttributes.isEmpty) {
      List(stWriter, recWriter)
    } else {
      val attrBw = multiBWWriter.getBatchWriter(ds.getAttrIdxTableName(sft))
      val attrWriter = AttributeTable.removeAttrIdx(attrBw, sft, indexedAttributes, rowIdPrefix)
      List(stWriter, recWriter, attrWriter)
    }
  }

  override def remove() =
    if (original != null) {
      val toRemove = new FeatureToWrite(original, defaultVisibility, encoder, indexValueEncoder)
      removers.foreach(remove => remove(toRemove))
    }

  override def hasNext = reader.hasNext

  /* only write if non null and it hasn't changed...*/
  /* original should be null only when reader runs out */
  override def write() =
    // comparison of feature ID and attributes - doesn't consider concrete class used
    if (!ScalaSimpleFeature.equalIdAndAttributes(live, original)) {
      if (original != null) {
        remove()
      }
      writeToAccumulo(live)
    }

  override def next: SimpleFeature = {
    original = null
    live = if (hasNext) {
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
    super.close() // closes writer
    reader.close()
  }

}
