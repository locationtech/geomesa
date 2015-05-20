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

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.RecordWriter
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter._
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.features.{ScalaSimpleFeature, ScalaSimpleFeatureFactory, SimpleFeatureSerializer}
import org.locationtech.geomesa.security.SecurityUtils
import SecurityUtils.FEATURE_VISIBILITY
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

object AccumuloFeatureWriter {

  type FeatureToMutations = (FeatureToWrite) => Seq[Mutation]
  type FeatureWriterFn    = (FeatureToWrite) => Unit
  type TableAndWriter     = (String, FeatureToMutations)

  type AccumuloRecordWriter = RecordWriter[Key, Value]

  class FeatureToWrite(val feature: SimpleFeature,
                       defaultVisibility: String,
                       encoder: SimpleFeatureSerializer,
                       indexValueEncoder: IndexValueEncoder) {
    val visibility =
      new Text(feature.getUserData.getOrElse(FEATURE_VISIBILITY, defaultVisibility).asInstanceOf[String])
    lazy val columnVisibility = new ColumnVisibility(visibility)
    // the index value is the encoded date/time/fid
    lazy val indexValue = new Value(indexValueEncoder.encode(feature))
    // the data value is the encoded SimpleFeature
    lazy val dataValue = new Value(encoder.serialize(feature))
  }

  def featureWriter(writers: Seq[(BatchWriter, FeatureToMutations)]): FeatureWriterFn =
    feature => writers.foreach { case (bw, fToM) => bw.addMutations(fToM(feature)) }

  /**
   * Gets writers and table names for each table (e.g. index) that supports the sft
   */
  def getTablesAndWriters(sft: SimpleFeatureType, ds: AccumuloConnectorCreator): Seq[TableAndWriter] = {
    val tablesAndNames = GeoMesaTable.getTablesAndNames(sft, ds)
    tablesAndNames.flatMap { case (table, name) => table.writer(sft).map((name, _)) }
  }

  /**
   * Gets removers and table names for each table (e.g. index) that supports the sft
   */
  def getTablesAndRemovers(sft: SimpleFeatureType, ds: AccumuloConnectorCreator): Seq[TableAndWriter] = {
    val tablesAndNames = GeoMesaTable.getTablesAndNames(sft, ds)
    tablesAndNames.flatMap { case (table, name) => table.remover(sft).map((name, _)) }
  }
}

abstract class AccumuloFeatureWriter(sft: SimpleFeatureType,
                                     encoder: SimpleFeatureSerializer,
                                     indexValueEncoder: IndexValueEncoder,
                                     ds: AccumuloDataStore,
                                     defaultVisibility: String) extends SimpleFeatureWriter with Logging {

  protected val multiBWWriter = ds.connector.createMultiTableBatchWriter(GeoMesaBatchWriterConfig())

  // A "writer" is a function that takes a simple feature and writes it to an index or table
  protected val writer: FeatureWriterFn = {
    val writers = getTablesAndWriters(sft, ds).map {
      case (table, write) => (multiBWWriter.getBatchWriter(table), write)
    }
    featureWriter(writers)
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

    writer(new FeatureToWrite(withFid, defaultVisibility, encoder, indexValueEncoder))
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = multiBWWriter.close()

  override def remove(): Unit = {}

  override def hasNext: Boolean = false
}

class AppendAccumuloFeatureWriter(sft: SimpleFeatureType,
                                  encoder: SimpleFeatureSerializer,
                                  indexValueEncoder: IndexValueEncoder,
                                  ds: AccumuloDataStore,
                                  defaultVisibility: String)
  extends AccumuloFeatureWriter(sft, encoder, indexValueEncoder, ds, defaultVisibility) {

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
                                  encoder: SimpleFeatureSerializer,
                                  indexValueEncoder: IndexValueEncoder,
                                  ds: AccumuloDataStore,
                                  defaultVisibility: String,
                                  filter: Filter)
  extends AccumuloFeatureWriter(sft, encoder, indexValueEncoder, ds, defaultVisibility) {

  val reader = ds.getFeatureReader(sft.getTypeName, new Query(sft.getTypeName, filter))

  var live: SimpleFeature = null      /* feature to let user modify   */
  var original: SimpleFeature = null  /* feature returned from reader */

  // A remover is a function that removes a feature from an
  // index or table. This list is configured to match the
  // version of the datastore (i.e. single table vs catalog
  // table + index tables)
  val remover: FeatureWriterFn = {
    val writers = getTablesAndRemovers(sft, ds).map {
      case (table, write) => (multiBWWriter.getBatchWriter(table), write)
    }
    featureWriter(writers)
  }

  override def remove() = if (original != null) {
    remover(new FeatureToWrite(original, defaultVisibility, encoder, indexValueEncoder))
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
      builder.init(original) // this copies user data as well
      builder.buildFeature(original.getID)
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
