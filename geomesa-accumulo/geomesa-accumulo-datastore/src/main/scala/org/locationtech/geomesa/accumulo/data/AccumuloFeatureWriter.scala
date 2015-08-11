/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.concurrent.atomic.AtomicLong

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
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.FeatureIdProperties.FEATURE_ID_GENERATOR
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToWrite, FeatureWriterFn}
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, Z3FeatureIdGenerator}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, ScalaSimpleFeatureFactory, SimpleFeatureSerializer}
import org.locationtech.geomesa.security.SecurityUtils.FEATURE_VISIBILITY
import org.locationtech.geomesa.utils.uuid.FeatureIdGenerator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

object AccumuloFeatureWriter extends Logging {

  type FeatureToMutations = (FeatureToWrite) => Seq[Mutation]
  type FeatureWriterFn    = (FeatureToWrite) => Unit
  type TableAndWriter     = (String, FeatureToMutations)

  type AccumuloRecordWriter = RecordWriter[Key, Value]

  val tempFeatureIds = new AtomicLong(0)

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
  def getTablesAndWriters(sft: SimpleFeatureType, ds: AccumuloConnectorCreator): Seq[TableAndWriter] =
    GeoMesaTable.getTables(sft).map(table => (ds.getTableName(sft.getTypeName, table), table.writer(sft)))

  /**
   * Gets removers and table names for each table (e.g. index) that supports the sft
   */
  def getTablesAndRemovers(sft: SimpleFeatureType, ds: AccumuloConnectorCreator): Seq[TableAndWriter] =
    GeoMesaTable.getTables(sft).map(table => (ds.getTableName(sft.getTypeName, table), table.remover(sft)))

  private val idGenerator: FeatureIdGenerator =
    try {
      logger.debug(s"Using feature id generator '${FEATURE_ID_GENERATOR.get}'")
      Class.forName(FEATURE_ID_GENERATOR.get).newInstance().asInstanceOf[FeatureIdGenerator]
    } catch {
      case e: Throwable =>
        logger.error(s"Could not load feature id generator class '${FEATURE_ID_GENERATOR.get}'", e)
        new Z3FeatureIdGenerator
    }

  /**
   * Sets the feature ID on the feature. If the user has requested a specific ID, that will be used,
   * otherwise one will be generated. If possible, the original feature will be modified and returned.
   */
  def featureWithFid(sft: SimpleFeatureType, feature: SimpleFeature): SimpleFeature = {
    if (feature.getUserData.containsKey(Hints.PROVIDED_FID)) {
      withFid(sft, feature, feature.getUserData.get(Hints.PROVIDED_FID).toString)
    } else if (feature.getUserData.containsKey(Hints.USE_PROVIDED_FID) &&
        feature.getUserData.get(Hints.USE_PROVIDED_FID).asInstanceOf[Boolean]) {
      feature
    } else {
      withFid(sft, feature, idGenerator.createId(sft, feature))
    }
  }

  private def withFid(sft: SimpleFeatureType, feature: SimpleFeature, fid: String): SimpleFeature =
    feature.getIdentifier match {
      case f: FeatureIdImpl =>
        f.setID(fid)
        feature
      case f =>
        logger.warn(s"Unknown feature ID implementation found, rebuilding feature: ${f.getClass} $f")
        ScalaSimpleFeatureFactory.copyFeature(sft, feature, fid)
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
    val writers = AccumuloFeatureWriter.getTablesAndWriters(sft, ds).map {
      case (table, write) => (multiBWWriter.getBatchWriter(table), write)
    }
    AccumuloFeatureWriter.featureWriter(writers)
  }

  // returns a temporary id - we will replace it just before write
  protected def nextFeatureId = AccumuloFeatureWriter.tempFeatureIds.getAndIncrement().toString

  protected def writeToAccumulo(feature: SimpleFeature): Unit = {
    // require non-null geometry to write to geomesa (can't index null geo, yo)
    if (feature.getDefaultGeometry == null) {
      logger.warn(s"Invalid feature to write (no default geometry): ${DataUtilities.encodeFeature(feature)}")
      return
    }

    // see if there's a suggested ID to use for this feature, else create one based on the feature
    val featureWithFid = AccumuloFeatureWriter.featureWithFid(sft, feature)

    writer(new FeatureToWrite(featureWithFid, defaultVisibility, encoder, indexValueEncoder))
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = multiBWWriter.close()

  override def remove(): Unit = {}

  override def hasNext: Boolean = false

  def flush(): Unit = multiBWWriter.flush()
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
    val writers = AccumuloFeatureWriter.getTablesAndRemovers(sft, ds).map {
      case (table, write) => (multiBWWriter.getBatchWriter(table), write)
    }
    AccumuloFeatureWriter.featureWriter(writers)
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
      // set the use provided FID hint - allows user to update fid if desired,
      // but if not we'll use the existing one
      original.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      ScalaSimpleFeatureFactory.copyFeature(sft, original, original.getID) // this copies user data as well
    } else {
      ScalaSimpleFeatureFactory.buildFeature(sft, Seq.empty, nextFeatureId)
    }
    live
  }

  override def close() = {
    super.close() // closes writer
    reader.close()
  }

}
