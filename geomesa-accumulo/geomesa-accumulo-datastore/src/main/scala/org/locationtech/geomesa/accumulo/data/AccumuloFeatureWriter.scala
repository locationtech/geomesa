/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.io.Flushable
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.RecordWriter
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.{Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.FeatureIdProperties.FEATURE_ID_GENERATOR
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToWrite, FeatureWriterFn}
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, Z3FeatureIdGenerator}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.{ScalaSimpleFeature, ScalaSimpleFeatureFactory, SimpleFeatureSerializer}
import org.locationtech.geomesa.security.SecurityUtils.FEATURE_VISIBILITY
import org.locationtech.geomesa.utils.uuid.FeatureIdGenerator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

object AccumuloFeatureWriter extends LazyLogging {

  type FeatureToMutations = (FeatureToWrite) => Seq[Mutation]
  type FeatureWriterFn    = (FeatureToWrite) => Unit
  type TableAndWriter     = (String, FeatureToMutations)

  type AccumuloRecordWriter = RecordWriter[Key, Value]

  val tempFeatureIds = new AtomicLong(0)

  class FeatureToWrite(val feature: SimpleFeature,
                       defaultVisibility: String,
                       serializer: SimpleFeatureSerializer,
                       indexValueEncoder: IndexValueEncoder,
                       binEncoder: Option[BinEncoder]) {

    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature

    lazy val visibility = new Text(feature.userData[String](FEATURE_VISIBILITY).getOrElse(defaultVisibility))

    lazy val columnVisibility = new ColumnVisibility(visibility)
    // the index value is the encoded date/time/fid
    lazy val indexValue = new Value(indexValueEncoder.encode(feature))
    // the data value is the encoded SimpleFeature
    lazy val dataValue = new Value(serializer.serialize(feature))
    // bin formatted value
    lazy val binValue = binEncoder.map(e => new Value(e.encode(feature)))
    // hash value of the feature id
    lazy val idHash = Math.abs(MurmurHash3.stringHash(feature.getID))

    // TODO GEOMESA-1254 optimize for case where all vis are the same
    lazy val perAttributeValues: Seq[RowValue] = {
      val count = feature.getFeatureType.getAttributeCount
      val visibilities = feature.userData[String](FEATURE_VISIBILITY).map(_.split(","))
          .getOrElse(Array.fill(count)(defaultVisibility))
      require(visibilities.length == count, "Per-attribute visibilities do not match feature type")
      val groups = visibilities.zipWithIndex.groupBy(_._1).mapValues(_.map(_._2.toByte).sorted).toSeq
      groups.map { case (vis, indices) =>
        val cq = new Text(indices)
        val values = indices.map(i => serializer.serialize(i, feature.getAttribute(i)))
        val output = KryoFeatureSerializer.getOutput() // note: same output object used in serializer.serialize
        values.foreach { value =>
          output.writeInt(value.length, true)
          output.write(value)
        }
        val value = new Value(output.toBytes)
        RowValue(GeoMesaTable.AttributeColumnFamily, cq, new ColumnVisibility(vis), value)
      }
    }
  }

  def featureWriter(writers: Seq[(BatchWriter, FeatureToMutations)]): FeatureWriterFn = feature => {
    // calculate all the mutations first, so that if something fails we won't have a partially written feature
    val mutations = writers.map { case (bw, fToM) => (bw, fToM(feature)) }
    mutations.foreach { case (bw, m) => bw.addMutations(m) }
  }

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

  case class RowValue(cf: Text, cq: Text, vis: ColumnVisibility, value: Value)
}

abstract class AccumuloFeatureWriter(sft: SimpleFeatureType,
                                     encoder: SimpleFeatureSerializer,
                                     ds: AccumuloDataStore,
                                     defaultVisibility: String)
    extends SimpleFeatureWriter with Flushable with LazyLogging {

  protected val multiBWWriter = ds.connector.createMultiTableBatchWriter(GeoMesaBatchWriterConfig())
  protected val binEncoder = BinEncoder(sft)
  protected val indexValueEncoder = IndexValueEncoder(sft)

  // A "writer" is a function that takes a simple feature and writes it to an index or table
  protected val writer: FeatureWriterFn = {
    val writers = AccumuloFeatureWriter.getTablesAndWriters(sft, ds).map {
      case (table, write) => (multiBWWriter.getBatchWriter(table), write)
    }
    AccumuloFeatureWriter.featureWriter(writers)
  }

  protected val statUpdater = ds.stats.statUpdater(sft)

  // returns a temporary id - we will replace it just before write
  protected def nextFeatureId = AccumuloFeatureWriter.tempFeatureIds.getAndIncrement().toString

  protected def writeToAccumulo(feature: SimpleFeature): Unit = {
    // see if there's a suggested ID to use for this feature, else create one based on the feature
    val featureWithFid = AccumuloFeatureWriter.featureWithFid(sft, feature)
    writer(new FeatureToWrite(featureWithFid, defaultVisibility, encoder, indexValueEncoder, binEncoder))
    statUpdater.add(featureWithFid)
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def hasNext: Boolean = false

  override def flush(): Unit = {
    multiBWWriter.flush()
    statUpdater.flush()
  }

  override def close(): Unit = {
    multiBWWriter.close()
    statUpdater.close()
  }
}

/**
 * Appends new features - can't modify or delete existing features.
 */
class AppendAccumuloFeatureWriter(sft: SimpleFeatureType,
                                  encoder: SimpleFeatureSerializer,
                                  ds: AccumuloDataStore,
                                  defaultVisibility: String)
  extends AccumuloFeatureWriter(sft, encoder, ds, defaultVisibility) {

  var currentFeature: SimpleFeature = null

  override def write(): Unit =
    if (currentFeature != null) {
      writeToAccumulo(currentFeature)
      currentFeature = null
    }

  override def remove(): Unit =
    throw new UnsupportedOperationException("Use getFeatureWriter instead of getFeatureWriterAppend")

  override def next(): SimpleFeature = {
    currentFeature = new ScalaSimpleFeature(nextFeatureId, sft)
    currentFeature
  }
}

/**
 * Modifies or deletes existing features. Per the data store api, does not allow appending new features.
 */
class ModifyAccumuloFeatureWriter(sft: SimpleFeatureType,
                                  encoder: SimpleFeatureSerializer,
                                  ds: AccumuloDataStore,
                                  defaultVisibility: String,
                                  filter: Filter)
  extends AccumuloFeatureWriter(sft, encoder, ds, defaultVisibility) {

  val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)

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
    remover(new FeatureToWrite(original, defaultVisibility, encoder, indexValueEncoder, binEncoder))
    statUpdater.remove(original)
  }

  override def hasNext = reader.hasNext

  /* only write if non null and it hasn't changed...*/
  /* original should be null only when reader runs out */
  override def write() =
    // comparison of feature ID and attributes - doesn't consider concrete class used
    if (!ScalaSimpleFeature.equalIdAndAttributes(live, original)) {
      remove()
      writeToAccumulo(live)
    }

  override def next: SimpleFeature = {
    original = reader.next()
    // set the use provided FID hint - allows user to update fid if desired,
    // but if not we'll use the existing one
    original.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    live = ScalaSimpleFeatureFactory.copyFeature(sft, original, original.getID) // this copies user data as well
    live
  }

  override def close() = {
    super.close() // closes writer
    reader.close()
  }
}
