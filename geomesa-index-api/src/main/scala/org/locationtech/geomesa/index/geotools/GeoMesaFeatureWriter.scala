/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.geotools

import java.io.{Closeable, Flushable}
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.{Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.{ScalaSimpleFeature, ScalaSimpleFeatureFactory}
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.uuid.{FeatureIdGenerator, Z3FeatureIdGenerator}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

object GeoMesaFeatureWriter extends LazyLogging {

  private val tempFeatureIds = new AtomicLong(0)

  private val idGenerator: FeatureIdGenerator = {
    import org.locationtech.geomesa.index.conf.FeatureIdProperties.FEATURE_ID_GENERATOR
    try {
      logger.debug(s"Using feature id generator '${FEATURE_ID_GENERATOR.get}'")
      Class.forName(FEATURE_ID_GENERATOR.get).newInstance().asInstanceOf[FeatureIdGenerator]
    } catch {
      case e: Throwable =>
        logger.error(s"Could not load feature id generator class '${FEATURE_ID_GENERATOR.get}'", e)
        new Z3FeatureIdGenerator
    }
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

  /**
    * Gets writers and table names for each table (e.g. index) that supports the sft
    */
  def getTablesAndConverters[DS <: GeoMesaDataStore[DS, F, W, Q], F <: WrappedFeature, W, Q](sft: SimpleFeatureType, ds: DS,
      indices: Option[Seq[GeoMesaFeatureIndex[DS, F, W, Q]]] = None): (Seq[String], Seq[(F) => W], Seq[(F) => W]) = {
    val toWrite = indices.getOrElse(ds.manager.indices(sft, IndexMode.Write))
    val tables = toWrite.map(_.getTableName(sft.getTypeName, ds))
    val writers = toWrite.map(_.writer(sft, ds))
    val removers = toWrite.map(_.remover(sft, ds))
    (tables, writers, removers)
  }
}

abstract class GeoMesaFeatureWriter[DS <: GeoMesaDataStore[DS, F, W, Q], F <: WrappedFeature, W, Q, T](val sft: SimpleFeatureType, val ds: DS)
    extends SimpleFeatureWriter with Flushable with LazyLogging {

  private val (tables, wConverters, rConverters) = GeoMesaFeatureWriter.getTablesAndConverters[DS, F, W, Q](sft, ds, None)
  protected val mutators = createMutators(tables)
  private val writers = wConverters.zip(createWrites(mutators))
  protected val writer = (f: F) => writers.foreach { case (convert, write) => write(convert(f)) }
  private lazy val removers = rConverters.zip(createRemoves(mutators))
  protected lazy val remover = (f: F) => removers.foreach { case (convert, remove) => remove(convert(f)) }

  protected val statUpdater = ds.stats.statUpdater(sft)

  // returns a temporary id - we will replace it just before write
  protected def nextFeatureId = GeoMesaFeatureWriter.tempFeatureIds.getAndIncrement().toString

  protected def writeFeature(feature: SimpleFeature): Unit = {
    // see if there's a suggested ID to use for this feature, else create one based on the feature
    val featureWithFid = GeoMesaFeatureWriter.featureWithFid(sft, feature)
    writer(wrapFeature(featureWithFid))
    statUpdater.add(featureWithFid)
  }

  protected def createMutators(tables: Seq[String]): Seq[T]

  protected def createWrites(mutators: Seq[T]): Seq[(W) => Unit]

  protected def createRemoves(mutators: Seq[T]): Seq[(W) => Unit]

  protected def wrapFeature(feature: SimpleFeature): F

  override def getFeatureType: SimpleFeatureType = sft

  override def hasNext: Boolean = false

  override def flush(): Unit = {
    mutators.collect { case c: Flushable => c }.foreach(_.flush())
    statUpdater.flush()
  }

  override def close(): Unit = {
    mutators.collect { case c: Closeable => c }.foreach(_.close())
    statUpdater.close()
  }
}

/**
 * Appends new features - can't modify or delete existing features.
 */
trait GeoMesaAppendFeatureWriter[DS <: GeoMesaDataStore[DS, F, W, Q], F <: WrappedFeature, W, Q, T]
    extends GeoMesaFeatureWriter[DS, F, W, Q, T] {

  var currentFeature: SimpleFeature = null

  override def write(): Unit =
    if (currentFeature != null) {
      writeFeature(currentFeature)
      currentFeature = null
    }

  override def remove(): Unit =
    throw new UnsupportedOperationException("Use getFeatureWriter instead of getFeatureWriterAppend")

  override def next(): SimpleFeature = {
    currentFeature = new ScalaSimpleFeature(nextFeatureId, sft)
    currentFeature
  }

  override protected def createRemoves(mutators: Seq[T]): Seq[(W) => Unit] = throw new NotImplementedError()
}

/**
 * Modifies or deletes existing features. Per the data store api, does not allow appending new features.
 */
trait GeoMesaModifyFeatureWriter[DS <: GeoMesaDataStore[DS, F, W, Q], F <: WrappedFeature, W, Q, T]
    extends GeoMesaFeatureWriter[DS, F, W, Q, T] {

  def filter: Filter

  private val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)

  // feature that caller will modify
  private var live: SimpleFeature = null
  // feature returned from reader
  private var original: SimpleFeature = null

  override def remove() = if (original != null) {
    remover(wrapFeature(original))
    statUpdater.remove(original)
  }

  override def hasNext = reader.hasNext

  /* only write if non null and it hasn't changed...*/
  /* original should be null only when reader runs out */
  override def write() =
    // comparison of feature ID and attributes - doesn't consider concrete class used
    if (!ScalaSimpleFeature.equalIdAndAttributes(live, original)) {
      remove()
      writeFeature(live)
    }

  override def next: SimpleFeature = {
    original = reader.next()
    // set the use provided FID hint - allows user to update fid if desired,
    // but if not we'll use the existing one
    original.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    live = ScalaSimpleFeatureFactory.copyFeature(sft, original, original.getID) // this copies user data as well
    live
  }

  abstract override def close(): Unit = {
    super.close() // closes writer
    reader.close()
  }
}
