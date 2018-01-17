/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.io.{Closeable, Flushable}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.github.benmanes.caffeine.cache.Caffeine
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.{Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.{ScalaSimpleFeature, ScalaSimpleFeatureFactory}
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter.FlushableFeatureWriter
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.io.{CloseQuietly, FlushQuietly}
import org.locationtech.geomesa.utils.uuid.{FeatureIdGenerator, Z3FeatureIdGenerator}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object GeoMesaFeatureWriter extends LazyLogging {

  private val tempFeatureIds = new AtomicLong(0)

  private val converterCache =
    Caffeine.newBuilder()
      .expireAfterWrite(60, TimeUnit.MINUTES) // re-load periodically
      .build[String, (IndexedSeq[String], IndexedSeq[(Any) => Seq[Any]], IndexedSeq[(Any) => Seq[Any]])]()

  private val idGenerator: FeatureIdGenerator = {
    import org.locationtech.geomesa.index.conf.FeatureProperties.FEATURE_ID_GENERATOR
    try {
      logger.debug(s"Using feature id generator '${FEATURE_ID_GENERATOR.get}'")
      Class.forName(FEATURE_ID_GENERATOR.get).newInstance().asInstanceOf[FeatureIdGenerator]
    } catch {
      case e: Throwable =>
        logger.error(s"Could not load feature id generator class '${FEATURE_ID_GENERATOR.get}'", e)
        new Z3FeatureIdGenerator
    }
  }

  trait FlushableFeatureWriter extends SimpleFeatureWriter with Flushable

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
    * Gets table names and converters for each table (e.g. index) that supports the sft
    *
    * @param sft simple feature type
    * @param ds data store
    * @param indices indices to write/delete
    * @return (table names, write converters, remove converters)
    */
  def getTablesAndConverters[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W](
      sft: SimpleFeatureType,
      ds: DS,
      indices: Option[Seq[GeoMesaFeatureIndex[DS, F, W]]] = None): (IndexedSeq[String], IndexedSeq[(F) => Seq[W]], IndexedSeq[(F) => Seq[W]]) = {
    val toWrite = indices.getOrElse(ds.manager.indices(sft, IndexMode.Write)).toIndexedSeq
    val key = s"${ds.config.catalog};${CacheKeyGenerator.cacheKey(sft)};${toWrite.map(_.identifier).mkString(",")}"

    val load = new java.util.function.Function[String, (IndexedSeq[String], IndexedSeq[(Any) => Seq[Any]], IndexedSeq[(Any) => Seq[Any]])] {
      override def apply(ignored: String): (IndexedSeq[String], IndexedSeq[(Any) => Seq[Any]], IndexedSeq[(Any) => Seq[Any]]) = {
        val tables = toWrite.map(_.getTableName(sft.getTypeName, ds))
        val writers = toWrite.map(_.writer(sft, ds))
        val removers = toWrite.map(_.remover(sft, ds))
        (tables, writers.asInstanceOf[IndexedSeq[(Any) => Seq[Any]]], removers.asInstanceOf[IndexedSeq[(Any) => Seq[Any]]])
      }
    }

    converterCache.get(key, load).asInstanceOf[(IndexedSeq[String], IndexedSeq[(F) => Seq[W]], IndexedSeq[(F) => Seq[W]])]
  }

  private [geomesa] def expireConverterCache(): Unit = converterCache.invalidateAll()
}

abstract class GeoMesaFeatureWriter[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, T]
    (val sft: SimpleFeatureType, val ds: DS, val indices: Option[Seq[GeoMesaFeatureIndex[DS, F, W]]])
    extends FlushableFeatureWriter with LazyLogging {

  private val statUpdater = ds.stats.statUpdater(sft)

  private val (tables, writeConverters, removeConverters) =
    GeoMesaFeatureWriter.getTablesAndConverters[DS, F, W](sft, ds, indices)

  protected val mutators: IndexedSeq[T] = createMutators(tables)
  private val writers = mutators.zip(writeConverters)
  private val removers = mutators.zip(removeConverters)

  protected val exceptions: ArrayBuffer[Throwable] = ArrayBuffer.empty[Throwable]

  // returns a temporary id - we will replace it just before write
  protected def nextFeatureId: String = GeoMesaFeatureWriter.tempFeatureIds.getAndIncrement().toString

  protected def writeFeature(feature: SimpleFeature): Unit = {
    // see if there's a suggested ID to use for this feature, else create one based on the feature
    val featureWithFid = GeoMesaFeatureWriter.featureWithFid(sft, feature)
    val wrapped = wrapFeature(featureWithFid)
    // calculate all mutations up front in case the feature is not valid, so we don't write partial entries
    val converted = try { writers.map { case (mutator, convert) => (mutator, convert(wrapped)) } } catch {
      case NonFatal(e) =>
        import scala.collection.JavaConversions._
        val attributes = s"${featureWithFid.getID}:${featureWithFid.getAttributes.mkString("|")}"
        throw new IllegalArgumentException(s"Error indexing feature '$attributes'", e)
    }
    converted.foreach { case (mutator, writes) => executeWrite(mutator, writes) }
    statUpdater.add(featureWithFid)
  }

  protected def removeFeature(feature: SimpleFeature): Unit = {
    val wrapped = wrapFeature(feature)
    removers.foreach { case (mutator, convert) => executeRemove(mutator, convert(wrapped)) }
    statUpdater.remove(feature)
  }

  protected def createMutators(tables: IndexedSeq[String]): IndexedSeq[T]

  protected def executeWrite(mutator: T, writes: Seq[W]): Unit

  protected def executeRemove(mutator: T, removes: Seq[W]): Unit

  protected def wrapFeature(feature: SimpleFeature): F

  override def getFeatureType: SimpleFeatureType = sft

  override def hasNext: Boolean = false

  override def flush(): Unit = {
    mutators.foreach {
      case m: Flushable => FlushQuietly(m).foreach(exceptions.+=)
      case _ => // no-op
    }
    FlushQuietly(statUpdater).foreach(exceptions.+=)
    propagateExceptions()
  }

  override def close(): Unit = {
    mutators.foreach {
      case m: Closeable => CloseQuietly(m).foreach(exceptions.+=)
      case _ => // no-op
    }
    CloseQuietly(statUpdater).foreach(exceptions.+=)
    propagateExceptions()
  }

  private def propagateExceptions(): Unit = {
    if (exceptions.nonEmpty) {
      val msg = s"Error writing features: ${exceptions.map(_.getMessage).distinct.mkString("; ")}"
      val cause = exceptions.head
      exceptions.clear()
      throw new RuntimeException(msg, cause)
    }
  }
}

/**
 * Appends new features - can't modify or delete existing features.
 */
trait GeoMesaAppendFeatureWriter[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, T]
    extends GeoMesaFeatureWriter[DS, F, W, T] {

  var currentFeature: SimpleFeature = _

  override def write(): Unit =
    if (currentFeature != null) {
      writeFeature(currentFeature)
      currentFeature = null
    }

  override def remove(): Unit =
    throw new UnsupportedOperationException("Use getFeatureWriter instead of getFeatureWriterAppend")

  override def next(): SimpleFeature = {
    currentFeature = new ScalaSimpleFeature(sft, nextFeatureId)
    currentFeature
  }

  override protected def executeRemove(mutator: T, removes: Seq[W]): Unit = throw new NotImplementedError()
}

/**
 * Modifies or deletes existing features. Per the data store api, does not allow appending new features.
 */
trait GeoMesaModifyFeatureWriter[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, T]
    extends GeoMesaFeatureWriter[DS, F, W, T] {

  def filter: Filter

  private val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)

  // feature that caller will modify
  private var live: SimpleFeature = _
  // feature returned from reader
  private var original: SimpleFeature = _

  override def remove(): Unit = if (original != null) {
    removeFeature(original)
  }

  override def hasNext: Boolean = reader.hasNext

  /* only write if non null and it hasn't changed...*/
  /* original should be null only when reader runs out */
  override def write(): Unit =
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
