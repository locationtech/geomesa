/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{Query, SimpleFeatureWriter, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.data.DataUtilities
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.api.IndexAdapter.IndexWriter
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter.WriteException
import org.locationtech.geomesa.index.stats.GeoMesaStats.StatUpdater
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.{CloseQuietly, FlushQuietly}
import org.locationtech.geomesa.utils.uuid.{FeatureIdGenerator, Z3FeatureIdGenerator}

import java.io.Flushable
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

trait GeoMesaFeatureWriter[DS <: GeoMesaDataStore[DS]] extends SimpleFeatureWriter with Flushable with LazyLogging {

  def ds: DS
  def sft: SimpleFeatureType
  def indices: Seq[GeoMesaFeatureIndex[_, _]]

  private val exceptions: ArrayBuffer[Throwable] = ArrayBuffer.empty[Throwable]

  protected val statUpdater: StatUpdater = ds.stats.writer.updater(sft)

  override def getFeatureType: SimpleFeatureType = sft

  protected def getWriter(feature: SimpleFeature): IndexWriter

  protected def updateFeature(update: SimpleFeature, previous: SimpleFeature): Unit = {
    // see if there's a suggested ID to use for this feature, else create one based on the feature
    val writable = GeoMesaFeatureWriter.featureWithFid(update)
    try {
      val writer = getWriter(writable)
      val remover = getWriter(previous)
      if (writer.eq(remover)) {
        // `update` will calculate all mutations up front in case the feature is not valid, so we don't write partial entries
        writer.update(writable, previous)
      } else {
        remover.delete(previous)
        writer.append(writable)
      }
    } catch {
      case NonFatal(e) => throwWriteErrors(e, writable)
    }
    statUpdater.add(writable)
  }

  protected def appendFeature(feature: SimpleFeature): Unit = {
    // see if there's a suggested ID to use for this feature, else create one based on the feature
    val writable = GeoMesaFeatureWriter.featureWithFid(feature)
    // `append` will calculate all mutations up front in case the feature is not valid, so we don't write partial entries
    try { getWriter(writable).append(writable) } catch {
      case NonFatal(e) => throwWriteErrors(e, writable)
    }
    statUpdater.add(writable)
  }

  protected def removeFeature(feature: SimpleFeature): Unit = {
    // the feature has come directly from our reader, so it should be valid and already have a FID
    getWriter(feature).delete(feature)
    statUpdater.remove(feature)
  }

  protected def suppressException(e: Throwable): Unit = exceptions += e

  protected def propagateExceptions(): Unit = {
    if (exceptions.nonEmpty) {
      val all = new RuntimeException(s"Error writing features:")
      exceptions.foreach(all.addSuppressed)
      exceptions.clear()
      throw all
    }
  }

  // returns a temporary id - we will replace it just before write
  protected def nextFeatureId: String = GeoMesaFeatureWriter.tempFeatureIds.getAndIncrement().toString

  @throws[Exception]
  private def throwWriteErrors(e: Throwable, feature: SimpleFeature): Unit = {
    val msg = s"Error indexing feature '${feature.getID}:${DataUtilities.encodeFeature(feature, false)}'"
    e match {
      case _: WriteException => throw e
      case _: IllegalArgumentException => throw new IllegalArgumentException(msg, e)
      case _ => throw new RuntimeException(msg, e)
    }
  }
}

object GeoMesaFeatureWriter extends LazyLogging {

  private val tempFeatureIds = new AtomicLong(0)

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

  /**
   * Create a feature writer
   *
   * @param ds datastore
   * @param sft simple feature type
   * @param indices indices to write
   * @param filter filter for selecting features for updating writes, or None for appending writes
   * @param atomic enforce atomic writes
   * @tparam DS datastore type
   * @return feature writer
   */
  def apply[DS <: GeoMesaDataStore[DS]](
      ds: DS,
      sft: SimpleFeatureType,
      indices: Seq[GeoMesaFeatureIndex[_, _]],
      filter: Option[Filter],
      atomic: Boolean): GeoMesaFeatureWriter[DS] = {
    if (TablePartition.partitioned(sft)) {
      filter match {
        case None => new PartitionFeatureWriter(ds, sft, indices, atomic) with GeoMesaAppendFeatureWriter[DS]
        case Some(f) =>
          new PartitionFeatureWriter(ds, sft, indices, atomic) with GeoMesaModifyFeatureWriter[DS] {
            override def filter: Filter = f
          }
      }
    } else {
      filter match {
        case None => new TableFeatureWriter(ds, sft, indices, atomic) with GeoMesaAppendFeatureWriter[DS]
        case Some(f) =>
          new TableFeatureWriter(ds, sft, indices, atomic) with GeoMesaModifyFeatureWriter[DS] {
            override def filter: Filter = f
          }
      }
    }
  }

  /**
   * Sets the feature ID on the feature. If the user has requested a specific ID, that will be used,
   * otherwise one will be generated. If possible, the original feature will be modified and returned.
   */
  def featureWithFid(feature: SimpleFeature): SimpleFeature = {
    if (feature.getUserData.containsKey(Hints.PROVIDED_FID)) {
      withFid(feature, feature.getUserData.get(Hints.PROVIDED_FID).toString)
    } else if (feature.getUserData.containsKey(Hints.USE_PROVIDED_FID) &&
        feature.getUserData.get(Hints.USE_PROVIDED_FID).asInstanceOf[Boolean]) {
      feature
    } else {
      withFid(feature, idGenerator.createId(feature.getFeatureType, feature))
    }
  }

  /**
   * Marker class to allow specific exceptions to bubble up
   *
   * @param msg error message
   * @param cause cause (may be null)
   */
  class WriteException(msg: String, cause: Throwable) extends RuntimeException(msg, cause) {
    def this(msg: String) = this(msg, null)
  }

  private def withFid(feature: SimpleFeature, fid: String): SimpleFeature = {
    feature match {
      case f: ScalaSimpleFeature => f.setId(fid); f
      case _ =>
        feature.getIdentifier match {
          case f: FeatureIdImpl => f.setID(fid); feature
          case f =>
            logger.warn(s"Unknown FeatureID implementation found, rebuilding feature: $f '${f.getClass.getName}'")
            val copy = ScalaSimpleFeature.copy(feature)
            copy.setId(fid)
            copy
        }
    }

  }

  /**
    * Writes to a single table per index
    */
  private abstract class TableFeatureWriter[DS <: GeoMesaDataStore[DS]](
      val ds: DS,
      val sft: SimpleFeatureType,
      val indices: Seq[GeoMesaFeatureIndex[_, _]],
      val atomic: Boolean
    ) extends GeoMesaFeatureWriter[DS] {

    private val writer = ds.adapter.createWriter(sft, indices, None, atomic)

    override protected def getWriter(feature: SimpleFeature): IndexWriter = writer

    override def flush(): Unit = {
      FlushQuietly(writer).foreach(suppressException)
      FlushQuietly(statUpdater).foreach(suppressException)
      propagateExceptions()
    }

    override def close(): Unit = {
      CloseQuietly(writer).foreach(suppressException)
      CloseQuietly(statUpdater).foreach(suppressException)
      propagateExceptions()
    }
  }

  /**
    * Support for writing to partitioned tables
    *
    */
  private abstract class PartitionFeatureWriter[DS <: GeoMesaDataStore[DS]](
      val ds: DS,
      val sft: SimpleFeatureType,
      val indices: Seq[GeoMesaFeatureIndex[_, _]],
      val atomic: Boolean
    ) extends GeoMesaFeatureWriter[DS] {

    import scala.collection.JavaConverters._

    private val partition = TablePartition(ds, sft).getOrElse {
      throw new IllegalStateException("Creating a partitioned writer for a non-partitioned schema")
    }

    private val cache = new java.util.HashMap[String, IndexWriter]()
    private val view = cache.asScala

    override protected def getWriter(feature: SimpleFeature): IndexWriter = {
      val p = partition.partition(feature)
      var writer = cache.get(p)
      if (writer == null) {
        // reconfigure the partition each time - this should be idempotent, and block
        // until it is fully created (which may happen in some other thread)
        def createOne(index: GeoMesaFeatureIndex[_, _]): Unit =
          ds.adapter.createTable(index, Some(p), index.getSplits(Some(p)))
        indices.toList.map(i => CachedThreadPool.submit(() => createOne(i))).foreach(_.get)
        writer = ds.adapter.createWriter(sft, indices, Some(p), atomic)
        cache.put(p, writer)
      }
      writer
    }

    override def flush(): Unit = {
      view.foreach { case (_, writer) => FlushQuietly(writer).foreach(suppressException) }
      FlushQuietly(statUpdater).foreach(suppressException)
      propagateExceptions()
    }

    override def close(): Unit = {
      view.foreach { case (_, writer) => CloseQuietly(writer).foreach(suppressException) }
      CloseQuietly(statUpdater).foreach(suppressException)
      propagateExceptions()
    }
  }

  /**
    * Appends new features - can't modify or delete existing features
    */
  private trait GeoMesaAppendFeatureWriter[DS <: GeoMesaDataStore[DS]] extends GeoMesaFeatureWriter[DS] {

    private var currentFeature: SimpleFeature = _

    override def hasNext: Boolean = false // per geotools spec, always return false

    override def next(): SimpleFeature = {
      currentFeature = new ScalaSimpleFeature(sft, nextFeatureId)
      currentFeature
    }

    override def write(): Unit = {
      if (currentFeature == null) {
        throw new IllegalStateException("next() must be called before write()")
      }
      appendFeature(currentFeature)
      currentFeature = null
    }

    override def remove(): Unit =
      throw new UnsupportedOperationException("Use getFeatureWriter instead of getFeatureWriterAppend")
  }

  /**
    * Modifies or deletes existing features. Per the data store api, does not allow appending new features.
    */
  private trait GeoMesaModifyFeatureWriter[DS <: GeoMesaDataStore[DS]] extends GeoMesaFeatureWriter[DS] {

    import org.locationtech.geomesa.security.SecureSimpleFeature

    def filter: Filter

    private val reader = ds.getFeatureReader(new Query(sft.getTypeName, filter), Transaction.AUTO_COMMIT)

    // feature returned from reader
    private var original: SimpleFeature = _

    // feature that caller will modify
    private var live: SimpleFeature = _

    override def hasNext: Boolean = reader.hasNext

    override def next: SimpleFeature = {
      original = reader.next()
      // set the use provided FID hint - allows user to update fid if desired,
      // but if not we'll use the existing one
      original.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      live = ScalaSimpleFeature.copy(sft, original) // this copies user data as well
      live
    }

    override def write(): Unit = {
      if (original == null) {
        throw new IllegalStateException("next() must be called before write()")
      }
      // update the feature id based on hints before we compare for changes
      live = GeoMesaFeatureWriter.featureWithFid(live)
      // only write if feature has actually changed...
      // comparison of feature ID and attributes - doesn't consider concrete class used
      if (!ScalaSimpleFeature.equalIdAndAttributes(live, original) || live.visibility != original.visibility) {
        updateFeature(live, original)
      }
      original = null
      live = null
    }

    override def remove(): Unit = {
      if (original == null) {
        throw new IllegalStateException("next() must be called before remove()")
      }
      removeFeature(original)
      original = null
      live = null
    }

    abstract override def close(): Unit = {
      CloseQuietly(reader).foreach(suppressException)
      super.close() // closes writer
    }
  }
}
