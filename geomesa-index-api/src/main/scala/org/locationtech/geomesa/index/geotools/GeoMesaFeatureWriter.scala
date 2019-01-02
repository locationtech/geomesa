/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.FlushableFeatureWriter
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.stats.StatUpdater
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.locationtech.geomesa.utils.io.{CloseQuietly, FlushQuietly}
import org.locationtech.geomesa.utils.uuid.{FeatureIdGenerator, Z3FeatureIdGenerator}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

trait GeoMesaFeatureWriter[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, T]
    extends SimpleFeatureWriter with Flushable with LazyLogging {

  import scala.collection.JavaConverters._

  def ds: DS
  def sft: SimpleFeatureType
  def indices: Seq[GeoMesaFeatureIndex[DS, F, W]]

  private val exceptions: ArrayBuffer[Throwable] = ArrayBuffer.empty[Throwable]

  protected val statUpdater: StatUpdater = ds.stats.statUpdater(sft)

  override def getFeatureType: SimpleFeatureType = sft

  protected def createMutator(table: String): T

  protected def writers(feature: SimpleFeature): Seq[(T, F => Seq[W])]

  protected def removers(feature: SimpleFeature): Seq[(T, F => Seq[W])]

  protected def executeWrite(mutator: T, writes: Seq[W]): Unit

  protected def executeRemove(mutator: T, removes: Seq[W]): Unit

  protected def wrapFeature(feature: SimpleFeature): F

  protected def writeFeature(feature: SimpleFeature): Unit = {
    // see if there's a suggested ID to use for this feature, else create one based on the feature
    val featureWithFid = GeoMesaFeatureWriter.featureWithFid(sft, feature)
    val wrapped = wrapFeature(featureWithFid)
    // calculate all mutations up front in case the feature is not valid, so we don't write partial entries
    val converted = try {
      writers(featureWithFid).map { case (mutator, convert) => (mutator, convert(wrapped)) }
    } catch {
      case NonFatal(e) =>
        val attributes = s"${featureWithFid.getID}:${featureWithFid.getAttributes.asScala.mkString("|")}"
        throw new IllegalArgumentException(s"Error indexing feature '$attributes'", e)
    }
    converted.foreach { case (mutator, writes) => executeWrite(mutator, writes) }
    statUpdater.add(featureWithFid)
  }

  protected def removeFeature(feature: SimpleFeature): Unit = {
    val wrapped = wrapFeature(feature)
    removers(feature).foreach { case (mutator, convert) => executeRemove(mutator, convert(wrapped)) }
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
}

object GeoMesaFeatureWriter extends LazyLogging {

  private type FeatureConverters[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W] =
    (Seq[F => Seq[W]], Seq[F => Seq[W]])

  private type UntypedFeatureConverters = (Seq[Any => Seq[Any]], Seq[Any => Seq[Any]])

  private val tempFeatureIds = new AtomicLong(0)

  // note: re-load periodically to get any schema modifications
  private val converterCache =
    Caffeine.newBuilder().expireAfterWrite(60, TimeUnit.MINUTES).build[String, UntypedFeatureConverters]()

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

  private def withFid(sft: SimpleFeatureType, feature: SimpleFeature, fid: String): SimpleFeature = {
    feature.getIdentifier match {
      case f: FeatureIdImpl => f.setID(fid); feature
      case f =>
        logger.warn(s"Unknown FeatureID implementation found, rebuilding feature: $f '${f.getClass.getName}'")
        ScalaSimpleFeature.copy(sft, feature)
    }
  }

  /**
    * Gets table names and converters for each table (e.g. index) that supports the sft
    *
    * @param sft simple feature type
    * @param ds data store
    * @param indices indices to write/delete
    * @return (table names, write converters, remove converters)
    */
  private def getIndexConverters[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
      (sft: SimpleFeatureType, ds: DS, indices: Seq[GeoMesaFeatureIndex[DS, F, W]]): FeatureConverters[DS, F, W] = {
    val key = s"${ds.config.catalog};${CacheKeyGenerator.cacheKey(sft)};${indices.map(_.identifier).mkString(",")}"

    val load = new java.util.function.Function[String, UntypedFeatureConverters] {
      override def apply(ignored: String): UntypedFeatureConverters = {
        val writers = indices.map(_.writer(sft, ds)).asInstanceOf[Seq[Any => Seq[Any]]]
        val removers = indices.map(_.remover(sft, ds)).asInstanceOf[Seq[Any => Seq[Any]]]
        (writers, removers)
      }
    }

    converterCache.get(key, load).asInstanceOf[FeatureConverters[DS, F, W]]
  }

  private [geomesa] def expireConverterCache(): Unit = converterCache.invalidateAll()

  /**
    * Factory for creating feature writers
    */
  trait FeatureWriterFactory[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W] {

    /**
      * Create a feature writer, either for appending or modifying
      *
      * @param sft simple feature type
      * @param indices indices to write to
      * @param filter if defined, indicates a modifying feature writer, otherwise an appending one
      * @return
      */
    def createFeatureWriter(sft: SimpleFeatureType,
                            indices: Seq[GeoMesaFeatureIndex[DS, F, W]],
                            filter: Option[Filter]): FlushableFeatureWriter
  }

  /**
    * Writes to a single table per index
    */
  trait TableFeatureWriter[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, T]
      extends GeoMesaFeatureWriter[DS, F, W, T] {

    private val (writeConverters, removeConverters) =
      GeoMesaFeatureWriter.getIndexConverters[DS, F, W](sft, ds, indices)

    private val mutators = indices.flatMap(_.getTableNames(sft, ds, None)).map(createMutator)
    private val writers = mutators.zip(writeConverters)
    private val removers = mutators.zip(removeConverters)

    override protected def writers(feature: SimpleFeature): Seq[(T, F => Seq[W])] = writers
    override protected def removers(feature: SimpleFeature): Seq[(T, F => Seq[W])] = removers

    abstract override def flush(): Unit = {
      mutators.foreach {
        case m: Flushable => FlushQuietly(m).foreach(suppressException)
        case _ => // no-op
      }
      FlushQuietly(statUpdater).foreach(suppressException)
      try { super.flush() } catch {
        case NonFatal(e) => suppressException(e)
      }
      propagateExceptions()
    }

    abstract override def close(): Unit = {
      mutators.foreach {
        case m: Closeable => CloseQuietly(m).foreach(suppressException)
        case _ => // no-op
      }
      CloseQuietly(statUpdater).foreach(suppressException)
      try { super.close() } catch {
        case NonFatal(e) => suppressException(e)
      }
      propagateExceptions()
    }
  }

  /**
    * Support for writing to partitioned tables
    *
    */
  trait PartitionedFeatureWriter[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, T]
      extends GeoMesaFeatureWriter[DS, F, W, T] {

    private val partition = TablePartition(ds, sft).getOrElse {
      throw new IllegalStateException("Creating a partitioned writer for a non-partitioned schema")
    }

    private val (writeConverters, removeConverters) =
      GeoMesaFeatureWriter.getIndexConverters[DS, F, W](sft, ds, indices)

    private val cache = scala.collection.mutable.Map.empty[String, (Seq[(T, F => Seq[W])], Seq[(T, F => Seq[W])])]

    override protected def writers(feature: SimpleFeature): Seq[(T, F => Seq[W])] = mutators(feature)._1
    override protected def removers(feature: SimpleFeature): Seq[(T, F => Seq[W])] = mutators(feature)._2

    private def mutators(feature: SimpleFeature): (Seq[(T, F => Seq[W])], Seq[(T, F => Seq[W])]) = {
      val p = partition.partition(feature)
      cache.getOrElseUpdate(p, {
        // reconfigure the partition each time - this should be idempotent, and block
        // until it is fully created (which may happen in some other thread)
        val mutators = indices.par.map(i => createMutator(i.configure(sft, ds, Some(p)))).seq
        (mutators.zip(writeConverters), mutators.zip(removeConverters))
      })
    }

    abstract override def flush(): Unit = {
      cache.foreach { case (_, (mutators, _)) =>
        mutators.foreach {
          case (m: Flushable, _) => FlushQuietly(m).foreach(suppressException)
          case _ => // no-op
        }
      }
      FlushQuietly(statUpdater).foreach(suppressException)
      try { super.flush() } catch {
        case NonFatal(e) => suppressException(e)
      }
      propagateExceptions()
    }

    abstract override def close(): Unit = {
      cache.foreach { case (_, (mutators, _)) =>
        mutators.foreach {
          case (m: Closeable, _) => CloseQuietly(m).foreach(suppressException)
          case _ => // no-op
        }
      }
      CloseQuietly(statUpdater).foreach(suppressException)
      try { super.close() } catch {
        case NonFatal(e) => suppressException(e)
      }
      propagateExceptions()
    }
  }

  /**
    * Appends new features - can't modify or delete existing features.
    */
  trait GeoMesaAppendFeatureWriter[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, T]
      extends GeoMesaFeatureWriter[DS, F, W, T] {

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
      writeFeature(currentFeature)
      currentFeature = null
    }

    override def remove(): Unit =
      throw new UnsupportedOperationException("Use getFeatureWriter instead of getFeatureWriterAppend")

    override protected def executeRemove(mutator: T, removes: Seq[W]): Unit = throw new NotImplementedError()
  }

  /**
    * Modifies or deletes existing features. Per the data store api, does not allow appending new features.
    */
  trait GeoMesaModifyFeatureWriter[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, T]
      extends GeoMesaFeatureWriter[DS, F, W, T] {

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
      // only write if feature has actually changed...
      // comparison of feature ID and attributes - doesn't consider concrete class used
      if (!ScalaSimpleFeature.equalIdAndAttributes(live, original)) {
        removeFeature(original)
        writeFeature(live)
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
