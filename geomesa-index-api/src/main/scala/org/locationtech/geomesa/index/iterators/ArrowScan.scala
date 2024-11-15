/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.ipc.message.IpcOption
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.arrow.io._
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding.Encoding
import org.locationtech.geomesa.arrow.{ArrowEncodedSft, ArrowProperties}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.text.StringSerialization

import java.util.Objects

trait ArrowScan extends AggregatingScan[ArrowScan.ArrowAggregate] {

  import org.locationtech.geomesa.index.iterators.ArrowScan._

  override def createResult(
      sft: SimpleFeatureType,
      transform: Option[SimpleFeatureType],
      batchSize: Int,
      options: Map[String, String]): ArrowAggregate = {

    import ArrowScan.Configuration._

    val arrowSft = transform.getOrElse(sft)
    val includeFids = options(IncludeFidsKey).toBoolean
    val proxyFids = options.get(ProxyFidsKey).exists(_.toBoolean)
    val flipAxisOrder = options.get(FlipAxisOrderKey).exists(_.toBoolean)
    val encoding = SimpleFeatureEncoding.min(includeFids, proxyFids, flipAxisOrder)
    val dictionary = options(DictionaryKey)
    val sort = options.get(SortKey).map(name => (name, options.get(SortReverseKey).exists(_.toBoolean)))
    val ipcOpts = FormatVersion.options(options(IpcVersionKey))
    val dictionaries = dictionary.split(",").filterNot(_.isEmpty)
    new DeltaAggregate(arrowSft, dictionaries, encoding, ipcOpts, sort, batchSize)
  }

  override protected def defaultBatchSize: Int = throw new IllegalArgumentException("Batch scan is specified per scan")
}

object ArrowScan extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  object Configuration {

    val IncludeFidsKey   = "fids"
    val ProxyFidsKey     = "proxy"
    val FlipAxisOrderKey = "flip-axis-order"
    val DictionaryKey    = "dict"
    val IpcVersionKey    = "ipc"
    val SortKey          = "sort"
    val SortReverseKey   = "sort-rev"
  }

  /**
    * Configure the iterator
    *
    * @param sft simple feature type
    * @param index feature index
    * @param stats stats, used for querying dictionaries
    * @param filter full filter from the query, if any
    * @param ecql secondary push down filter, if any
    * @param hints query hints
    * @return
    */
  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                stats: GeoMesaStats,
                filter: Option[Filter],
                ecql: Option[Filter],
                hints: Hints): ArrowScanConfig = {
    import AggregatingScan.{OptionToConfig, StringToConfig}
    import Configuration._

    val arrowSft = hints.getTransformSchema.getOrElse(sft)
    val includeFids = hints.isArrowIncludeFid
    val proxyFids = hints.isArrowProxyFid
    val flipAxisOrder = hints.isFlipAxisOrder
    val encoding = SimpleFeatureEncoding.min(includeFids, proxyFids, flipAxisOrder)
    val sort = hints.getArrowSort
    val batchSize = getBatchSize(hints)
    val ipc = hints.getArrowFormatVersion.getOrElse(FormatVersion.ArrowFormatVersion.get)
    val ipcOpts = FormatVersion.options(ipc)
    val dictionaryFields = hints.getArrowDictionaryFields

    val config = {
      val base = AggregatingScan.configure(sft, index, ecql, hints.getTransform, hints.getSampling, batchSize)
      base ++ AggregatingScan.optionalMap(
        IncludeFidsKey   -> includeFids.toString,
        ProxyFidsKey     -> proxyFids.toString,
        FlipAxisOrderKey -> flipAxisOrder.toString,
        IpcVersionKey    -> ipc,
        SortKey          -> sort.map(_._1),
        SortReverseKey   -> sort.map(_._2.toString),
        DictionaryKey    -> dictionaryFields.mkString(",")
      )
    }

    val process = hints.isArrowProcessDeltas
    val reducer = new DeltaReducer(arrowSft, dictionaryFields, encoding, ipcOpts, batchSize, sort, sorted = false, process)
    ArrowScanConfig(config, reducer)
  }

  /**
    * Gets the batch size from query hints, or falls back to the system property
    *
    * @param hints query hints
    * @return
    */
  def getBatchSize(hints: Hints): Int = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)

  /**
    * Simple feature used for returning from scans
    *
    * @return
    */
  def resultFeature(): SimpleFeature =
    new ScalaSimpleFeature(ArrowEncodedSft, "", Array(null, GeometryUtils.zeroPoint))

  /**
    * Trait for aggregating arrow files
    */
  trait ArrowAggregate extends AggregatingScan.Result

  /**
    * Returns batches of [threading key][dictionary deltas][record batch]. Will sort each batch,
    * but not between batches.
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionary fields
    * @param encoding arrow encoding
    * @param sort sort field, sort reverse
    * @param batchSize batch size
    */
  class DeltaAggregate(
      sft: SimpleFeatureType,
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      batchSize: Int
    ) extends ArrowAggregate {

    private val features = Array.ofDim[SimpleFeature](batchSize)

    private var writer: DeltaWriter = _

    private var index = 0

    override def init(): Unit = {
      writer = new DeltaWriter(sft, dictionaryFields, encoding, ipcOpts, sort, batchSize)
    }

    override def aggregate(sf: SimpleFeature): Int = {
      // we have to copy since the feature might be re-used
      // TODO we could probably optimize this...
      features(index) = ScalaSimpleFeature.copy(sf)
      index += 1
      1
    }

    override def encode(): Array[Byte] = try { writer.encode(features, index) } finally { index = 0 }

    override def cleanup(): Unit = {
      CloseWithLogging(writer)
      writer = null
    }
  }

  /**
    * Reduce function for delta batches.
    *
    * First feature contains metadata for arrow file and dictionary batch, subsequent features
    * contain record batches, final feature contains EOF indicator
    *
    * @param sft simple feature type
    * @param dictionaryFields dictionaries
    * @param encoding encoding
    * @param batchSize batch size
    * @param sort sort
    * @return
    */
  class DeltaReducer(
      private var sft: SimpleFeatureType,
      private var dictionaryFields: Seq[String],
      private var encoding: SimpleFeatureEncoding,
      private var ipcOpts: IpcOption,
      private var batchSize: Int,
      private var sort: Option[(String, Boolean)],
      private var sorted: Boolean,
      private var process: Boolean
    ) extends FeatureReducer {

    def this() = this(null, null, null, null, -1, null, false, true) // no-arg constructor required for serialization

    override def init(state: Map[String, String]): Unit = {
      sft = ReducerConfig.sft(state)
      dictionaryFields = StringSerialization.decodeSeq(state(ReducerConfig.DictionariesKey))
      encoding = ReducerConfig.encoding(state)
      ipcOpts = ReducerConfig.ipcOption(state)
      batchSize = ReducerConfig.batch(state)
      sort = ReducerConfig.sort(state)
      sorted = ReducerConfig.sorted(state)
      process = ReducerConfig.process(state)
    }

    override def state: Map[String, String] = Map(
      ReducerConfig.sftName(sft),
      ReducerConfig.sftSpec(sft),
      ReducerConfig.DictionariesKey -> StringSerialization.encodeSeq(dictionaryFields),
      ReducerConfig.encoding(encoding),
      ReducerConfig.flipAxisOrder(encoding),
      ReducerConfig.ipcOption(ipcOpts),
      ReducerConfig.batch(batchSize),
      ReducerConfig.sort(sort),
      ReducerConfig.sorted(sorted),
      ReducerConfig.process(process)
    )

    override def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val files = features.map(_.getAttribute(0).asInstanceOf[Array[Byte]])
      val result = DeltaWriter.reduce(sft, dictionaryFields, encoding, ipcOpts, sort, sorted, batchSize, process, files)
      val sf = resultFeature()
      result.map { bytes => sf.setAttribute(0, bytes); sf }
    }

    def canEqual(other: Any): Boolean = other.isInstanceOf[DeltaReducer]

    override def equals(other: Any): Boolean = other match {
      case that: DeltaReducer if that.canEqual(this) =>
        sft == that.sft && dictionaryFields == that.dictionaryFields && encoding == that.encoding &&
            batchSize == that.batchSize && sort == that.sort && sorted == that.sorted && that.process == process
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(sft, dictionaryFields, encoding, batchSize, sort, process)
      state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
    }
  }

  object ReducerConfig {

    val SftKey           = "sft"
    val SpecKey          = "spec"
    val DictionariesKey  = "dicts"
    val EncodingKey      = "enc"
    val FlipAxisOrderKey = "flip-axis-order"
    val IpcKey           = "ipc"
    val BatchKey         = "batch"
    val SortKey          = "sort"
    val SortedKey        = "sorted"
    val ProcessKey       = "process"

    def sftName(sft: SimpleFeatureType): (String, String) = SftKey -> sft.getTypeName
    def sftSpec(sft: SimpleFeatureType): (String, String) =
      SpecKey -> SimpleFeatureTypes.encodeType(sft, includeUserData = true)

    def sft(options: Map[String, String]): SimpleFeatureType =
      SimpleFeatureTypes.createType(options(SftKey), options(SpecKey))

    def encoding(e: SimpleFeatureEncoding): (String, String) =
      EncodingKey -> s"${e.fids.getOrElse("")}:${e.geometry}:${e.date}"

    def flipAxisOrder(e: SimpleFeatureEncoding): (String, String) =
      FlipAxisOrderKey -> s"${e.flipAxisOrder}"

    def encoding(options: Map[String, String]): SimpleFeatureEncoding = {
      val Array(fids, geom, dtg) = options(EncodingKey).split(":")
      val fidOpt = Option(fids).filterNot(_.isEmpty).map(Encoding.withName)
      val flipAxisOrder = options.get(FlipAxisOrderKey).exists(_.toBoolean)
      SimpleFeatureEncoding(fidOpt, Encoding.withName(geom), Encoding.withName(dtg), flipAxisOrder)
    }

    def ipcOption(options: Map[String, String]): IpcOption = FormatVersion.options(options(IpcKey))
    def ipcOption(ipcOpts: IpcOption): (String, String) = IpcKey -> FormatVersion.version(ipcOpts)

    def batch(b: Int): (String, String) = BatchKey -> b.toString
    def batch(options: Map[String, String]): Int = options(BatchKey).toInt

    def sort(s: Option[(String, Boolean)]): (String, String) =
      SortKey -> s.map { case (f, reverse) => s"$reverse:$f" }.getOrElse("")

    def sort(options: Map[String, String]): Option[(String, Boolean)] = {
      options.get(SortKey).filterNot(_.isEmpty).map { s =>
        val Array(rev, f) = s.split(":")
        (f, rev.toBoolean)
      }
    }

    def sorted(s: Boolean): (String, String) = SortedKey -> s.toString
    def sorted(options: Map[String, String]): Boolean = options.get(SortedKey).exists(_.toBoolean)

    def process(s: Boolean): (String, String) = ProcessKey -> s.toString
    def process(options: Map[String, String]): Boolean = options.get(ProcessKey).forall(_.toBoolean)
  }

  /**
    * Converts arrow-encoded results to features
    *
    * @tparam T result type
    */
  abstract class ArrowResultsToFeatures[T] extends ResultsToFeatures[T] {

    override def init(state: Map[String, String]): Unit = {}

    override def state: Map[String, String] = Map.empty

    override def schema: SimpleFeatureType = ArrowEncodedSft

    override def apply(result: T): SimpleFeature =
      new ScalaSimpleFeature(ArrowEncodedSft, "", Array(bytes(result), GeometryUtils.zeroPoint))

    protected def bytes(result: T): Array[Byte]

    def canEqual(other: Any): Boolean = other.isInstanceOf[ArrowResultsToFeatures[T]]

    override def equals(other: Any): Boolean = other match {
      case that: ArrowResultsToFeatures[T] if that.canEqual(this) => true
      case _ => false
    }

    override def hashCode(): Int = schema.hashCode()
  }

  case class ArrowScanConfig(config: Map[String, String], reduce: FeatureReducer)
}
