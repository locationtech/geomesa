/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector.ipc.message.IpcOption
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.arrow.io.{DeltaWriter, FormatVersion}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.api.QueryPlan.ResultsToFeatures.IdentityResultsToFeatures
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.iterators.ArrowScan.DeltaReducer
import org.locationtech.geomesa.index.iterators.{ArrowScan, DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.LocalQueryRunner.LocalQueryPlan
import org.locationtech.geomesa.index.stats.Stat
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.{Explainer, FeatureSampler, SortingSimpleFeatureIterator}
import org.locationtech.geomesa.security.{AuthorizationsProvider, VisibilityUtils}
import org.locationtech.geomesa.utils.bin.BinaryEncodeCallback.ByteStreamCallback
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, RenderingGrid, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.jts.geom.Envelope

import java.io.ByteArrayOutputStream

/**
  * Query runner that handles transforms, visibilities and analytic queries locally. Subclasses are responsible
  * for implementing basic filtering.
  *
  * @param authProvider auth provider
  */
abstract class LocalQueryRunner(authProvider: Option[AuthorizationsProvider])
    extends QueryRunner {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  /**
    * Return features for the given schema and filter. Does not need to account for visibility
    *
    * @param sft simple feature type
    * @param filter filter (will not be Filter.INCLUDE), if any
    * @return
    */
  protected def features(sft: SimpleFeatureType, filter: Option[Filter]): Iterator[SimpleFeature]

  override protected def getQueryPlans(sft: SimpleFeatureType, query: Query, explain: Explainer): Seq[QueryPlan] = {
    val hints = query.getHints
    val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE)

    val filterFunction: SimpleFeature => Boolean = {
      val visible = VisibilityUtils.visible(authProvider)
      val sampler = hints.getSampling.flatMap { case (percent, field) =>
        FeatureSampler.sample(percent, field.map(sft.indexOf).filter(_ != -1))
      }
      sampler match {
        case None => visible
        case Some(s) => f => visible(f) && s(f) // note: make sure visibility is checked first so sampling is correct
      }
    }

    val plan = LocalQueryRunner.plan(sft, hints)

    val scanner = () => plan.processor(features(sft, filter).filter(filterFunction.apply))

    val maxFeatures = hints.getMaxFeatures
    val projection = hints.getProjection

    val sort = hints.getSortFields.orElse {
      if (hints.isBinQuery && hints.isBinSorting) {
        hints.getBinDtgField.orElse(sft.getDtgField).map(dtg => Seq(dtg -> false))
      } else if (hints.isArrowQuery) {
        hints.getArrowSort.map(Seq(_))
      } else {
        None
      }
    }

    Seq(LocalQueryPlan(scanner, plan.toFeatures, plan.reducer, sort, maxFeatures, projection))
  }
}

object LocalQueryRunner extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  type LocalScanProcessor = CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature]

  /**
   * Separates out the parts of a scan that normally happen remotely (the processor) with the parts that happen
   * locally (the reducer), which are sometimes skipped when we're aggregating multiple scans together
   *
   * @param toFeatures resultsToFeatures
   * @param processor "remote" processing
   * @param reducer reducer
   */
  private case class LocalQueryScan(
    toFeatures: ResultsToFeatures[SimpleFeature],
    processor: LocalScanProcessor,
    reducer: Option[FeatureReducer]
  )

  case class LocalQueryPlan(
      scanner: () => CloseableIterator[SimpleFeature],
      resultsToFeatures: ResultsToFeatures[SimpleFeature],
      reducer: Option[FeatureReducer],
      sort: Option[Seq[(String, Boolean)]],
      maxFeatures: Option[Int],
      projection: Option[QueryReferenceSystems],
    ) extends QueryPlan {
    override type Results = SimpleFeature
    override def scan(): CloseableIterator[SimpleFeature] = scanner()
    override def explain(explainer: Explainer, prefix: String = ""): Unit = explainer(s"${prefix}LocalQuery")
  }

  /**
   * TODO make sure this is still doing what we expect in e.g. AccumuloIndexAdapter
   *
    * Reducer for local transforms. Handles ecql and visibility filtering, transforms and analytic queries.
    *
    * @param sft simple feature type being queried
    * @param hints query hints
    * @return
    */
  class LocalTransformReducer(
      private var sft: SimpleFeatureType,
      private var filter: Option[Filter],
      private var visibility: Option[SimpleFeature => Boolean],
      private var transform: Option[(String, SimpleFeatureType)],
      private var hints: Hints
    ) extends FeatureReducer with LazyLogging {

    def this() = this(null, null, None, None, null) // no-arg constructor required for serialization

    override def init(state: Map[String, String]): Unit = {
      sft = SimpleFeatureTypes.createType(state("name"), state("spec"))
      filter = state.get("filt").filterNot(_.isEmpty).map(ECQL.toFilter)
      hints = ViewParams.deserialize(state("hint"))
      transform = for {
        tdef <- state.get("tdef").filterNot(_.isEmpty)
        tnam <- state.get("tnam").filterNot(_.isEmpty)
        spec <- state.get("tsft").filterNot(_.isEmpty)
      } yield {
        (tdef, SimpleFeatureTypes.createType(tnam, spec))
      }
    }

    override def state: Map[String, String] = {
      if (visibility.isDefined) {
        throw new UnsupportedOperationException("Visibility filtering is not serializable")
      }
      Map(
        "name" -> sft.getTypeName,
        "spec" -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
        "filt" -> filter.map(ECQL.toCQL).getOrElse(""),
        "tdef" -> transform.map(_._1).getOrElse(""),
        "tnam" -> transform.map(_._2.getTypeName).getOrElse(""),
        "tsft" -> transform.map(t => SimpleFeatureTypes.encodeType(t._2, includeUserData = true)).getOrElse(""),
        "hint" -> ViewParams.serialize(hints)
      )
    }

    override def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val filtered = (filter, visibility) match {
        case (None, None)       => features
        case (Some(f), None)    => features.filter(f.evaluate)
        case (None, Some(v))    => features.filter(v.apply)
        case (Some(f), Some(v)) => features.filter(feature => v(feature) && f.evaluate(feature))
      }
      LocalQueryRunner.transform(sft, filtered, transform, hints)
    }
  }

  /**
   * Create the local scan plan
   *
   * @param sft feature type
   * @param hints query hints
   * @return
   */
  private def plan(sft: SimpleFeatureType, hints: Hints): LocalQueryScan = {
    val toFeatures = hints.getTransform match {
      case None => new IdentityResultsToFeatures(sft)
      case Some((defs, tsft)) => new TransformFeatures(sft, tsft, defs)
    }
    if (hints.isArrowQuery) {
      val processor = new ArrowProcessor(toFeatures.schema, hints)
      val reducer = new DeltaReducer(toFeatures.schema, hints, sorted = true)
      LocalQueryScan(toFeatures, processor, Some(reducer))
    } else if (hints.isBinQuery) {
      LocalQueryScan(toFeatures, new BinProcessor(toFeatures.schema, hints), None)
    } else if (hints.isDensityQuery) {
      LocalQueryScan(toFeatures, new DensityProcessor(toFeatures.schema, hints), None)
    } else if (hints.isStatsQuery) {
      LocalQueryScan(toFeatures, new StatsProcessor(toFeatures.schema, hints), None)
    } else {
      LocalQueryScan(toFeatures, CloseableIterator.apply(_, Unit), None)
    }
  }

  /**
   * Applies a transform to the local features
   *
   * @param sft feature type
   * @param transform transform feature type
   * @param definitions transform definition string
   */
  private class TransformFeatures(sft: SimpleFeatureType, transform: SimpleFeatureType, definitions: String)
      extends ResultsToFeatures[SimpleFeature] {

    def this() = this(null, null, null)

    private val transformSf = TransformSimpleFeature(sft, transform, definitions)

    override def schema: SimpleFeatureType = transform
    override def apply(result: SimpleFeature): SimpleFeature = ScalaSimpleFeature.copy(transformSf.setFeature(result))
    override def init(state: Map[String, String]): Unit = throw new UnsupportedOperationException()
    override def state: Map[String, String] = throw new UnsupportedOperationException()
  }

  /**
   * Processor for local arrow transforms
   *
   * @param sft feature type
   * @param batchSize arrow batch size
   * @param encoding arrow encoding options
   * @param ipcOpts arrow ipc format options
   * @param dictionaryFields list of fields to dictionary encode
   */
  class ArrowProcessor(
      sft: SimpleFeatureType,
      batchSize: Int,
      encoding: SimpleFeatureEncoding,
      ipcOpts: IpcOption,
      dictionaryFields: Seq[String],
    ) extends LocalScanProcessor {

    def this(sft: SimpleFeatureType, hints: Hints) =
      this(sft, ArrowScan.getBatchSize(hints), ArrowScan.getEncoding(hints), ArrowScan.getIpcOpts(hints), hints.getArrowDictionaryFields)

    override def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val writer = new DeltaWriter(sft, dictionaryFields, encoding, ipcOpts, None, batchSize)
      val array = Array.ofDim[SimpleFeature](batchSize)

      val sf = ArrowScan.resultFeature()

      val arrows = features.grouped(batchSize).map { group =>
        var index = 0
        group.foreach { sf =>
          array(index) = sf
          index += 1
        }
        sf.setAttribute(0, writer.encode(array, index))
        sf
      }
      CloseableIterator(arrows, CloseWithLogging(Seq(writer, features)))
    }
  }

  /**
   * Processor for local BIN transforms
   *
   * @param sft simple feature type being queried
   * @param trackId bin track id
   * @param geom bin geom field
   * @param dtg bin dtg field
   * @param label bin label field
   */
  class BinProcessor(
      sft: SimpleFeatureType,
      trackId: Option[Int],
      geom: Option[Int],
      dtg: Option[Int],
      label: Option[Int],
    ) extends LocalScanProcessor {

    def this(sft: SimpleFeatureType, hints: Hints) =
      this(sft, Option(hints.getBinTrackIdField).filter(_ != "id").map(sft.indexOf), hints.getBinGeomField.map(sft.indexOf),
        hints.getBinDtgField.map(sft.indexOf), hints.getBinLabelField.map(sft.indexOf))

    override def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val encoder = BinaryOutputEncoder(sft, EncodingOptions(geom, dtg, trackId, label))
      val os = new ByteArrayOutputStream(1024)
      val callback = new ByteStreamCallback(os)

      val bins = features.grouped(64).map { group =>
        os.reset()
        group.foreach(encoder.encode(_, callback))
        new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, "", Array(os.toByteArray, GeometryUtils.zeroPoint))
      }
      CloseableIterator(bins, features.close())
    }
  }

  /**
   * Local processor for density transforms
   *
   * @param sft feature type
   * @param envelope rendering envelope
   * @param width number of pixels in width for the rendered grid
   * @param height number of pixels in height for the rendered grid
   * @param geom geometry attribute
   * @param weight weight attribute
   */
  class DensityProcessor(
      sft: SimpleFeatureType,
      envelope: Envelope,
      width: Int,
      height: Int,
      geom: String,
      weight: Option[String],
    ) extends LocalScanProcessor {

    def this(sft: SimpleFeatureType, hints: Hints) =
      this(sft, hints.getDensityEnvelope.get, hints.getDensityBounds.get._1, hints.getDensityBounds.get._2,
        DensityScan.getDensityGeometry(sft, hints), hints.getDensityWeight)

    override def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val renderer = DensityScan.getRenderer(sft, geom, weight)
      val grid = new RenderingGrid(envelope, width, height)
      try { features.foreach(renderer.render(grid, _)) } finally { features.close() }

      val sf = new ScalaSimpleFeature(DensityScan.DensitySft, "", Array(GeometryUtils.zeroPoint))
      // return value in user data so it's preserved when passed through a RetypingFeatureCollection
      sf.getUserData.put(DensityScan.DensityValueKey, DensityScan.encodeResult(grid))
      CloseableIterator.single(sf)
    }
  }

  /**
   * Processor for local stats transforms
   *
   * @param sft feature type
   * @param query stats query
   * @param encode encode results as binary, or otherwise return json
   */
  class StatsProcessor(sft: SimpleFeatureType, query: String, encode: Boolean) extends LocalScanProcessor {

    def this(sft: SimpleFeatureType, hints: Hints) = this(sft, hints.getStatsQuery, hints.isStatsEncode || hints.isSkipReduce)

    override def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val stat = Stat(sft, query)
      try { features.foreach(stat.observe) } finally { features.close() }
      val encoded = if (encode) { StatsScan.encodeStat(sft)(stat) } else { stat.toJson }
      val sf = new ScalaSimpleFeature(StatsScan.StatsSft, "stat", Array(encoded, GeometryUtils.zeroPoint))
      CloseableIterator.single(sf)
    }
  }

  /**
    * Transform plain features into the appropriate return type, based on the hints
    *
    * @param sft simple feature type being queried
    * @param features plain, untransformed features matching the simple feature type
    * @param hints query hints
    * @return
    */
  def transform(
      sft: SimpleFeatureType,
      features: CloseableIterator[SimpleFeature],
      transform: Option[(String, SimpleFeatureType)],
      hints: Hints): CloseableIterator[SimpleFeature] = {
    val sampled = hints.getSampling match {
      case None => features
      case Some((percent, field)) => sample(sft, percent, field)(features)
    }
    if (hints.isBinQuery) {
      val trackId = Option(hints.getBinTrackIdField).filter(_ != "id").map(sft.indexOf)
      val geom = hints.getBinGeomField.map(sft.indexOf)
      val dtg = hints.getBinDtgField.map(sft.indexOf)
      val sorting = hints.isBinSorting
      binTransform(sampled, sft, trackId, geom, dtg, hints.getBinLabelField.map(sft.indexOf), sorting)
    } else if (hints.isArrowQuery) {
      arrowTransform(sampled, sft, transform, hints)
    } else if (hints.isDensityQuery) {
      val Some(envelope) = hints.getDensityEnvelope
      val Some((width, height)) = hints.getDensityBounds
      val geom = DensityScan.getDensityGeometry(sft, hints)
      densityTransform(sampled, sft, geom, envelope, width, height, hints.getDensityWeight)
    } else if (hints.isStatsQuery) {
      statsTransform(sampled, sft, transform, hints.getStatsQuery, hints.isStatsEncode || hints.isSkipReduce)
    } else {
      transform match {
        case None => noTransform(sampled, hints.getSortFields)
        case Some((defs, tsft)) => projectionTransform(sampled, sft, tsft, defs, hints.getSortFields)
      }
    }
  }

  private def binTransform(
      features: CloseableIterator[SimpleFeature],
      sft: SimpleFeatureType,
      trackId: Option[Int],
      geom: Option[Int],
      dtg: Option[Int],
      label: Option[Int],
      sorting: Boolean): CloseableIterator[SimpleFeature] = {
    val encoder = BinaryOutputEncoder(sft, EncodingOptions(geom, dtg, trackId, label))
    val sorted = if (!sorting) { features } else {
      val i = dtg.orElse(sft.getDtgIndex).getOrElse(throw new IllegalArgumentException("Can't sort BIN features by date"))
      new SortingSimpleFeatureIterator(features, Seq(sft.getDescriptor(i).getLocalName -> false))
    }

    val os = new ByteArrayOutputStream(1024)
    val callback = new ByteStreamCallback(os)

    new CloseableIterator[SimpleFeature] {
      override def hasNext: Boolean = sorted.hasNext
      override def next(): SimpleFeature = {
        os.reset()
        sorted.take(64).foreach(encoder.encode(_, callback))
        new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, "", Array(os.toByteArray, GeometryUtils.zeroPoint))
      }
      override def close(): Unit = sorted.close()
    }
  }

  private def arrowTransform(
      original: CloseableIterator[SimpleFeature],
      sft: SimpleFeatureType,
      transform: Option[(String, SimpleFeatureType)],
      hints: Hints): CloseableIterator[SimpleFeature] = {

    val sort = hints.getArrowSort.map(Seq.fill(1)(_))
    val batchSize = ArrowScan.getBatchSize(hints)
    val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid, hints.isFlipAxisOrder)
    val ipcOpts = FormatVersion.options(hints.getArrowFormatVersion.getOrElse(FormatVersion.ArrowFormatVersion.get))

    val (features, arrowSft) = transform match {
      case None => (noTransform(original, sort), sft)
      case Some((definitions, tsft)) => (projectionTransform(original, sft, tsft, definitions, sort), tsft)
    }

    val dictionaryFields = hints.getArrowDictionaryFields
    val writer = new DeltaWriter(arrowSft, dictionaryFields, encoding, ipcOpts, None, batchSize)
    val array = Array.ofDim[SimpleFeature](batchSize)

    val sf = ArrowScan.resultFeature()

    val arrows = new CloseableIterator[SimpleFeature] {
      override def hasNext: Boolean = features.hasNext
      override def next(): SimpleFeature = {
        var index = 0
        while (index < batchSize && features.hasNext) {
          array(index) = features.next
          index += 1
        }
        sf.setAttribute(0, writer.encode(array, index))
        sf
      }
      override def close(): Unit = CloseWithLogging(Seq(features, writer))
    }
    if (hints.isSkipReduce) { arrows } else {
      val process = hints.isArrowProcessDeltas
      new ArrowScan.DeltaReducer(arrowSft, dictionaryFields, encoding, ipcOpts, batchSize, sort.map(_.head), sorted = true, process)(arrows)
    }
  }

  private def densityTransform(
      features: CloseableIterator[SimpleFeature],
      sft: SimpleFeatureType,
      geom: String,
      envelope: Envelope,
      width: Int,
      height: Int,
      weight: Option[String]): CloseableIterator[SimpleFeature] = {
    val renderer = DensityScan.getRenderer(sft, geom, weight)
    val grid = new RenderingGrid(envelope, width, height)
    try { features.foreach(renderer.render(grid, _)) } finally { features.close() }

    val sf = new ScalaSimpleFeature(DensityScan.DensitySft, "", Array(GeometryUtils.zeroPoint))
    // Return value in user data so it's preserved when passed through a RetypingFeatureCollection
    sf.getUserData.put(DensityScan.DensityValueKey, DensityScan.encodeResult(grid))
    CloseableIterator(Iterator(sf))
  }

  private def statsTransform(features: CloseableIterator[SimpleFeature],
                             sft: SimpleFeatureType,
                             transform: Option[(String, SimpleFeatureType)],
                             query: String,
                             encode: Boolean): CloseableIterator[SimpleFeature] = {
    val (statSft, toObserve) = transform match {
      case None                => (sft, features)
      case Some((tdefs, tsft)) => (tsft, projectionTransform(features, sft, tsft, tdefs, None))
    }
    val stat = Stat(statSft, query)
    try { toObserve.foreach(stat.observe) } finally { toObserve.close() }
    val encoded = if (encode) { StatsScan.encodeStat(statSft)(stat) } else { stat.toJson }
    CloseableIterator(Iterator(new ScalaSimpleFeature(StatsScan.StatsSft, "stat", Array(encoded, GeometryUtils.zeroPoint))))
  }

  private def projectionTransform(
      features: CloseableIterator[SimpleFeature],
      sft: SimpleFeatureType,
      transform: SimpleFeatureType,
      definitions: String,
      sort: Option[Seq[(String, Boolean)]]): CloseableIterator[SimpleFeature] = {
    val transformSf = TransformSimpleFeature(sft, transform, definitions)

    def setValues(from: SimpleFeature, to: ScalaSimpleFeature): ScalaSimpleFeature = {
      transformSf.setFeature(from)
      var i = 0
      while (i < transform.getAttributeCount) {
        to.setAttributeNoConvert(i, transformSf.getAttribute(i))
        i += 1
      }
      to.setId(from.getID)
      to
    }

    val result = features.map(setValues(_, new ScalaSimpleFeature(transform, "")))

    sort match {
      case None    => result
      case Some(s) => new SortingSimpleFeatureIterator(result, s)
    }
  }

  private def noTransform(
      features: CloseableIterator[SimpleFeature],
      sort: Option[Seq[(String, Boolean)]]): CloseableIterator[SimpleFeature] = {
    sort match {
      case None    => features
      case Some(s) => new SortingSimpleFeatureIterator(features, s)
    }
  }

  /**
    * Sample the features by selecting a subset
    *
    * @param sft simple feature type
    * @param percent percent of features to keep
    * @param by field to group by for sampling
    * @param features features to sample
    * @return
    */
  private def sample(sft: SimpleFeatureType, percent: Float, by: Option[String])
                    (features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
    if (!(percent > 0 && percent < 1f)) {
      throw new IllegalArgumentException(s"Sampling must be a percentage between (0, 1): $percent")
    }
    val nth = (1 / percent.toFloat).toInt
    val field = by.map { name =>
      val i = sft.indexOf(name)
      if (i == -1) {
        throw new IllegalArgumentException(s"Invalid sampling field '$name' for schema " +
            s"${sft.getTypeName} ${SimpleFeatureTypes.encodeType(sft)}")
      }
      i
    }

    if (nth <= 1) { features } else {
      val sample = FeatureSampler.sample(nth, field)
      features.filter(sample.apply)
    }
  }
}
