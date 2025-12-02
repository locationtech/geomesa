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
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.arrow.io.DeltaWriter
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.api.QueryPlan.ResultsToFeatures.IdentityResultsToFeatures
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.iterators.ArrowScan.DeltaReducer
import org.locationtech.geomesa.index.iterators.StatsScan.StatsReducer
import org.locationtech.geomesa.index.iterators.{ArrowScan, DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.LocalQueryRunner.{LocalProcessor, LocalQueryPlan, LocalScan}
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
import org.locationtech.geomesa.utils.text.StringSerialization
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

  /**
    * Return features for the given schema and filter. Does not need to account for visibility
    *
    * @param sft simple feature type
    * @param filter filter (will not be Filter.INCLUDE), if any
    * @return
    */
  protected def features(sft: SimpleFeatureType, filter: Option[Filter]): CloseableIterator[SimpleFeature]

  override protected def getQueryPlans(sft: SimpleFeatureType, query: Query, explain: Explainer): Seq[QueryPlan] = {
    val processor = LocalProcessor(sft, query.getHints, authProvider)
    val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE)
    val scanner = LocalScan(this, sft, filter)
    val toFeatures = new IdentityResultsToFeatures(sft)
    val projection = query.getHints.getProjection

    Seq(LocalQueryPlan(scanner, processor, toFeatures, projection))
  }
}

object LocalQueryRunner extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  type LocalScanProcessor = CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature]

  /**
   * Set any distributed sort into a regular sort hint, for cases where distributed sorting isn't being processed
   *
   * @param hints hints
   * @param defaultDtg default date field to use for bin sorting, if not specified
   */
  private def setLocalSorting(hints: Hints, defaultDtg: Option[String]): Unit = {
    if (hints.isArrowQuery) {
      hints.getArrowSort.foreach { case (field, reverse) =>
        hints.put(QueryHints.Internal.SORT_FIELDS, StringSerialization.encodeSeq(Seq(field, reverse.toString)))
      }
    } else if (hints.isBinQuery && hints.isBinSorting) {
      hints.getBinDtgField.orElse(defaultDtg).foreach { dtg =>
        hints.put(QueryHints.Internal.SORT_FIELDS, StringSerialization.encodeSeq(Seq(dtg, "false")))
      }
    }
  }

  /**
   * Create the local scan plan
   *
   * @param sft feature type
   * @param hints query hints
   * @return
   */
  private def plan(sft: SimpleFeatureType, hints: Hints): (LocalScanProcessor, Option[FeatureReducer]) = {
    val schema = hints.getTransformSchema.getOrElse(sft)
    if (hints.isArrowQuery) {
      (new ArrowProcessor(schema, hints), Some(new DeltaReducer(schema, hints, sorted = true)))
    } else if (hints.isBinQuery) {
      (new BinProcessor(schema, hints), None)
    } else if (hints.isDensityQuery) {
      (new DensityProcessor(schema, hints), None)
    } else if (hints.isStatsQuery) {
      (new StatsProcessor(schema, hints), Some(new StatsReducer(schema, hints.getStatsQuery, hints.isStatsEncode)))
    } else {
      (CloseableIterator.apply(_, ()), None)
    }
  }

  case class LocalQueryPlan(
      scanner: LocalScan,
      processor: LocalProcessor,
      resultsToFeatures: ResultsToFeatures[SimpleFeature],
      projection: Option[QueryReferenceSystems],
    ) extends LocalProcessorPlan {
    override def scan(): CloseableIterator[SimpleFeature] = processor(scanner())
    override def explain(explainer: Explainer): Unit = {
      explainer.pushLevel("LocalQueryPlan:")
      explainer(s"Filter: ${scanner.filter.fold("none")(FilterHelper.toString)}")
      processor.explain(explainer)
      explainer(s"Reduce: ${reducer.getOrElse("none")}")
      explainer.popLevel()
    }
  }

  /**
   * Plan with a local processing step
   */
  trait LocalProcessorPlan extends QueryPlan {

    override type Results = SimpleFeature

    def processor: LocalProcessor

    override def reducer: Option[FeatureReducer] = processor.reducer
    // handled by local processor
    override def sort: Option[Seq[(String, Boolean)]] = None
    override def maxFeatures: Option[Int] = None
  }

  case class LocalScan(runner: LocalQueryRunner, sft: SimpleFeatureType, filter: Option[Filter])
    extends (() => CloseableIterator[SimpleFeature]) {
    override def apply(): CloseableIterator[SimpleFeature] = runner.features(sft, filter)
  }

  /**
   * Reducer for local transforms. Handles sampling + analytic queries (BIN, arrow, etc). Expects that filtering
   * and transforming have already been applied.
   *
   * Note: creating this transformer will automatically move distributed sort hints into regular sort hints,
   * to ensure that features are sorted *before* being passed to this reducer. Ensure that sort hints
   * are evaluated *after* creating this reducer.
   *
   * @param sft simple feature type being queried
   * @param hints query hints
   */
  class LocalTransformReducer(private var sft: SimpleFeatureType, private var hints: Hints)
      extends FeatureReducer with LazyLogging {

    def this() = this(null, null) // no-arg constructor required for serialization

    if (hints != null) {
      setLocalSorting(hints, Option(sft).flatMap(_.getDtgField))
    }

    override def init(state: Map[String, String]): Unit = {
      sft = SimpleFeatureTypes.createType(state("name"), state("spec"))
      hints = ViewParams.deserialize(state("hint"))
    }

    override def state: Map[String, String] = {
      Map(
        "name" -> sft.getTypeName,
        "spec" -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
        "hint" -> ViewParams.serialize(hints),
      )
    }

    override def apply(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val (processor, reducer) = plan(sft, hints)

      var features = iter
      hints.getSampling.foreach { case (percent, field) =>
        FeatureSampler.sample(percent, field.map(sft.indexOf).filter(_ != -1)).foreach { sampler =>
          features = features.filter(sampler.apply)
        }
      }
      features = processor(features)
      reducer.foreach { reducer =>
        features = reducer(features)
      }
      features
    }

    private def canEqual(other: Any): Boolean = other.isInstanceOf[LocalTransformReducer]

    override def equals(other: Any): Boolean = other match {
      case that: LocalTransformReducer => that.canEqual(this) && sft == that.sft && hints == that.hints
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(sft, hints)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }

  /**
   * Local processor, for datastores that don't support remote filtering/transforms. Handles filtering, transforms,
   * sorting, max features, and analytic processing (may be partially encapsulated in the reducer).
   *
   * Note: creating this processor will automatically move distributed sort hints into regular sort hints,
   * to ensure that features are sorted *before* being passed to any reducer. Ensure that sort hints
   * are evaluated *after* creating this processor.
   *
   * @param sft feature type
   * @param hints query hints
   * @param authProvider auth provider
   */
  case class LocalProcessor(sft: SimpleFeatureType, hints: Hints, authProvider: Option[AuthorizationsProvider])
      extends LocalScanProcessor {

    setLocalSorting(hints, sft.getDtgField)

    private val filterFunction: SimpleFeature => Boolean = {
      val visible = VisibilityUtils.visible(authProvider)
      val sampler = hints.getSampling.flatMap { case (percent, field) =>
        FeatureSampler.sample(percent, field.map(sft.indexOf).filter(_ != -1))
      }
      // note: make sure sampling is checked after other filtering so it's correct
      sampler match {
        case None => visible
        case Some(sample) => f => visible(f) && sample(f)
      }
    }

    private val (processor, _reducer) = plan(sft, hints)

    val reducer: Option[FeatureReducer] = _reducer

    override def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val filtered = features.filter(filterFunction.apply)
      val transformed = hints.getTransform.fold(filtered) {  case (tdefs, tsft) =>
        // note: we do the transform here and not in the query plan, as the processor step expects already transformed features
        val transform = TransformSimpleFeature(sft, tsft, tdefs)
        filtered.map(f => ScalaSimpleFeature.copy(transform.setFeature(f)))
      }
      // handle sorting + max features here before any encoding complicates things
      val sorted = hints.getSortFields.fold(transformed)(s => new SortingSimpleFeatureIterator(transformed, s))
      val limited = hints.getMaxFeatures.fold(sorted)(m => sorted.take(m))
      processor(limited)
    }

    def explain(explainer: Explainer): Unit = {
      explainer(s"Transform: ${hints.getTransform.fold("none")(t => s"${t._1} ${SimpleFeatureTypes.encodeType(t._2)}")}")
      explainer(s"Sort: ${hints.getSortFields.fold("none")(_.mkString(", "))}")
      explainer(s"Max Features: ${hints.getMaxFeatures.getOrElse("none")}")
    }
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
   */
  class StatsProcessor(sft: SimpleFeatureType, query: String) extends LocalScanProcessor {

    def this(sft: SimpleFeatureType, hints: Hints) = this(sft, hints.getStatsQuery)

    override def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val stat = Stat(sft, query)
      try { features.foreach(stat.observe) } finally { features.close() }
      val encoded = StatsScan.encodeStat(sft)(stat)
      val sf = new ScalaSimpleFeature(StatsScan.StatsSft, "stat", Array(encoded, GeometryUtils.zeroPoint))
      CloseableIterator.single(sf)
    }
  }
}
