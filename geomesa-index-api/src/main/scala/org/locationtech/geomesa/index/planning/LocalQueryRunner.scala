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
    val scanner = LocalScan(this, sft, Option(query.getFilter).filter(_ != Filter.INCLUDE))
    val processor = LocalProcessor(sft, query.getHints, authProvider)
    Seq(LocalQueryPlan(scanner, processor, query.getHints.getProjection))
  }
}

object LocalQueryRunner extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  case class LocalQueryPlan(
      scanner: LocalScan,
      processor: LocalProcessor,
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

    override val resultsToFeatures: ResultsToFeatures[SimpleFeature] =
      new IdentityResultsToFeatures(processor.hints.getReturnSft)

    override def reducer: Option[FeatureReducer] = processor.reducer
    // handled by local processor
    override def sort: Option[Seq[(String, Boolean)]] = None
    override def maxFeatures: Option[Int] = None
  }

  /**
   * Local scan
   *
   * @param runner runner
   * @param sft feature type
   * @param filter filter
   */
  case class LocalScan(runner: LocalQueryRunner, sft: SimpleFeatureType, filter: Option[Filter])
      extends (() => CloseableIterator[SimpleFeature]) {
    override def apply(): CloseableIterator[SimpleFeature] = runner.features(sft, filter)
  }

  /**
   * Local scan processor trait
   */
  trait LocalScanProcessor extends (CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature]) {

    /**
     * Reducer step for this processor
     *
     * @return
     */
    def reducer: Option[FeatureReducer]
  }

  /**
   * Local processor, for datastores that don't support remote filtering/transforms. Handles filtering, transforms,
   * sorting, max features, and analytic processing (may be partially encapsulated in the reducer).
   *
   * Note: will not perform any auth filtering if the authProvider is not defined
   *
   * @param sft feature type
   * @param hints query hints
   * @param authProvider auth provider, if auth filtering is desired
   */
  case class LocalProcessor(sft: SimpleFeatureType, hints: Hints, authProvider: Option[AuthorizationsProvider])
      extends LocalScanProcessor {

    private val filterFunction: Option[SimpleFeature => Boolean] = {
      val visible = authProvider.map(VisibilityUtils.visible)
      val sampler = hints.getSampling.flatMap { case (percent, field) =>
        FeatureSampler.sample(percent, field.map(sft.indexOf).filter(_ != -1))
      }
      (visible, sampler) match {
        case (None, None) => None
        case (Some(v), None) => Some(f => v(f))
        case (None, Some(s)) => Some(f => s(f))
        case (Some(v), Some(s)) => Some(f => v(f) && s(f)) // note: make sure sampling is checked after filtering so it's correct
      }
    }

    // pull out distributed sort hints into regular sort hints that we process here
    private val sort = hints.getSortFields.orElse {
      if (hints.isArrowQuery) {
        hints.getArrowSort.map { case (field, reverse) => Seq(field -> reverse) }
      } else if (hints.isBinQuery && hints.isBinSorting) {
        hints.getBinDtgField.orElse(sft.getDtgField).map(dtg => Seq(dtg -> false))
      } else {
        None
      }
    }

    private val processor = {
      val inputSchema = hints.getTransformSchema.getOrElse(sft)
      if (hints.isArrowQuery) {
        Some(new ArrowProcessor(inputSchema, hints))
      } else if (hints.isBinQuery) {
        Some(new BinProcessor(inputSchema, hints))
      } else if (hints.isDensityQuery) {
        Some(new DensityProcessor(inputSchema, hints))
      } else if (hints.isStatsQuery) {
        Some(new StatsProcessor(inputSchema, hints))
      } else {
        None
      }
    }

    override val reducer: Option[FeatureReducer] = processor.flatMap(_.reducer)

    override def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val filtered = filterFunction.fold(features)(f => features.filter(f.apply))
      val transformed = hints.getTransform.fold(filtered) {  case (tdefs, tsft) =>
        // note: we do the transform here and not in the query plan, as the processor step expects already transformed features
        val transform = TransformSimpleFeature(sft, tsft, tdefs)
        filtered.map(f => ScalaSimpleFeature.copy(transform.setFeature(f)))
      }
      // handle sorting + max features here before any encoding complicates things
      val sorted = sort.fold(transformed)(s => new SortingSimpleFeatureIterator(transformed, s))
      val limited = hints.getMaxFeatures.fold(sorted)(m => sorted.take(m))
      processor.fold(limited)(_.apply(limited))
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
      dictionaryFields: Seq[String],
      encoding: SimpleFeatureEncoding,
      batchSize: Int,
      ipcOpts: IpcOption,
      sort: Option[(String, Boolean)],
      processDeltas: Boolean,
    ) extends LocalScanProcessor {

    def this(sft: SimpleFeatureType, hints: Hints) =
      this(sft, hints.getArrowDictionaryFields, ArrowScan.getEncoding(hints), ArrowScan.getBatchSize(hints),
        ArrowScan.getIpcOpts(hints), hints.getSortFields.map(_.head).orElse(hints.getArrowSort), hints.isArrowProcessDeltas)

    override val reducer: Option[FeatureReducer] =
      Some(new DeltaReducer(sft, dictionaryFields, encoding, ipcOpts, batchSize, sort, sorted = true, processDeltas))

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

    override def reducer: Option[FeatureReducer] = None

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

    override def reducer: Option[FeatureReducer] = None

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
  class StatsProcessor(sft: SimpleFeatureType, query: String, encode: Boolean) extends LocalScanProcessor {

    def this(sft: SimpleFeatureType, hints: Hints) = this(sft, hints.getStatsQuery, hints.isStatsEncode)

    override val reducer: Option[FeatureReducer] = Some(new StatsReducer(sft, query, encode))

    override def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      val stat = Stat(sft, query)
      try { features.foreach(stat.observe) } finally { features.close() }
      val encoded = StatsScan.encodeStat(sft)(stat)
      val sf = new ScalaSimpleFeature(StatsScan.StatsSft, "stat", Array(encoded, GeometryUtils.zeroPoint))
      CloseableIterator.single(sf)
    }
  }
}
