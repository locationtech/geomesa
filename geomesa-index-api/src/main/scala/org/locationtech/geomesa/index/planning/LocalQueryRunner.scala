/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.arrow.io.{DeltaWriter, FormatVersion}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.index.api.QueryPlan.FeatureReducer
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.iterators.{ArrowScan, DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.QueryRunner.QueryResult
import org.locationtech.geomesa.index.utils.{Explainer, FeatureSampler, Reprojection, SortingSimpleFeatureIterator}
import org.locationtech.geomesa.security.{AuthorizationsProvider, VisibilityUtils}
import org.locationtech.geomesa.utils.bin.BinaryEncodeCallback.ByteStreamCallback
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, RenderingGrid, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.stats.Stat
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

  import LocalQueryRunner.transform
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  protected def name: String

  /**
    * Return features for the given schema and filter. Does not need to account for visibility
    *
    * @param sft simple feature type
    * @param filter filter (will not be Filter.INCLUDE), if any
    * @return
    */
  protected def features(sft: SimpleFeatureType, filter: Option[Filter]): CloseableIterator[SimpleFeature]

  override def runQuery(sft: SimpleFeatureType, original: Query, explain: Explainer): QueryResult = {
    val query = configureQuery(sft, original)
    QueryResult(query.getHints.getReturnSft, query.getHints, run(sft, query, explain))
  }

  private def run(sft: SimpleFeatureType, query: Query, explain: Explainer)(): CloseableIterator[SimpleFeature] = {
    explain.pushLevel(s"$name query: '${sft.getTypeName}' ${org.locationtech.geomesa.filter.filterToString(query.getFilter)}")
    explain(s"bin[${query.getHints.isBinQuery}] arrow[${query.getHints.isArrowQuery}] " +
        s"density[${query.getHints.isDensityQuery}] stats[${query.getHints.isStatsQuery}] " +
        s"sampling[${query.getHints.getSampling.map { case (s, f) => s"$s${f.map(":" + _).getOrElse("")}"}.getOrElse("none")}]")
    explain(s"Transforms: ${query.getHints.getTransformDefinition.getOrElse("None")}")
    explain(s"Sort: ${query.getHints.getSortFields.map(QueryHints.sortReadableString).getOrElse("none")}")
    explain.popLevel()

    val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE)
    val visible = VisibilityUtils.visible(authProvider)
    val iter = features(sft, filter).filter(visible.apply)

    var result = transform(sft, iter, query.getHints.getTransform, query.getHints)

    query.getHints.getMaxFeatures.foreach { maxFeatures =>
      if (query.getHints.getReturnSft == BinaryOutputEncoder.BinEncodedSft) {
        // bin queries pack multiple records into each feature
        // to count the records, we have to count the total bytes coming back, instead of the number of features
        val label = query.getHints.getBinLabelField.isDefined
        result = new BinaryOutputEncoder.FeatureLimitingIterator(result, maxFeatures, label)
      } else {
        result = result.take(maxFeatures)
      }
    }

    query.getHints.getProjection.foreach { crs =>
      val r = Reprojection(query.getHints.getReturnSft, crs)
      result = result.map(r.apply)
    }

    result
  }

  override protected [geomesa] def getReturnSft(sft: SimpleFeatureType, hints: Hints): SimpleFeatureType = {
    if (hints.isBinQuery) {
      BinaryOutputEncoder.BinEncodedSft
    } else if (hints.isArrowQuery) {
      org.locationtech.geomesa.arrow.ArrowEncodedSft
    } else if (hints.isDensityQuery) {
      DensityScan.DensitySft
    } else if (hints.isStatsQuery) {
      StatsScan.StatsSft
    } else {
      super.getReturnSft(sft, hints)
    }
  }
}

object LocalQueryRunner extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  /**
    * Filter to checking visibilities
    *
    * @param provider auth provider, if any
    * @return
    */
  @deprecated("Replaced with `VisibilityUtils.visible`")
  def visible(provider: Option[AuthorizationsProvider]): SimpleFeature => Boolean = VisibilityUtils.visible(provider)

  /**
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
