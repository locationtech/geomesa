/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.Date

import org.geotools.data.Query
import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.io.{DeltaWriter, DictionaryBuildingWriter}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.index.iterators.{ArrowScan, DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.LocalQueryRunner.ArrowDictionaryHook
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.{Explainer, FeatureSampler, Reprojection}
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityUtils, VisibilityEvaluator}
import org.locationtech.geomesa.utils.bin.BinaryEncodeCallback.ByteStreamCallback
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap, SimpleFeatureOrdering, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats.{Stat, TopK}
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Query runner that handles transforms, visibilities and analytic queries locally. Subclasses are responsible
  * for implementing basic filtering.
  *
  * @param stats stats
  * @param authProvider auth provider
  */
abstract class LocalQueryRunner(stats: GeoMesaStats, authProvider: Option[AuthorizationsProvider])
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

  override def runQuery(sft: SimpleFeatureType, original: Query, explain: Explainer): CloseableIterator[SimpleFeature] = {
    val query = configureQuery(sft, original)

    explain.pushLevel(s"$name query: '${sft.getTypeName}' ${org.locationtech.geomesa.filter.filterToString(query.getFilter)}")
    explain(s"bin[${query.getHints.isBinQuery}] arrow[${query.getHints.isArrowQuery}] " +
        s"density[${query.getHints.isDensityQuery}] stats[${query.getHints.isStatsQuery}] " +
        s"sampling[${query.getHints.getSampling.map { case (s, f) => s"$s${f.map(":" + _).getOrElse("")}"}.getOrElse("none")}]")
    explain(s"Transforms: ${query.getHints.getTransformDefinition.getOrElse("None")}")
    explain(s"Sort: ${query.getHints.getSortReadableString}")
    explain.popLevel()

    val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE)
    val visible = LocalQueryRunner.visible(authProvider)
    val iter = features(sft, filter).filter(visible.apply)

    val hook = Some(ArrowDictionaryHook(stats, filter))
    val result = transform(sft, iter, query.getHints.getTransform, query.getHints, hook)

    Reprojection(query) match {
      case None    => result
      case Some(r) => result.map(r.reproject)
    }
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

object LocalQueryRunner {
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  import scala.collection.JavaConversions._

  case class ArrowDictionaryHook(stats: GeoMesaStats, filter: Option[Filter])

  /**
    * Filter to checking visibilities
    *
    * @param provider auth provider, if any
    * @return
    */
  def visible(provider: Option[AuthorizationsProvider]): SimpleFeature => Boolean = {
    provider match {
      case None    => noAuthVisibilityCheck
      case Some(p) => authVisibilityCheck(_, p.getAuthorizations.map(_.getBytes(StandardCharsets.UTF_8)))
    }
  }

  /**
    * Transform plain features into the appropriate return type, based on the hints
    *
    * @param sft simple feature type being queried
    * @param features plain, untransformed features matching the simple feature type
    * @param hints query hints
    * @param arrow stats hook and cql filter - used for dictionary building in certain arrow queries
    * @return
    */
  def transform(sft: SimpleFeatureType,
                features: CloseableIterator[SimpleFeature],
                transform: Option[(String, SimpleFeatureType)],
                hints: Hints,
                arrow: Option[ArrowDictionaryHook] = None): CloseableIterator[SimpleFeature] = {
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
      arrowTransform(sampled, sft, transform, hints, arrow)
    } else if (hints.isDensityQuery) {
      val Some(envelope) = hints.getDensityEnvelope
      val Some((width, height)) = hints.getDensityBounds
      densityTransform(sampled, sft, envelope, width, height, hints.getDensityWeight)
    } else if (hints.isStatsQuery) {
      statsTransform(sampled, sft, transform, hints.getStatsQuery, hints.isStatsEncode || hints.isSkipReduce)
    } else {
      transform match {
        case None =>
          val sort = hints.getSortFields.map(SimpleFeatureOrdering(sft, _))
          noTransform(sft, sampled, sort)
        case Some((defs, tsft)) =>
          val sort = hints.getSortFields.map(SimpleFeatureOrdering(tsft, _))
          projectionTransform(sampled, sft, tsft, defs, sort)
      }
    }
  }

  private def binTransform(features: CloseableIterator[SimpleFeature],
                           sft: SimpleFeatureType,
                           trackId: Option[Int],
                           geom: Option[Int],
                           dtg: Option[Int],
                           label: Option[Int],
                           sorting: Boolean): CloseableIterator[SimpleFeature] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val encoder = BinaryOutputEncoder(sft, EncodingOptions(geom, dtg, trackId, label))
    val sorted = if (!sorting) { features } else {
      val i = dtg.orElse(sft.getDtgIndex).getOrElse(throw new IllegalArgumentException("Can't sort BIN features by date"))
      CloseableIterator(features.toList.sortBy(_.getAttribute(i).asInstanceOf[Date]).iterator, features.close())
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

  private def arrowTransform(original: CloseableIterator[SimpleFeature],
                             sft: SimpleFeatureType,
                             transform: Option[(String, SimpleFeatureType)],
                             hints: Hints,
                             hook: Option[ArrowDictionaryHook]): CloseableIterator[SimpleFeature] = {

    import org.locationtech.geomesa.arrow.allocator

    ArrowScan.setSortHints(hints) // handle any sort hints from the query

    val sort = hints.getArrowSort
    val batchSize = ArrowScan.getBatchSize(hints)
    val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid)

    val (features, arrowSft) = transform match {
      case None =>
        val sorting = sort.map { case (field, reverse) =>
          if (reverse) { SimpleFeatureOrdering(sft, field).reverse } else { SimpleFeatureOrdering(sft, field) }
        }
        (noTransform(sft, original, sorting), sft)
      case Some((definitions, tsft)) =>
        val sorting = sort.map { case (field, reverse) =>
          if (reverse) { SimpleFeatureOrdering(tsft, field).reverse } else { SimpleFeatureOrdering(tsft, field) }
        }
        (projectionTransform(original, sft, tsft, definitions, sorting), tsft)
    }

    lazy val ArrowDictionaryHook(stats, filter) = hook.getOrElse {
      throw new IllegalStateException("Arrow query called without required hooks for dictionary lookups")
    }

    val dictionaryFields = hints.getArrowDictionaryFields
    val providedDictionaries = hints.getArrowDictionaryEncodedValues(sft)
    val cachedDictionaries: Map[String, TopK[AnyRef]] = if (!hints.isArrowCachedDictionaries) { Map.empty } else {
      val toLookup = dictionaryFields.filterNot(providedDictionaries.contains)
      if (toLookup.isEmpty) { Map.empty } else {
        stats.getStats[TopK[AnyRef]](sft, toLookup).map(k => k.property -> k).toMap
      }
    }

    if (hints.isArrowDoublePass ||
        dictionaryFields.forall(f => providedDictionaries.contains(f) || cachedDictionaries.contains(f))) {
      // we have all the dictionary values, or we will run a query to determine them up front
      val dictionaries = ArrowScan.createDictionaries(stats, sft, filter, dictionaryFields,
        providedDictionaries, cachedDictionaries)

      val vector = SimpleFeatureVector.create(arrowSft, dictionaries, encoding)
      val batchWriter = new RecordBatchUnloader(vector)

      val sf = ArrowScan.resultFeature()

      val arrows = new CloseableIterator[SimpleFeature] {
        override def hasNext: Boolean = features.hasNext
        override def next(): SimpleFeature = {
          var index = 0
          vector.clear()
          while (index < batchSize && features.hasNext) {
            vector.writer.set(index, features.next)
            index += 1
          }
          sf.setAttribute(0, batchWriter.unload(index))
          sf
        }
        override def close(): Unit = features.close()
      }

      if (hints.isSkipReduce) { arrows } else {
        ArrowScan.mergeBatches(arrowSft, dictionaries, encoding, batchSize, sort)(arrows)
      }
    } else if (hints.isArrowMultiFile) {
      val writer = DictionaryBuildingWriter.create(arrowSft, dictionaryFields, encoding)
      val os = new ByteArrayOutputStream()

      val sf = ArrowScan.resultFeature()

      val arrows = new CloseableIterator[SimpleFeature] {
        override def hasNext: Boolean = features.hasNext
        override def next(): SimpleFeature = {
          writer.clear()
          os.reset()
          var index = 0
          while (index < batchSize && features.hasNext) {
            writer.add(features.next)
            index += 1
          }
          writer.encode(os)
          sf.setAttribute(0, os.toByteArray)
          sf
        }
        override def close(): Unit = features.close()
      }
      if (hints.isSkipReduce) { arrows } else {
        ArrowScan.mergeFiles(arrowSft, dictionaryFields, encoding, sort)(arrows)
      }
    } else {
      val writer = new DeltaWriter(arrowSft, dictionaryFields, encoding, None, batchSize)
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
        override def close(): Unit = features.close()
      }
      if (hints.isSkipReduce) { arrows } else {
        ArrowScan.mergeDeltas(arrowSft, dictionaryFields, encoding, batchSize, sort)(arrows)
      }
    }
  }

  private def densityTransform(features: CloseableIterator[SimpleFeature],
                               sft: SimpleFeatureType,
                               envelope: Envelope,
                               width: Int,
                               height: Int,
                               weight: Option[String]): CloseableIterator[SimpleFeature] = {
    val grid = new GridSnap(envelope, width, height)
    val result = scala.collection.mutable.Map.empty[(Int, Int), Double].withDefaultValue(0d)
    val getWeight = DensityScan.getWeight(sft, weight)
    val writeGeom = DensityScan.writeGeometry(sft, grid)
    try { features.foreach(f => writeGeom(f, getWeight(f), result)) } finally { features.close() }

    val sf = new ScalaSimpleFeature(DensityScan.DensitySft, "", Array(GeometryUtils.zeroPoint))
    // Return value in user data so it's preserved when passed through a RetypingFeatureCollection
    sf.getUserData.put(DensityScan.DensityValueKey, DensityScan.encodeResult(result))
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

  private def projectionTransform(features: CloseableIterator[SimpleFeature],
                                  sft: SimpleFeatureType,
                                  transform: SimpleFeatureType,
                                  definitions: String,
                                  ordering: Option[Ordering[SimpleFeature]]): CloseableIterator[SimpleFeature] = {
    val attributes = TransformSimpleFeature.attributes(sft, transform, definitions)

    def setValues(from: SimpleFeature, to: ScalaSimpleFeature): ScalaSimpleFeature = {
      var i = 0
      while (i < attributes.length) {
        to.setAttributeNoConvert(i, attributes(i).apply(from))
        i += 1
      }
      to.setId(from.getID)
      to
    }

    val result = features.map(setValues(_, new ScalaSimpleFeature(transform, "")))

    ordering match {
      case None    => result
      case Some(o) => CloseableIterator(result.toList.sorted(o).iterator, result.close())
    }
  }

  private def noTransform(sft: SimpleFeatureType,
                          features: CloseableIterator[SimpleFeature],
                          ordering: Option[Ordering[SimpleFeature]]): CloseableIterator[SimpleFeature] = {
    ordering match {
      case None    => features
      case Some(o) => CloseableIterator(features.toList.sorted(o).iterator, features.close())
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

  /**
    * Used when we don't have an auth provider - any visibilities in the feature will
    * cause the check to fail, so we can skip parsing
    *
    * @param f simple feature to check
    * @return true if feature is visible without any authorizations, otherwise false
    */
  private def noAuthVisibilityCheck(f: SimpleFeature): Boolean = {
    val vis = SecurityUtils.getVisibility(f)
    vis == null || vis.isEmpty
  }

  /**
    * Parses any visibilities in the feature and compares with the user's authorizations
    *
    * @param f simple feature to check
    * @param auths authorizations for the current user
    * @return true if feature is visible to the current user, otherwise false
    */
  private def authVisibilityCheck(f: SimpleFeature, auths: Seq[Array[Byte]]): Boolean = {
    val vis = SecurityUtils.getVisibility(f)
    vis == null || VisibilityEvaluator.parse(vis).evaluate(auths)
  }
}
