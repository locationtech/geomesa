/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.Date

import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.io.{DeltaWriter, DictionaryBuildingWriter}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.iterators.{ArrowScan, DensityScan, StatsScan}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.{Explainer, Reprojection}
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityUtils, VisibilityEvaluator}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap, SimpleFeatureOrdering}
import org.locationtech.geomesa.utils.stats.{Stat, TopK}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

abstract class InMemoryQueryRunner(stats: GeoMesaStats, authProvider: Option[AuthorizationsProvider])
    extends QueryRunner {

  import InMemoryQueryRunner.{authVisibilityCheck, noAuthVisibilityCheck, transform}
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private val isVisible: (SimpleFeature, Seq[Array[Byte]]) => Boolean =
    if (authProvider.isDefined) { authVisibilityCheck } else { noAuthVisibilityCheck }

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
    import scala.collection.JavaConversions._

    val auths = authProvider.map(_.getAuthorizations.map(_.getBytes(StandardCharsets.UTF_8))).getOrElse(Seq.empty)

    val query = configureQuery(sft, original)
    optimizeFilter(sft, query)

    explain.pushLevel(s"$name query: '${sft.getTypeName}' ${org.locationtech.geomesa.filter.filterToString(query.getFilter)}")
    explain(s"bin[${query.getHints.isBinQuery}] arrow[${query.getHints.isArrowQuery}] " +
        s"density[${query.getHints.isDensityQuery}] stats[${query.getHints.isStatsQuery}]")
    explain(s"Transforms: ${query.getHints.getTransformDefinition.getOrElse("None")}")
    explain(s"Sort: ${query.getHints.getSortReadableString}")
    explain.popLevel()

    val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE)
    val iter = features(sft, filter).filter(isVisible(_, auths))

    val result = CloseableIterator(transform(iter, sft, stats, query.getHints, filter))

    Reprojection(query) match {
      case None    => result
      case Some(r) => result.map(r.reproject)
    }
  }

  override protected def optimizeFilter(sft: SimpleFeatureType, filter: Filter): Filter =
    FastFilterFactory.optimize(sft, filter)

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

object InMemoryQueryRunner {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  def transform(features: Iterator[SimpleFeature],
                sft: SimpleFeatureType,
                stats: GeoMesaStats,
                hints: Hints,
                filter: Option[Filter]): Iterator[SimpleFeature] = {
    if (hints.isBinQuery) {
      val trackId = Option(hints.getBinTrackIdField).map(sft.indexOf)
      val geom = hints.getBinGeomField.map(sft.indexOf)
      val dtg = hints.getBinDtgField.map(sft.indexOf)
      binTransform(features, sft, trackId, geom, dtg, hints.getBinLabelField.map(sft.indexOf), hints.isBinSorting)
    } else if (hints.isArrowQuery) {
      arrowTransform(features, sft, stats, hints, filter)
    } else if (hints.isDensityQuery) {
      val Some(envelope) = hints.getDensityEnvelope
      val Some((width, height)) = hints.getDensityBounds
      densityTransform(features, sft, envelope, width, height, hints.getDensityWeight)
    } else if (hints.isStatsQuery) {
      statsTransform(features, sft, hints.getTransform, hints.getStatsQuery, hints.isStatsEncode || hints.isSkipReduce)
    } else {
      hints.getTransform match {
        case None =>
          val sort = hints.getSortFields.map(SimpleFeatureOrdering(sft, _))
          noTransform(sft, features, sort)
        case Some((defs, tsft)) =>
          val sort = hints.getSortFields.map(SimpleFeatureOrdering(tsft, _))
          projectionTransform(features, sft, tsft, defs, sort)
      }
    }
  }

  private def binTransform(features: Iterator[SimpleFeature],
                           sft: SimpleFeatureType,
                           trackId: Option[Int],
                           geom: Option[Int],
                           dtg: Option[Int],
                           label: Option[Int],
                           sorting: Boolean): Iterator[SimpleFeature] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val encoder = BinaryOutputEncoder(sft, EncodingOptions(geom, dtg, trackId, label))
    val sf = new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, "", Array(null, GeometryUtils.zeroPoint))
    val sorted = if (!sorting) { features } else {
      val i = dtg.orElse(sft.getDtgIndex).getOrElse(throw new IllegalArgumentException("Can't sort BIN features by date"))
      features.toList.sortBy(_.getAttribute(i).asInstanceOf[Date]).iterator
    }
    sorted.map { feature =>
      sf.setAttribute(BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX, encoder.encode(feature))
      sf
    }
  }

  private def arrowTransform(original: Iterator[SimpleFeature],
                             sft: SimpleFeatureType,
                             stats: GeoMesaStats,
                             hints: Hints,
                             filter: Option[Filter]): Iterator[SimpleFeature] = {

    import org.locationtech.geomesa.arrow.allocator

    val sort = hints.getArrowSort
    val batchSize = ArrowScan.getBatchSize(hints)
    val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid)

    val (features, arrowSft) = hints.getTransform match {
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

    val dictionaryFields = hints.getArrowDictionaryFields
    val providedDictionaries = hints.getArrowDictionaryEncodedValues(sft)
    val cachedDictionaries: Map[String, TopK[AnyRef]] = if (!hints.isArrowCachedDictionaries) { Map.empty } else {
      def name(i: Int): String = sft.getDescriptor(i).getLocalName
      val toLookup = dictionaryFields.filterNot(providedDictionaries.contains)
      stats.getStats[TopK[AnyRef]](sft, toLookup).map(k => name(k.attribute) -> k).toMap
    }

    if (hints.isArrowDoublePass ||
        dictionaryFields.forall(f => providedDictionaries.contains(f) || cachedDictionaries.contains(f))) {
      // we have all the dictionary values, or we will run a query to determine them up front
      val dictionaries = ArrowScan.createDictionaries(stats, sft, filter, dictionaryFields,
        providedDictionaries, cachedDictionaries)

      val vector = SimpleFeatureVector.create(arrowSft, dictionaries, encoding)
      val batchWriter = new RecordBatchUnloader(vector)

      val sf = ArrowScan.resultFeature()

      val arrows = new Iterator[SimpleFeature] {
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
      }

      if (hints.isSkipReduce) { arrows } else {
        ArrowScan.mergeBatches(arrowSft, dictionaries, encoding, batchSize, sort)(arrows)
      }
    } else if (hints.isArrowMultiFile) {
      val writer = DictionaryBuildingWriter.create(arrowSft, dictionaryFields, encoding)
      val os = new ByteArrayOutputStream()

      val sf = ArrowScan.resultFeature()

      val arrows = new Iterator[SimpleFeature] {
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
      }
      if (hints.isSkipReduce) { arrows } else {
        ArrowScan.mergeFiles(arrowSft, dictionaryFields, encoding, sort)(arrows)
      }
    } else {
      val writer = new DeltaWriter(arrowSft, dictionaryFields, encoding, None, batchSize)
      val array = Array.ofDim[SimpleFeature](batchSize)

      val sf = ArrowScan.resultFeature()

      val arrows = new Iterator[SimpleFeature] {
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
      }
      if (hints.isSkipReduce) { arrows } else {
        ArrowScan.mergeDeltas(arrowSft, dictionaryFields, encoding, batchSize, sort)(arrows)
      }
    }
  }

  private def densityTransform(features: Iterator[SimpleFeature],
                               sft: SimpleFeatureType,
                               envelope: Envelope,
                               width: Int,
                               height: Int,
                               weight: Option[String]): Iterator[SimpleFeature] = {
    val grid = new GridSnap(envelope, width, height)
    val result = scala.collection.mutable.Map.empty[(Int, Int), Double]
    val getWeight = DensityScan.getWeight(sft, weight)
    val writeGeom = DensityScan.writeGeometry(sft, grid)
    features.foreach(f => writeGeom(f, getWeight(f), result))

    val sf = new ScalaSimpleFeature(DensityScan.DensitySft, "", Array(GeometryUtils.zeroPoint))
    // Return value in user data so it's preserved when passed through a RetypingFeatureCollection
    sf.getUserData.put(DensityScan.DensityValueKey, DensityScan.encodeResult(result))
    Iterator(sf)
  }

  private def statsTransform(features: Iterator[SimpleFeature],
                             sft: SimpleFeatureType,
                             transform: Option[(String, SimpleFeatureType)],
                             query: String,
                             encode: Boolean): Iterator[SimpleFeature] = {
    val stat = Stat(sft, query)
    val toObserve = transform match {
      case None                => features
      case Some((tdefs, tsft)) => projectionTransform(features, sft, tsft, tdefs, None)
    }
    toObserve.foreach(stat.observe)
    val encoded = if (encode) { StatsScan.encodeStat(sft)(stat) } else { stat.toJson }
    Iterator(new ScalaSimpleFeature(StatsScan.StatsSft, "stat", Array(encoded, GeometryUtils.zeroPoint)))
  }

  private def projectionTransform(features: Iterator[SimpleFeature],
                                  sft: SimpleFeatureType,
                                  transform: SimpleFeatureType,
                                  definitions: String,
                                  ordering: Option[Ordering[SimpleFeature]]): Iterator[SimpleFeature] = {
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

    ordering match {
      case None => val reusableSf = new ScalaSimpleFeature(transform, ""); features.map(setValues(_, reusableSf))
      case Some(o) => features.map(setValues(_, new ScalaSimpleFeature(transform, ""))).toList.sorted(o).iterator
    }
  }

  private def noTransform(sft: SimpleFeatureType,
                          features: Iterator[SimpleFeature],
                          ordering: Option[Ordering[SimpleFeature]]): Iterator[SimpleFeature] = {
    ordering match {
      case None    => features
      case Some(o) => features.toList.sorted(o).iterator
    }
  }

  /**
    * Used when we don't have an auth provider - any visibilities in the feature will
    * cause the check to fail, so we can skip parsing
    *
    * @param f simple feature to check
    * @param ignored not used
    * @return true if feature is visible without any authorizations, otherwise false
    */
  private def noAuthVisibilityCheck(f: SimpleFeature, ignored: Seq[Array[Byte]]): Boolean = {
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
