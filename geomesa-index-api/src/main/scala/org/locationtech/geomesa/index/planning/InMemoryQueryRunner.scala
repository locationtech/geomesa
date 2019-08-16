/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import java.io.ByteArrayOutputStream
import java.util.Date

import com.vividsolutions.jts.geom.Envelope
import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.io.{DeltaWriter, DictionaryBuildingWriter}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.index.iterators.{ArrowScan, DensityScan}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.KryoLazyStatsUtils
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, RenderingGrid, SimpleFeatureOrdering}
import org.locationtech.geomesa.utils.stats.{Stat, TopK}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortBy

object InMemoryQueryRunner {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  case class ArrowDictionaryHook(stats: GeoMesaStats, filter: Option[Filter])

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
                features: Iterator[SimpleFeature],
                transform: Option[(String, SimpleFeatureType)],
                sort: Option[Array[SortBy]],
                hints: Hints,
                arrow: Option[ArrowDictionaryHook] = None): Iterator[SimpleFeature] = {
    if (hints.isBinQuery) {
      val trackId = Option(hints.getBinTrackIdField).filter(_ != "id").map(sft.indexOf)
      val geom = hints.getBinGeomField.map(sft.indexOf)
      val dtg = hints.getBinDtgField.map(sft.indexOf)
      binTransform(features, sft, trackId, geom, dtg, hints.getBinLabelField.map(sft.indexOf), hints.isBinSorting)
    } else if (hints.isArrowQuery) {
      arrowTransform(features, sft, transform, hints, arrow)
    } else if (hints.isDensityQuery) {
      val Some(envelope) = hints.getDensityEnvelope
      val Some((width, height)) = hints.getDensityBounds
      densityTransform(features, sft, envelope, width, height, hints.getDensityWeight)
    } else if (hints.isStatsQuery) {
      statsTransform(features, sft, transform, hints.getStatsQuery, hints.isStatsEncode || hints.isSkipReduce)
    } else {
      transform match {
        case None => noTransform(sft, features, sort.map(SimpleFeatureOrdering(sft, _)))
        case Some((defs, tsft)) => projectionTransform(features, sft, tsft, defs, sort.map(SimpleFeatureOrdering(tsft, _)))
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
                             transform: Option[(String, SimpleFeatureType)],
                             hints: Hints,
                             hook: Option[ArrowDictionaryHook]): Iterator[SimpleFeature] = {

    import org.locationtech.geomesa.arrow.allocator

    val sort = hints.getArrowSort
    val batchSize = ArrowScan.getBatchSize(hints)
    val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid)

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
        stats.getStats[TopK[AnyRef]](sft, toLookup).map(k => sft.getDescriptor(k.attribute).getLocalName -> k).toMap
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
    val grid = new RenderingGrid(envelope, width, height)
    val renderer = DensityScan.getRenderer(sft, weight)
    features.foreach(f => renderer.render(grid, f))

    val sf = new ScalaSimpleFeature(DensityScan.DensitySft, "", Array(GeometryUtils.zeroPoint))
    // Return value in user data so it's preserved when passed through a RetypingFeatureCollection
    sf.getUserData.put(DensityScan.DensityValueKey, DensityScan.encodeResult(grid))
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
    val encoded = if (encode) { KryoLazyStatsUtils.encodeStat(sft)(stat) } else { stat.toJson }
    Iterator(new ScalaSimpleFeature(KryoLazyStatsUtils.StatsSft, "stat", Array(encoded, GeometryUtils.zeroPoint)))
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
}
