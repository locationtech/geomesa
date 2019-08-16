/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.io.{DeltaWriter, DictionaryBuildingWriter}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.iterators.{ArrowScan, DensityScan}
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.{Explainer, KryoLazyStatsUtils}
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache.ReadableFeatureCache
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityUtils, VisibilityEvaluator}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, RenderingGrid, SimpleFeatureOrdering}
import org.locationtech.geomesa.utils.stats.{Stat, TopK}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{Filter, Id}

class KafkaQueryRunner(features: ReadableFeatureCache, stats: GeoMesaStats, authProvider: Option[AuthorizationsProvider])
    extends QueryRunner {

  import KafkaQueryRunner.{authVisibilityCheck, noAuthVisibilityCheck}
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private val isVisible: (SimpleFeature, Seq[Array[Byte]]) => Boolean =
    if (authProvider.isDefined) { authVisibilityCheck } else { noAuthVisibilityCheck }

  override def runQuery(sft: SimpleFeatureType, original: Query, explain: Explainer): CloseableIterator[SimpleFeature] = {
    import scala.collection.JavaConversions._

    val auths = authProvider.map(_.getAuthorizations.map(_.getBytes(StandardCharsets.UTF_8))).getOrElse(Seq.empty)

    val query = configureQuery(sft, original)
    optimizeFilter(sft, query)

    explain.pushLevel(s"Kafka lambda query: '${sft.getTypeName}' ${org.locationtech.geomesa.filter.filterToString(query.getFilter)}")
    explain(s"bin[${query.getHints.isBinQuery}] arrow[${query.getHints.isArrowQuery}] " +
        s"density[${query.getHints.isDensityQuery}] stats[${query.getHints.isStatsQuery}]")
    explain(s"Transforms: ${query.getHints.getTransformDefinition.getOrElse("None")}")
    explain(s"Sort: ${Option(query.getSortBy).filter(_.nonEmpty).map(_.mkString(", ")).getOrElse("none")}")
    explain.popLevel()

    // just iterate through the features
    // we could use an in-memory index here if performance isn't good enough

    val iter = Option(query.getFilter).filter(_ != Filter.INCLUDE) match {
      case Some(f: Id) => f.getIDs.iterator.map(i => features.get(i.toString)).filter(sf => sf != null && isVisible(sf, auths))
      case Some(f)     => features.all().filter(sf => f.evaluate(sf) && isVisible(sf, auths))
      case None        => features.all().filter(sf => isVisible(sf, auths))
    }

    CloseableIterator(transform(iter, sft, query.getHints, query.getFilter))
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
      KryoLazyStatsUtils.StatsSft
    } else {
      super.getReturnSft(sft, hints)
    }
  }

  private def transform(features: Iterator[SimpleFeature],
                        sft: SimpleFeatureType,
                        hints: Hints,
                        filter: Filter): Iterator[SimpleFeature] = {
    if (hints.isBinQuery) {
      val trackId = Option(hints.getBinTrackIdField).map(sft.indexOf)
      val geom = hints.getBinGeomField.map(sft.indexOf)
      val dtg = hints.getBinDtgField.map(sft.indexOf)
      binTransform(features, sft, trackId, geom, dtg, hints.getBinLabelField.map(sft.indexOf))
    } else if (hints.isArrowQuery) {
      arrowTransform(features, sft, hints, filter)
    } else if (hints.isDensityQuery) {
      val Some(envelope) = hints.getDensityEnvelope
      val Some((width, height)) = hints.getDensityBounds
      densityTransform(features, sft, envelope, width, height, hints.getDensityWeight)
    } else if (hints.isStatsQuery) {
      statsTransform(features, hints.getTransformSchema.getOrElse(sft), hints.getTransformDefinition,
        hints.getStatsQuery, hints.isStatsEncode || hints.isSkipReduce)
    } else {
      hints.getTransform match {
        case None => features
        case Some((definitions, transform)) => projectionTransform(features, transform, definitions)
      }
    }
  }

  private def binTransform(features: Iterator[SimpleFeature],
                           sft: SimpleFeatureType,
                           trackId: Option[Int],
                           geom: Option[Int],
                           dtg: Option[Int],
                           label: Option[Int]): Iterator[SimpleFeature] = {
    val encoder = BinaryOutputEncoder(sft, EncodingOptions(geom, dtg, trackId, label))
    val sf = new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, "", Array(null, GeometryUtils.zeroPoint))
    features.map { feature =>
      sf.setAttribute(BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX, encoder.encode(feature))
      sf
    }
  }

  private def arrowTransform(original: Iterator[SimpleFeature],
                             sft: SimpleFeatureType,
                             hints: Hints,
                             filt: Filter): Iterator[SimpleFeature] = {

    import org.locationtech.geomesa.arrow.allocator

    val filter = Option(filt).filter(_ != Filter.INCLUDE)

    val sort = hints.getArrowSort
    val batchSize = ArrowScan.getBatchSize(hints)
    val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid)

    val (features, arrowSft) = hints.getTransform match {
      case None =>
        val sorting = sort.map { case (field, reverse) =>
          if (reverse) { SimpleFeatureOrdering(sft, field).reverse } else { SimpleFeatureOrdering(sft, field) }
        }
        sorting match {
          case None => (original, sft)
          case Some(o) => (original.toSeq.sorted(o).iterator, sft)
        }
      case Some((definitions, tsft)) =>
        val sorting = sort.map { case (field, reverse) =>
          if (reverse) { SimpleFeatureOrdering(tsft, field).reverse } else { SimpleFeatureOrdering(tsft, field) }
        }
        sorting match {
          case None => (projectionTransform(original, tsft, definitions), tsft)
          case Some(o) => (projectionTransform(original.toSeq.sorted(o).iterator, tsft, definitions), tsft)
        }
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
                             transform: Option[String],
                             query: String,
                             encode: Boolean): Iterator[SimpleFeature] = {
    val stat = Stat(sft, query)
    val toObserve = transform match {
      case None        => features
      case Some(tdefs) => projectionTransform(features, sft, tdefs)
    }
    toObserve.foreach(stat.observe)
    val encoded = if (encode) { KryoLazyStatsUtils.encodeStat(sft)(stat) } else { stat.toJson }
    Iterator(new ScalaSimpleFeature(KryoLazyStatsUtils.StatsSft, "stat", Array(encoded, GeometryUtils.zeroPoint)))
  }

  private def projectionTransform(features: Iterator[SimpleFeature],
                                  transform: SimpleFeatureType,
                                  definitions: String): Iterator[SimpleFeature] = {
    val tdefs = TransformProcess.toDefinition(definitions)
    val reusableSf = new ScalaSimpleFeature(transform, "")
    var i = 0
    features.map { feature =>
      reusableSf.setId(feature.getID)
      i = 0
      while (i < tdefs.size) {
        reusableSf.setAttribute(i, tdefs.get(i).expression.evaluate(feature))
        i += 1
      }
      reusableSf
    }
  }
}

object KafkaQueryRunner {

  private def noAuthVisibilityCheck(f: SimpleFeature, ignored: Seq[Array[Byte]]): Boolean = {
    val vis = SecurityUtils.getVisibility(f)
    vis == null || vis.isEmpty
  }

  private def authVisibilityCheck(f: SimpleFeature, auths: Seq[Array[Byte]]): Boolean = {
    val vis = SecurityUtils.getVisibility(f)
    vis == null || VisibilityEvaluator.parse(vis).evaluate(auths)
  }
}