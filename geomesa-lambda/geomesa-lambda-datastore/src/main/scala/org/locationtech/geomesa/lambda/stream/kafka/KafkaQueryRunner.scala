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
import org.locationtech.geomesa.arrow.io.DictionaryBuildingWriter
import org.locationtech.geomesa.arrow.io.records.RecordBatchUnloader
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.vector.{ArrowDictionary, SimpleFeatureVector}
import org.locationtech.geomesa.arrow.{ArrowEncodedSft, ArrowProperties}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.iterators.{ArrowBatchScan, DensityScan}
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.{Explainer, KryoLazyStatsUtils}
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache.ReadableFeatureCache
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityUtils, VisibilityEvaluator}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap}
import org.locationtech.geomesa.utils.stats.Stat
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{Filter, Id}

import scala.math.Ordering

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
    val sf = new ScalaSimpleFeature("", BinaryOutputEncoder.BinEncodedSft, Array(null, GeometryUtils.zeroPoint))
    features.map { feature =>
      sf.setAttribute(BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX, encoder.encode(feature))
      sf
    }
  }

  private def arrowTransform(features: Iterator[SimpleFeature],
                             sft: SimpleFeatureType,
                             hints: Hints,
                             filter: Filter): Iterator[SimpleFeature] = {

    val (transforms, arrowSft) = hints.getTransform match {
      case None => (features, sft)
      case Some((definitions, transform)) => (projectionTransform(features, transform, definitions), transform)
    }
    val batchSize = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)
    val dictionaryFields = hints.getArrowDictionaryFields
    val providedDictionaries = hints.getArrowDictionaryEncodedValues
    val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid)

    if (hints.getArrowSort.isDefined || hints.isArrowComputeDictionaries ||
        dictionaryFields.forall(providedDictionaries.contains)) {
      val dictionaries = ArrowBatchScan.createDictionaries(stats, sft, Option(filter), dictionaryFields,
        providedDictionaries, hints.isArrowCachedDictionaries)
      val arrows = hints.getArrowSort match {
        case None => arrowBatchTransform(transforms, arrowSft, encoding, dictionaries, batchSize)
        case Some((sortField, reverse)) => arrowSortTransform(transforms, arrowSft, encoding, dictionaries, sortField, reverse, batchSize)
      }
      if (hints.isSkipReduce) { arrows } else { ArrowBatchScan.reduceFeatures(arrowSft, hints, dictionaries)(arrows) }
    } else {
      arrowFileTransform(transforms, arrowSft, encoding, dictionaryFields, batchSize)
    }
  }

  private def arrowBatchTransform(features: Iterator[SimpleFeature],
                                  sft: SimpleFeatureType,
                                  encoding: SimpleFeatureEncoding,
                                  dictionaries: Map[String, ArrowDictionary],
                                  batchSize: Int): Iterator[SimpleFeature] = {
    import org.locationtech.geomesa.arrow.allocator

    val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
    val batchWriter = new RecordBatchUnloader(vector)

    val sf = new ScalaSimpleFeature("", ArrowEncodedSft, Array(null, GeometryUtils.zeroPoint))

    new Iterator[SimpleFeature] {
      override def hasNext: Boolean = features.hasNext
      override def next(): SimpleFeature = {
        var index = 0
        vector.clear()
        features.take(batchSize).foreach { f =>
          vector.writer.set(index, f)
          index += 1
        }

        sf.setAttribute(0, batchWriter.unload(index))
        sf
      }
    }
  }

  private def arrowSortTransform(features: Iterator[SimpleFeature],
                                 sft: SimpleFeatureType,
                                 encoding: SimpleFeatureEncoding,
                                 dictionaries: Map[String, ArrowDictionary],
                                 sortField: String,
                                 sortReverse: Boolean,
                                 batchSize: Int): Iterator[SimpleFeature] = {
    import org.locationtech.geomesa.arrow.allocator

    val array = Array.ofDim[SimpleFeature](batchSize)

    val vector = SimpleFeatureVector.create(sft, dictionaries, encoding)
    val batchWriter = new RecordBatchUnloader(vector)
    val sortIndex = sft.indexOf(sortField)

    val ordering = new Ordering[SimpleFeature] {
      override def compare(x: SimpleFeature, y: SimpleFeature): Int = {
        val left = x.getAttribute(sortIndex).asInstanceOf[Comparable[Any]]
        val right = y.getAttribute(sortIndex).asInstanceOf[Comparable[Any]]
        left.compareTo(right)
      }
    }

    val sf = new ScalaSimpleFeature("", ArrowEncodedSft, Array(null, GeometryUtils.zeroPoint))

    new Iterator[SimpleFeature] {
      override def hasNext: Boolean = features.hasNext
      override def next(): SimpleFeature = {
        var index = 0
        vector.clear()
        features.take(batchSize).foreach { f =>
          array(index) = ScalaSimpleFeature.copy(f) // we have to copy since the same feature object is re-used
          index += 1
        }
        java.util.Arrays.sort(array, 0, index, if (sortReverse) { ordering.reverse } else { ordering })

        var i = 0
        while (i < index) {
          vector.writer.set(i, array(i))
          i += 1
        }

        sf.setAttribute(0, batchWriter.unload(index))
        sf
      }
    }
  }

  private def arrowFileTransform(features: Iterator[SimpleFeature],
                                 sft: SimpleFeatureType,
                                 encoding: SimpleFeatureEncoding,
                                 dictionaryFields: Seq[String],
                                 batchSize: Int): Iterator[SimpleFeature] = {
    import org.locationtech.geomesa.arrow.allocator

    val writer = DictionaryBuildingWriter.create(sft, dictionaryFields, encoding)
    val os = new ByteArrayOutputStream()

    val sf = new ScalaSimpleFeature("", ArrowEncodedSft, Array(null, GeometryUtils.zeroPoint))

    new Iterator[SimpleFeature] {
      override def hasNext: Boolean = features.hasNext
      override def next(): SimpleFeature = {
        writer.clear()
        os.reset()
        features.take(batchSize).foreach(writer.add)
        writer.encode(os)
        sf.setAttribute(0, os.toByteArray)
        sf
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

    val sf = new ScalaSimpleFeature("", DensityScan.DensitySft, Array(GeometryUtils.zeroPoint))
    // Return value in user data so it's preserved when passed through a RetypingFeatureCollection
    sf.getUserData.put(DensityScan.DensityValueKey, DensityScan.encodeResult(result))
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
    Iterator(new ScalaSimpleFeature("stat", KryoLazyStatsUtils.StatsSft, Array(encoded, GeometryUtils.zeroPoint)))
  }

  private def projectionTransform(features: Iterator[SimpleFeature],
                                  transform: SimpleFeatureType,
                                  definitions: String): Iterator[SimpleFeature] = {
    val tdefs = TransformProcess.toDefinition(definitions)
    val reusableSf = new ScalaSimpleFeature("", transform)
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