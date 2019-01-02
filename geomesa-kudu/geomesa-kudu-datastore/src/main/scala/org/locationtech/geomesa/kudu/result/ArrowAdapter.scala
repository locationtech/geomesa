/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.result

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.kudu.client.RowResult
import org.geotools.data.DataUtilities
import org.geotools.filter.text.ecql.ECQL
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.{FilterHelper, filterToString}
import org.locationtech.geomesa.index.iterators.ArrowScan
import org.locationtech.geomesa.index.iterators.ArrowScan._
import org.locationtech.geomesa.kudu.result.ArrowAdapter.ArrowConfig
import org.locationtech.geomesa.kudu.result.KuduResultAdapter.KuduResultAdapterSerialization
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, UnusedFeatureIdAdapter, VisibilityAdapter}
import org.locationtech.geomesa.kudu.schema.{KuduSimpleFeatureSchema, RowResultSimpleFeature}
import org.locationtech.geomesa.security.{SecurityUtils, VisibilityEvaluator}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.ByteBuffers.ExpandingByteBuffer
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.{Expression, PropertyName}

/**
  * Converts rows into arrow vectors
  *
  * @param sft simple feature type
  * @param auths authorizations
  * @param ecql filter
  * @param transform transform
  * @param config arrow output configuration
  */
case class ArrowAdapter(sft: SimpleFeatureType,
                        auths: Seq[Array[Byte]],
                        ecql: Option[Filter],
                        transform: Option[(String, SimpleFeatureType)],
                        config: ArrowConfig) extends KuduResultAdapter {

  import scala.collection.JavaConverters._

  require(!config.doublePass, "Double pass Arrow dictionary scans are not supported")

  private val requiresFid = config.includeFid || ecql.exists(FilterHelper.hasIdFilter)

  private val encoding = SimpleFeatureEncoding.min(config.includeFid, config.proxyFid)

  // determine all the attributes that we need to be able to evaluate the transform and filter
  private val attributes = transform.map { case (tdefs, _) =>
    val fromTransform = TransformProcess.toDefinition(tdefs).asScala.map(_.expression).flatMap {
      case p: PropertyName => Seq(p.getPropertyName)
      case e: Expression   => DataUtilities.attributeNames(e, sft)
    }
    val fromFilter = ecql.map(FilterHelper.propertyNames(_, sft)).getOrElse(Seq.empty)
    (fromTransform ++ fromFilter).distinct
  }

  // type serialized to arrow vectors
  private val arrowSft = transform.map(_._2).getOrElse(sft)

  private val schema = KuduSimpleFeatureSchema(sft)
  private val featureIdAdapter = if (requiresFid) { FeatureIdAdapter } else { UnusedFeatureIdAdapter }
  private val feature = new RowResultSimpleFeature(sft, featureIdAdapter, schema.adapters)
  private val transformFeature = transform match {
    case None => feature
    case Some((tdefs, tsft)) =>
      val tf = TransformSimpleFeature(sft, tsft, tdefs)
      tf.setFeature(feature)
      tf
  }

  override val columns: Seq[String] = {
    val base = if (requiresFid) { Seq(FeatureIdAdapter, VisibilityAdapter) } else { Seq(VisibilityAdapter) }
    val props = attributes.map(schema.schema(_)).getOrElse(schema.schema).map(_.getName)
    base.map(_.name) ++ props
  }

  override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
    val dictionaries = config.dictionaryFields

    val (aggregator, reduce) = if (dictionaries.forall(f => config.providedDictionaries.contains(f))) {
      // we have all the dictionary values
      val dicts = ArrowScan.createDictionaries(null, sft, ecql, config.dictionaryFields,
        config.providedDictionaries, Map.empty)
      val aggregate = config.sort match {
        case None => new BatchAggregate(arrowSft, dicts, encoding)
        case Some((sort, reverse)) => new SortingBatchAggregate(arrowSft, dicts, encoding, sort, reverse)
      }
      val reduce = ArrowScan.mergeBatches(arrowSft, dicts, encoding, config.batchSize, config.sort) _
      (aggregate, reduce)
    } else if (config.multiFile) {
      val aggregate = config.sort match {
        case None => new MultiFileAggregate(arrowSft, dictionaries, encoding)
        case Some((sort, reverse)) => new MultiFileSortingAggregate(arrowSft, dictionaries, encoding, sort, reverse)
      }
      val reduce = ArrowScan.mergeFiles(arrowSft, dictionaries, encoding, config.sort) _
      (aggregate, reduce)
    } else {
      val aggregate = new DeltaAggregate(arrowSft, dictionaries, encoding, config.sort, config.batchSize)
      val reduce = ArrowScan.mergeDeltas(arrowSft, dictionaries, encoding, config.batchSize, config.sort) _
      (aggregate, reduce)
    }

    val features = results.flatMap { row =>
      val vis = VisibilityAdapter.readFromRow(row)
      if ((vis == null || VisibilityEvaluator.parse(vis).evaluate(auths)) &&
          { feature.setRowResult(row); ecql.forall(_.evaluate(feature)) }) {
        SecurityUtils.setFeatureVisibility(feature, vis)
        Iterator.single(transformFeature)
      } else {
        CloseableIterator.empty
      }
    }

    aggregator.init(config.batchSize)

    val arrows = new Iterator[SimpleFeature] {
      private val sf = ArrowScan.resultFeature()

      override def hasNext: Boolean = features.hasNext
      override def next(): SimpleFeature = {
        aggregator.clear()
        while (features.hasNext && aggregator.size < config.batchSize) {
          aggregator.add(features.next)
        }
        sf.setAttribute(0, aggregator.encode())
        sf
      }
    }

    val result = CloseableIterator(arrows, features.close())
    if (config.skipReduce) { result } else { reduce(result) }
  }

  override def toString: String =
    s"ArrowAdapter(sft=${sft.getTypeName}{${SimpleFeatureTypes.encodeType(sft)}}, " +
        s"filter=${ecql.map(filterToString).getOrElse("INCLUDE")}, " +
        s"transform=${transform.map(_._1).getOrElse("")}, config=$config" +
        s"auths=${auths.map(new String(_, StandardCharsets.UTF_8)).mkString(",")})"
}

object ArrowAdapter extends KuduResultAdapterSerialization[ArrowAdapter] {

  override def serialize(adapter: ArrowAdapter, bb: ExpandingByteBuffer): Unit = {
    bb.putString(adapter.sft.getTypeName)
    bb.putString(SimpleFeatureTypes.encodeType(adapter.sft, includeUserData = true))
    bb.putInt(adapter.auths.length)
    adapter.auths.foreach(bb.putBytes)
    bb.putString(adapter.ecql.map(ECQL.toCQL).orNull)
    bb.putString(adapter.transform.map(t => SimpleFeatureTypes.encodeType(t._2, includeUserData = true)).orNull)
    bb.putString(adapter.transform.map(_._1).orNull)
    bb.putBool(adapter.config.includeFid)
    bb.putBool(adapter.config.proxyFid)
    bb.putInt(adapter.config.dictionaryFields.length)
    adapter.config.dictionaryFields.foreach(bb.putString)
    if (adapter.config.providedDictionaries.isEmpty) {
      bb.putString(null)
    } else {
      bb.putString(StringSerialization.encodeSeqMap(adapter.config.providedDictionaries.mapValues(_.toSeq)))
    }
    bb.putString(adapter.config.sort.map(_._1).orNull)
    bb.putBool(adapter.config.sort.exists(_._2))
    bb.putInt(adapter.config.batchSize)
    bb.putBool(adapter.config.skipReduce)
    bb.putBool(adapter.config.doublePass)
    bb.putBool(adapter.config.multiFile)
  }

  override def deserialize(bb: ByteBuffer): ArrowAdapter = {
    import org.locationtech.geomesa.utils.io.ByteBuffers.RichByteBuffer

    val sft = SimpleFeatureTypes.createType(bb.getString, bb.getString)
    val auths = Seq.fill(bb.getInt)(bb.getBytes)
    val ecql = Option(bb.getString).map(FastFilterFactory.toFilter(sft, _))
    val tsft = Option(bb.getString).map(SimpleFeatureTypes.createType(sft.getTypeName, _))
    val tdefs = Option(bb.getString)
    val transform = tsft.flatMap(s => tdefs.map(d => (d, s)))
    val includeFid = bb.getBool
    val proxyFid = bb.getBool
    val dictionaryFields = Seq.fill(bb.getInt)(bb.getString)
    val providedDictionaries = Option(bb.getString).map(StringSerialization.decodeSeqMap(sft, _)).getOrElse(Map.empty)
    val sortField = Option(bb.getString)
    val sortReverse = bb.getBool
    val sort = sortField.map((_, sortReverse))
    val batchSize = bb.getInt
    val skipReduce = bb.getBool
    val doublePass = bb.getBool
    val multiFile = bb.getBool

    val config = ArrowConfig(includeFid, proxyFid, dictionaryFields, providedDictionaries, sort,
      batchSize, skipReduce, doublePass, multiFile)

    ArrowAdapter(sft, auths, ecql, transform, config)
  }

  case class ArrowConfig(includeFid: Boolean,
                         proxyFid: Boolean,
                         dictionaryFields: Seq[String],
                         providedDictionaries: Map[String, Array[AnyRef]],
                         sort: Option[(String, Boolean)],
                         batchSize: Int,
                         skipReduce: Boolean,
                         doublePass: Boolean,
                         multiFile: Boolean)
}
