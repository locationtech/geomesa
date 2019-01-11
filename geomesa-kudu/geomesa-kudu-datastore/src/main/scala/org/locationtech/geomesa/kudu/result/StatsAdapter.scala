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
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.{FilterHelper, filterToString}
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.kudu.result.KuduResultAdapter.KuduResultAdapterSerialization
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, UnusedFeatureIdAdapter, VisibilityAdapter}
import org.locationtech.geomesa.kudu.schema.{KuduSimpleFeatureSchema, RowResultSimpleFeature}
import org.locationtech.geomesa.security.VisibilityEvaluator
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.ByteBuffers.ExpandingByteBuffer
import org.locationtech.geomesa.utils.stats.{Stat, StatParser}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Aggregates rows into statistic objects
  *
  * @param sft simple feature type
  * @param auths authorizations
  * @param ecql filter
  * @param transform transform
  * @param query stat query
  * @param encode encode stats as binary or return as json
  */
case class StatsAdapter(sft: SimpleFeatureType,
                        auths: Seq[Array[Byte]],
                        ecql: Option[Filter],
                        transform: Option[(String, SimpleFeatureType)],
                        query: String,
                        encode: Boolean) extends KuduResultAdapter {

  private val requiresFid = ecql.exists(FilterHelper.hasIdFilter)

  // determine all the attributes that we need to be able to evaluate the transform and filter
  private val attributes = {
    val fromStat = StatParser.propertyNames(sft, query) // TODO support function transforms before the stat evaluation
    val fromFilter = ecql.map(FilterHelper.propertyNames(_, sft)).getOrElse(Seq.empty)
    (fromStat ++ fromFilter).distinct
  }

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

  private val statSft = transformFeature.getFeatureType

  override val columns: Seq[String] =
    if (requiresFid) { Seq(FeatureIdAdapter.name, VisibilityAdapter.name) } else { Seq(VisibilityAdapter.name) } ++
        schema.schema(attributes).map(_.getName)

  override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
    val stat = Stat(statSft, query)

    results.foreach { row =>
      val vis = VisibilityAdapter.readFromRow(row)
      if ((vis == null || VisibilityEvaluator.parse(vis).evaluate(auths)) &&
          { feature.setRowResult(row); ecql.forall(_.evaluate(feature)) }) {
        stat.observe(transformFeature)
      }
    }

    val encoded = if (encode) { StatsScan.encodeStat(statSft)(stat) } else { stat.toJson }
    val sf = new ScalaSimpleFeature(StatsScan.StatsSft, "stat", Array(encoded, GeometryUtils.zeroPoint))
    CloseableIterator(Iterator.single(sf))
  }

  override def toString: String =
    s"StatsAdapter(sft=${sft.getTypeName}{${SimpleFeatureTypes.encodeType(sft)}}, " +
        s"filter=${ecql.map(filterToString).getOrElse("INCLUDE")}, " +
        s"transform=${transform.map(_._1).getOrElse("")}, stat=$query, encode=$encode, " +
        s"auths=${auths.map(new String(_, StandardCharsets.UTF_8)).mkString(",")})"
}

object StatsAdapter extends KuduResultAdapterSerialization[StatsAdapter] {

  override def serialize(adapter: StatsAdapter, bb: ExpandingByteBuffer): Unit = {
    bb.putString(adapter.sft.getTypeName)
    bb.putString(SimpleFeatureTypes.encodeType(adapter.sft, includeUserData = true))
    bb.putInt(adapter.auths.length)
    adapter.auths.foreach(bb.putBytes)
    bb.putString(adapter.ecql.map(ECQL.toCQL).orNull)
    bb.putString(adapter.transform.map(t => SimpleFeatureTypes.encodeType(t._2, includeUserData = true)).orNull)
    bb.putString(adapter.transform.map(_._1).orNull)
    bb.putString(adapter.query)
    bb.putBool(adapter.encode)
  }

  override def deserialize(bb: ByteBuffer): StatsAdapter = {
    import org.locationtech.geomesa.utils.io.ByteBuffers.RichByteBuffer

    val sft = SimpleFeatureTypes.createType(bb.getString, bb.getString)
    val auths = Seq.fill(bb.getInt)(bb.getBytes)
    val ecql = Option(bb.getString).map(FastFilterFactory.toFilter(sft, _))
    val tsft = Option(bb.getString).map(SimpleFeatureTypes.createType(sft.getTypeName, _))
    val tdefs = Option(bb.getString)
    val transform = tsft.flatMap(s => tdefs.map(d => (d, s)))
    val query = bb.getString
    val encode = bb.getBool
    StatsAdapter(sft, auths, ecql, transform, query, encode)
  }
}
