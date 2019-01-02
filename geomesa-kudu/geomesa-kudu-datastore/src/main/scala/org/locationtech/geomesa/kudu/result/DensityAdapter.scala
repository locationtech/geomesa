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

import org.locationtech.jts.geom.Envelope
import org.apache.kudu.client.RowResult
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.{FilterHelper, filterToString}
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.kudu.result.KuduResultAdapter.KuduResultAdapterSerialization
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, UnusedFeatureIdAdapter, VisibilityAdapter}
import org.locationtech.geomesa.kudu.schema.{KuduSimpleFeatureSchema, RowResultSimpleFeature}
import org.locationtech.geomesa.security.VisibilityEvaluator
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.ByteBuffers.ExpandingByteBuffer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Aggregates rows into a density map
  *
  * @param sft simple feature type
  * @param auths authorizations
  * @param ecql filter
  * @param envelope rendering envelope
  * @param width width in pixels
  * @param height height in pixels
  * @param weight weight expression, for non-uniform feature weighting
  */
case class DensityAdapter(sft: SimpleFeatureType,
                          auths: Seq[Array[Byte]],
                          ecql: Option[Filter],
                          envelope: Envelope,
                          width: Int,
                          height: Int,
                          weight: Option[String]) extends KuduResultAdapter {

  private val requiresFid = ecql.exists(FilterHelper.hasIdFilter)

  // determine all the attributes that we need to be able to evaluate the transform and filter
  private val attributes = {
    val fromGeom = Seq(sft.getGeometryDescriptor.getLocalName)
    val fromWeight = weight.map(w => FilterHelper.propertyNames(ECQL.toExpression(w), sft)).getOrElse(Seq.empty)
    val fromFilter = ecql.map(FilterHelper.propertyNames(_, sft)).getOrElse(Seq.empty)
    (fromGeom ++ fromWeight ++ fromFilter).distinct
  }

  private val schema = KuduSimpleFeatureSchema(sft)
  private val featureIdAdapter = if (requiresFid) { FeatureIdAdapter } else { UnusedFeatureIdAdapter }
  private val feature = new RowResultSimpleFeature(sft, featureIdAdapter, schema.adapters)

  override val columns: Seq[String] =
    if (requiresFid) { Seq(FeatureIdAdapter.name, VisibilityAdapter.name) } else { Seq(VisibilityAdapter.name) } ++
        schema.schema(attributes).map(_.getName)

  override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
    val grid = new GridSnap(envelope, width, height)
    val result = scala.collection.mutable.Map.empty[(Int, Int), Double].withDefaultValue(0d)
    val getWeight = DensityScan.getWeight(sft, weight)
    val writeGeom = DensityScan.writeGeometry(sft, grid)
    try {
      results.foreach { row =>
        val vis = VisibilityAdapter.readFromRow(row)
        if ((vis == null || VisibilityEvaluator.parse(vis).evaluate(auths)) &&
            { feature.setRowResult(row); ecql.forall(_.evaluate(feature)) }) {
          writeGeom(feature, getWeight(feature), result)
        }
      }
    } finally {
      results.close()
    }

    val sf = new ScalaSimpleFeature(DensityScan.DensitySft, "", Array(GeometryUtils.zeroPoint))
    // Return value in user data so it's preserved when passed through a RetypingFeatureCollection
    sf.getUserData.put(DensityScan.DensityValueKey, DensityScan.encodeResult(result))
    CloseableIterator(Iterator.single(sf))
  }

  override def toString: String =
    s"DensityAdapter(sft=${sft.getTypeName}{${SimpleFeatureTypes.encodeType(sft)}}, " +
        s"filter=${ecql.map(filterToString).getOrElse("INCLUDE")}, " +
        s"envelope=[${envelope.getMinX},${envelope.getMinY},${envelope.getMaxX},${envelope.getMaxY}]" +
        s"width=$width, height=$height, weight=${weight.getOrElse("1.0")}, " +
        s"auths=${auths.map(new String(_, StandardCharsets.UTF_8)).mkString(",")})"
}

object DensityAdapter extends KuduResultAdapterSerialization[DensityAdapter] {

  override def serialize(adapter: DensityAdapter, bb: ExpandingByteBuffer): Unit = {
    bb.putString(adapter.sft.getTypeName)
    bb.putString(SimpleFeatureTypes.encodeType(adapter.sft, includeUserData = true))
    bb.putInt(adapter.auths.length)
    adapter.auths.foreach(bb.putBytes)
    bb.putString(adapter.ecql.map(ECQL.toCQL).orNull)
    bb.putDouble(adapter.envelope.getMinX)
    bb.putDouble(adapter.envelope.getMaxX)
    bb.putDouble(adapter.envelope.getMinY)
    bb.putDouble(adapter.envelope.getMaxY)
    bb.putInt(adapter.width)
    bb.putInt(adapter.height)
    bb.putString(adapter.weight.orNull)
  }

  override def deserialize(bb: ByteBuffer): DensityAdapter = {
    import org.locationtech.geomesa.utils.io.ByteBuffers.RichByteBuffer

    val sft = SimpleFeatureTypes.createType(bb.getString, bb.getString)
    val auths = Seq.fill(bb.getInt)(bb.getBytes)
    val ecql = Option(bb.getString).map(FastFilterFactory.toFilter(sft, _))
    val envelope = new Envelope(bb.getDouble, bb.getDouble, bb.getDouble, bb.getDouble)
    val width = bb.getInt
    val height = bb.getInt
    val weight = Option(bb.getString)

    DensityAdapter(sft, auths, ecql, envelope, width, height, weight)
  }
}
