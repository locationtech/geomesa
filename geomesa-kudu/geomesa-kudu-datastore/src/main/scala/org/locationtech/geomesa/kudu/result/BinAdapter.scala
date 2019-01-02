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
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.{FilterHelper, filterToString}
import org.locationtech.geomesa.kudu.result.KuduResultAdapter.KuduResultAdapterSerialization
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, UnusedFeatureIdAdapter, VisibilityAdapter}
import org.locationtech.geomesa.kudu.schema.{KuduSimpleFeatureSchema, RowResultSimpleFeature}
import org.locationtech.geomesa.security.VisibilityEvaluator
import org.locationtech.geomesa.utils.bin.BinaryEncodeCallback.ByteArrayCallback
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.ByteBuffers.ExpandingByteBuffer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Converts rows to bin format
  *
  * @param sft simple feature type
  * @param auths authorizations
  * @param ecql filter
  * @param trackId track id field to encode
  * @param geom geometry field to encode
  * @param dtg date field to encode
  * @param label label field to encode
  * @param batchSize batch size
  * @param sorting sort
  */
case class BinAdapter(sft: SimpleFeatureType,
                      auths: Seq[Array[Byte]],
                      ecql: Option[Filter],
                      trackId: Option[String],
                      geom: Option[String],
                      dtg: Option[String],
                      label: Option[String],
                      batchSize: Int,
                      sorting: Boolean) extends KuduResultAdapter {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  require(!sorting, "BIN sorting not supported") // TODO

  private val requiresFid = trackId.isEmpty || ecql.exists(FilterHelper.hasIdFilter)

  // determine all the attributes that we need to be able to evaluate the transform and filter
  private val attributes = {
    val fromBin = Seq(Some(geom.getOrElse(sft.getGeomField)), trackId, dtg.orElse(sft.getDtgField), label).flatten
    val fromFilter = ecql.map(FilterHelper.propertyNames(_, sft)).getOrElse(Seq.empty)
    (fromBin ++ fromFilter).distinct
  }

  private val schema = KuduSimpleFeatureSchema(sft)
  private val featureIdAdapter = if (requiresFid) { FeatureIdAdapter } else { UnusedFeatureIdAdapter }
  private val feature = new RowResultSimpleFeature(sft, featureIdAdapter, schema.adapters)

  override val columns: Seq[String] =
    if (requiresFid) { Seq(FeatureIdAdapter.name, VisibilityAdapter.name) } else { Seq(VisibilityAdapter.name) } ++
        schema.schema(attributes).map(_.getName)

  private val encoder = {
    val opts = EncodingOptions(geom.map(sft.indexOf), dtg.map(sft.indexOf), trackId.map(sft.indexOf), label.map(sft.indexOf))
    BinaryOutputEncoder(sft, opts)
  }

  override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
    val sf = new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, "", Array(null, GeometryUtils.zeroPoint))

    val iter = new Iterator[SimpleFeature] {
      // we may end up with an empty byte array if all the results are filtered, but it shouldn't matter
      override def hasNext: Boolean = results.hasNext
      override def next(): SimpleFeature = {
        var i = 0
        while (i < batchSize && results.hasNext) {
          val row = results.next
          val vis = VisibilityAdapter.readFromRow(row)
          if ((vis == null || VisibilityEvaluator.parse(vis).evaluate(auths)) &&
              { feature.setRowResult(row); ecql.forall(_.evaluate(feature)) }) {
            encoder.encode(feature, ByteArrayCallback)
            i += 1
          }
        }
        sf.setAttribute(BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX, ByteArrayCallback.result)
        sf
      }
    }

    CloseableIterator(iter, results.close())
  }

  override def toString: String =
    s"BinAdapter(sft=${sft.getTypeName}{${SimpleFeatureTypes.encodeType(sft)}}, " +
        s"filter=${ecql.map(filterToString).getOrElse("INCLUDE")}, trackId=$trackId, " +
        s"geom=$geom, dtg=$dtg, label=$label, batchSize=$batchSize, sorting=$sorting, " +
        s"auths=${auths.map(new String(_, StandardCharsets.UTF_8)).mkString(",")})"
}

object BinAdapter extends KuduResultAdapterSerialization[BinAdapter] {

  override def serialize(adapter: BinAdapter, bb: ExpandingByteBuffer): Unit = {
    bb.putString(adapter.sft.getTypeName)
    bb.putString(SimpleFeatureTypes.encodeType(adapter.sft, includeUserData = true))
    bb.putInt(adapter.auths.length)
    adapter.auths.foreach(bb.putBytes)
    bb.putString(adapter.ecql.map(ECQL.toCQL).orNull)
    bb.putString(adapter.trackId.orNull)
    bb.putString(adapter.geom.orNull)
    bb.putString(adapter.dtg.orNull)
    bb.putString(adapter.label.orNull)
    bb.putInt(adapter.batchSize)
    bb.putBool(adapter.sorting)
  }

  override def deserialize(bb: ByteBuffer): BinAdapter = {
    import org.locationtech.geomesa.utils.io.ByteBuffers.RichByteBuffer

    val sft = SimpleFeatureTypes.createType(bb.getString, bb.getString)
    val auths = Seq.fill(bb.getInt)(bb.getBytes)
    val ecql = Option(bb.getString).map(FastFilterFactory.toFilter(sft, _))
    val trackId = Option(bb.getString)
    val geom = Option(bb.getString)
    val dtg = Option(bb.getString)
    val label = Option(bb.getString)
    val batchSize = bb.getInt
    val sorting = bb.getBool

    BinAdapter(sft, auths, ecql, trackId, geom, dtg, label, batchSize, sorting)
  }
}
