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
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.kudu.result.KuduResultAdapter.KuduResultAdapterSerialization
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.{FeatureIdAdapter, VisibilityAdapter}
import org.locationtech.geomesa.kudu.schema.{KuduSimpleFeatureSchema, RowResultSimpleFeature}
import org.locationtech.geomesa.security.{SecurityUtils, VisibilityEvaluator}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.ByteBuffers.ExpandingByteBuffer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.{Expression, PropertyName}

/**
  * Converts rows into simple features, with transforming
  *
  * @param sft simple feature type
  * @param auths authorizations
  * @param tsft transform simple feature type
  * @param tdefs transform definitions
  */
case class TransformAdapter(sft: SimpleFeatureType,
                            auths: Seq[Array[Byte]],
                            tsft: SimpleFeatureType,
                            tdefs: String) extends KuduResultAdapter {

  import scala.collection.JavaConverters._

  // determine all the attributes that we need to be able to evaluate the transform
  private val attributes = TransformProcess.toDefinition(tdefs).asScala.map(_.expression).flatMap {
    case p: PropertyName => Seq(p.getPropertyName)
    case e: Expression   => DataUtilities.attributeNames(e, sft)
  }.distinct

  private val schema = KuduSimpleFeatureSchema(sft)
  private val feature = new RowResultSimpleFeature(sft, FeatureIdAdapter, schema.adapters)
  private val transformFeature = TransformSimpleFeature(sft, tsft, tdefs)
  transformFeature.setFeature(feature)

  override val columns: Seq[String] =
    Seq(FeatureIdAdapter.name, VisibilityAdapter.name) ++ schema.schema(attributes).map(_.getName)

  override def adapt(results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
    results.flatMap { row =>
      val vis = VisibilityAdapter.readFromRow(row)
      if (vis == null || VisibilityEvaluator.parse(vis).evaluate(auths)) {
        feature.setRowResult(row)
        SecurityUtils.setFeatureVisibility(feature, vis)
        Iterator.single(transformFeature)
      } else {
        CloseableIterator.empty
      }
    }
  }

  override def toString: String =
    s"TransformAdapter(sft=${sft.getTypeName}{${SimpleFeatureTypes.encodeType(sft)}}, " +
        s"transform=$tdefs, auths=${auths.map(new String(_, StandardCharsets.UTF_8)).mkString(",")})"
}

object TransformAdapter extends KuduResultAdapterSerialization[TransformAdapter] {
  override def serialize(adapter: TransformAdapter, bb: ExpandingByteBuffer): Unit = {
    bb.putString(adapter.sft.getTypeName)
    bb.putString(SimpleFeatureTypes.encodeType(adapter.sft, includeUserData = true))
    bb.putInt(adapter.auths.length)
    adapter.auths.foreach(bb.putBytes)
    bb.putString(SimpleFeatureTypes.encodeType(adapter.tsft, includeUserData = true))
    bb.putString(adapter.tdefs)
  }

  override def deserialize(bb: ByteBuffer): TransformAdapter = {
    import org.locationtech.geomesa.utils.io.ByteBuffers.RichByteBuffer

    val sft = SimpleFeatureTypes.createType(bb.getString, bb.getString)
    val auths = Seq.fill(bb.getInt)(bb.getBytes)
    val tsft = SimpleFeatureTypes.createType(sft.getTypeName, bb.getString)
    val tdefs = bb.getString
    TransformAdapter(sft, auths, tsft, tdefs)
  }
}
