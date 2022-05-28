/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.filters

import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.hbase.rpc.filter.CqlTransformFilter
import org.locationtech.geomesa.hbase.rpc.filter.CqlTransformFilter.{DelegateFilter, FilterDelegate, FilterTransformDelegate, NullFeatureIndex, TransformDelegate, deserialize}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.apache.hadoop.hbase.util.Bytes
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer

@deprecated("used for filter compatibility mode 2.3")
class JSimpleFeatureFilter(delegate: DelegateFilter) extends CqlTransformFilter(delegate) {

  override def toByteArray: Array[Byte] = {
    val sftString = SimpleFeatureTypes.encodeType(delegate.sft, includeUserData = true)
    val filterString = delegate.filter.map(ECQL.toCQL).orNull
    val transform = delegate.transform.map(_._1).orNull
    val transformSchema = delegate.transform.map { case (_, v) =>
      SimpleFeatureTypes.encodeType(v, includeUserData = true)
    }.orNull

    def getLengthArray(s: String): Array[Byte] = {
      if (s == null || s.isEmpty) { Bytes.toBytes(0) } else {
        Bytes.add(Bytes.toBytes(s.length), s.getBytes)
      }
    }

    val arrays =
      Array(
        getLengthArray(sftString),
        getLengthArray(filterString),
        getLengthArray(transform),
        getLengthArray(transformSchema)
      )
    Bytes.add(arrays)
  }
}

object JSimpleFeatureFilter {
  @throws(classOf[DeserializationException])
  def parseFrom(pbBytes: Array[Byte]): org.apache.hadoop.hbase.filter.Filter = {

    val sftLen = Bytes.readAsInt(pbBytes, 0, 4)
    val sft = SimpleFeatureTypes.createType("", new String(pbBytes, 4, sftLen))

    val filterLen = Bytes.readAsInt(pbBytes, sftLen + 4, 4)
    val filterString = new String(pbBytes, sftLen + 8, filterLen)

    val transformLen = Bytes.readAsInt(pbBytes, sftLen + filterLen + 8, 4)
    val transformString = new String(pbBytes, sftLen + filterLen + 12, transformLen)

    val transformSchemaLen = Bytes.readAsInt(pbBytes, sftLen + filterLen + transformLen + 12, 4)
    val transformSchemaString =
      new String(pbBytes, sftLen + filterLen + transformLen + 16, transformSchemaLen)

    // we do just enough here to be able to serialize the filter again
    // this should only be invoked on the client, where the filter won't be used
    lazy val feature = {
      val f = KryoFeatureSerializer(sft, SerializationOptions.withoutId).getReusableFeature
      f.setTransforms(transformString, SimpleFeatureTypes.createType("", transformSchemaString))
      f
    }

    val delegate = if (filterLen == 0) {
      new TransformDelegate(sft, NullFeatureIndex, feature, None)
    } else if (transformLen == 0) {
      new FilterDelegate(sft, NullFeatureIndex, null, ECQL.toFilter(filterString), None)
    } else {
      new FilterTransformDelegate(sft, NullFeatureIndex, feature, ECQL.toFilter(filterString), None)
    }
    new JSimpleFeatureFilter(delegate)
  }
}
