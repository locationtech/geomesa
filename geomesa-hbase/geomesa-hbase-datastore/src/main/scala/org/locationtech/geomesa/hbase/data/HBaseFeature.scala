/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.security.visibility.CellVisibility
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.hbase.data.HBaseFeature.RowValue
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.security.SecurityUtils.FEATURE_VISIBILITY
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class HBaseFeature(val feature: SimpleFeature,
                   serializer: SimpleFeatureSerializer,
                   idSerializer: (String) => Array[Byte]) extends WrappedFeature {

  import HBaseFeatureIndex.{DataColumnFamily, DataColumnQualifier}
  import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature

  lazy val fullValue = new RowValue(DataColumnFamily, DataColumnQualifier, visibility, serializer.serialize(feature))

  override lazy val idBytes: Array[Byte] = idSerializer.apply(feature.getID)

  private lazy val visibility =
    feature.userData[String](FEATURE_VISIBILITY).map(new CellVisibility(_))
}

object HBaseFeature {

  def wrapper(sft: SimpleFeatureType): (SimpleFeature) => HBaseFeature = {
    val serializer = KryoFeatureSerializer(sft, SerializationOptions.withoutId)
    val idSerializer = GeoMesaFeatureIndex.idToBytes(sft)
    (feature) => new HBaseFeature(feature, serializer, idSerializer)
  }

  class RowValue(val cf: Array[Byte], val cq: Array[Byte], val vis: Option[CellVisibility], toValue: => Array[Byte]) {
    lazy val value: Array[Byte] = toValue
  }
}

