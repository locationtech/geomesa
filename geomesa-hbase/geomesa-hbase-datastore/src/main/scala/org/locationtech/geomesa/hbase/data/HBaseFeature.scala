/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.security.visibility.CellVisibility
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.hbase.data.HBaseFeature.RowValue
import org.locationtech.geomesa.hbase.index.HBaseColumnGroups
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.security.SecurityUtils.FEATURE_VISIBILITY
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class HBaseFeature(val feature: SimpleFeature,
                   serializers: Seq[(Array[Byte], SimpleFeatureSerializer)],
                   idSerializer: (String) => Array[Byte]) extends WrappedFeature {

  import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature

  lazy val values: Seq[RowValue] = serializers.map { case (colFamily, serializer) =>
    new RowValue(colFamily, HBaseColumnGroups.default, serializer.serialize(feature))
  }

  lazy val visibility: Option[CellVisibility] = feature.userData[String](FEATURE_VISIBILITY).map(new CellVisibility(_))

  override lazy val idBytes: Array[Byte] = idSerializer.apply(feature.getID)
}

object HBaseFeature {

  def wrapper(sft: SimpleFeatureType): (SimpleFeature) => HBaseFeature = {
    val serializers = HBaseColumnGroups.serializers(sft)
    val idSerializer = GeoMesaFeatureIndex.idToBytes(sft)
    (feature) => new HBaseFeature(feature, serializers, idSerializer)
  }

  class RowValue(val cf: Array[Byte], val cq: Array[Byte], toValue: => Array[Byte]) {
    lazy val value: Array[Byte] = toValue
  }
}

