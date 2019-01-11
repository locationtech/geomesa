/***********************************************************************
 * Copyright (c) 2017-2019 IBM
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.SimpleFeatureSerializer
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class CassandraFeature(val feature: SimpleFeature,
                       serializer: SimpleFeatureSerializer,
                       idSerializer: (String) => Array[Byte]) extends WrappedFeature {
  lazy val fullValue: Array[Byte] = serializer.serialize(feature)

  override lazy val idBytes: Array[Byte] = idSerializer.apply(feature.getID)
}

object CassandraFeature {

  def wrapper(sft: SimpleFeatureType): (SimpleFeature) => CassandraFeature = {
    val serializer = KryoFeatureSerializer(sft, SerializationOptions.withoutId)
    val idSerializer = GeoMesaFeatureIndex.idToBytes(sft)
    (feature) => new CassandraFeature(feature, serializer, idSerializer)
  }
}
