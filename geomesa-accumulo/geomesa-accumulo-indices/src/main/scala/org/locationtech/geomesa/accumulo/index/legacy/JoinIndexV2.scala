/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy

import org.locationtech.geomesa.accumulo.index.AttributeJoinIndex
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.geotools.api.feature.simple.SimpleFeatureType
// noinspection ScalaDeprecation
import org.locationtech.geomesa.features.kryo.serialization.IndexValueSerializer.IndexValueEncoderImpl
import org.locationtech.geomesa.index.api.{RowKeyValue, WritableFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.legacy.AttributeIndexV2
import org.locationtech.geomesa.index.index.attribute.legacy.AttributeIndexV2.AttributeIndexKeySpaceV2
import org.locationtech.geomesa.index.index.attribute.{AttributeIndexKey, AttributeIndexKeySpace}

class JoinIndexV2(ds: GeoMesaDataStore[_],
                  sft: SimpleFeatureType,
                  attribute: String,
                  dtg: Option[String],
                  mode: IndexMode)
    extends AttributeIndexV2(ds, sft, attribute, dtg, mode) with AttributeJoinIndex {

  override val keySpace: AttributeIndexKeySpace = new AttributeIndexKeySpaceV2(sft, attribute) {

    // noinspection ScalaDeprecation
    private val serializer = new IndexValueEncoderImpl(sft)

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[AttributeIndexKey] = {
      val kv = super.toIndexKey(writable, tier, id, lenient)
      lazy val serialized = serializer.serialize(writable.feature)
      kv.copy(values = kv.values.map(_.copy(cf = Array.empty, cq = Array.empty, toValue = serialized)))
    }
  }
}
