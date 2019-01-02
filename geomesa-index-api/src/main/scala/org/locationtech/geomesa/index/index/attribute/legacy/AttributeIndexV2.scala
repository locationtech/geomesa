/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute.legacy

import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.api.{RowKeyValue, WritableFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.legacy.AttributeIndexV2.AttributeIndexKeySpaceV2
import org.locationtech.geomesa.index.index.attribute.legacy.AttributeIndexV7.AttributeIndexKeySpaceV7
import org.locationtech.geomesa.index.index.attribute.{AttributeIndexKey, AttributeIndexKeySpace}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// value serialized with id
class AttributeIndexV2(ds: GeoMesaDataStore[_],
                       sft: SimpleFeatureType,
                       attribute: String,
                       dtg: Option[String],
                       mode: IndexMode) extends AttributeIndexV3(ds, sft, 2, attribute, dtg, mode) {

  override val serializedWithId: Boolean = true

  override val keySpace: AttributeIndexKeySpace = new AttributeIndexKeySpaceV2(sft, attribute)
}

object AttributeIndexV2 {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  class AttributeIndexKeySpaceV2(sft: SimpleFeatureType, attributeField: String)
      extends AttributeIndexKeySpaceV7(sft, sft.getTableSharingBytes, NoShardStrategy, attributeField) {

    private val serializer = KryoFeatureSerializer(sft) // note: withId

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
