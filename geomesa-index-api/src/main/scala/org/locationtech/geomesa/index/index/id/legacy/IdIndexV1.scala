/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.id.legacy

import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.id.IdIndexKeySpace
import org.locationtech.geomesa.index.index.id.legacy.IdIndexV1.IdIndexKeySpaceV1
import org.locationtech.geomesa.index.index.id.legacy.IdIndexV3.IdIndexKeySpaceV3
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

class IdIndexV1(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, mode: IndexMode)
    extends IdIndexV2(ds, sft, 1, mode) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val serializedWithId: Boolean = true

  override val keySpace: IdIndexKeySpace = new IdIndexKeySpaceV1(sft, sft.getTableSharingBytes)
}

object IdIndexV1 {

  class IdIndexKeySpaceV1(sft: SimpleFeatureType, sharing: Array[Byte]) extends IdIndexKeySpaceV3(sft, sharing) {

    private val serializer = KryoFeatureSerializer(sft) // note: withId

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[Array[Byte]] = {
      val kv = super.toIndexKey(writable, tier, id, lenient)
      lazy val serialized = serializer.serialize(writable.feature)
      kv.copy(values = kv.values.map(_.copy(toValue = serialized)))
    }
  }
}