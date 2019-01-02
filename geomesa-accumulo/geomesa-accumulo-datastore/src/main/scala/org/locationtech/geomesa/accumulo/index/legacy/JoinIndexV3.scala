/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy

import org.locationtech.geomesa.accumulo.data.AccumuloWritableFeature
import org.locationtech.geomesa.accumulo.index.AccumuloJoinIndex
import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.api.{RowKeyValue, WritableFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.legacy.AttributeIndexV3
import org.locationtech.geomesa.index.index.attribute.legacy.AttributeIndexV7.AttributeIndexKeySpaceV7
import org.locationtech.geomesa.index.index.attribute.{AttributeIndexKey, AttributeIndexKeySpace}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

class JoinIndexV3(ds: GeoMesaDataStore[_],
                  sft: SimpleFeatureType,
                  attribute: String,
                  dtg: Option[String],
                  mode: IndexMode)
    extends AttributeIndexV3(ds, sft, attribute, dtg, mode) with AccumuloJoinIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val keySpace: AttributeIndexKeySpace =
    new AttributeIndexKeySpaceV7(sft, sft.getTableSharingBytes, NoShardStrategy, attribute) {
      override def toIndexKey(writable: WritableFeature,
                              tier: Array[Byte],
                              id: Array[Byte],
                              lenient: Boolean): RowKeyValue[AttributeIndexKey] = {
        val kv = super.toIndexKey(writable, tier, id, lenient)
        kv.copy(values = writable.asInstanceOf[AccumuloWritableFeature].indexValues)
      }
    }
}
