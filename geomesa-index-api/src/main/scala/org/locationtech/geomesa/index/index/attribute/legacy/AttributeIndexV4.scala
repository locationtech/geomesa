/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute.legacy

import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKeySpace
import org.locationtech.geomesa.index.index.attribute.legacy.AttributeIndexV7.AttributeIndexKeySpaceV7
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// no shards
class AttributeIndexV4 protected (ds: GeoMesaDataStore[_],
                                  sft: SimpleFeatureType,
                                  version: Int,
                                  attribute: String,
                                  secondaries: Seq[String],
                                  mode: IndexMode)
    extends AttributeIndexV5(ds, sft, version, attribute, secondaries, mode) {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, attribute: String, secondaries: Seq[String], mode: IndexMode) =
    this(ds, sft, 4, attribute, secondaries, mode)

  override val keySpace: AttributeIndexKeySpace =
    new AttributeIndexKeySpaceV7(sft, sft.getTableSharingBytes, NoShardStrategy, attribute)
}
