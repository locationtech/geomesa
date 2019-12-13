/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.id.legacy

import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.LegacyTableNaming
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

class IdIndexV2 protected (ds: GeoMesaDataStore[_], sft: SimpleFeatureType, version: Int, mode: IndexMode)
    extends IdIndexV3(ds, sft, version, mode) with LegacyTableNaming[Set[Array[Byte]], Array[Byte]] {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, mode: IndexMode) =
    this(ds, sft, 2, mode)

  override protected val tableNameKey: String = s"table.records.v$version"

  override protected val fallbackTableNameKey: String = "tables.record.name"
}
