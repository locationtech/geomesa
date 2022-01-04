/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3.legacy

import org.locationtech.geomesa.curve.{TimePeriod, XZ3SFC}
import org.locationtech.geomesa.index.api.ShardStrategy
import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z3.legacy.XZ3IndexV2.XZ3IndexKeySpaceV2
import org.locationtech.geomesa.index.index.z3.{XZ3Index, XZ3IndexKeySpace}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// legacy yearly epoch z curve
class XZ3IndexV2 protected (
    ds: GeoMesaDataStore[_],
    sft: SimpleFeatureType,
    version: Int,
    geom: String,
    dtg: String,
    mode: IndexMode
  ) extends XZ3Index(ds, sft, version, geom, dtg, mode) {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, dtg: String, mode: IndexMode) =
    this(ds, sft, 2, geom, dtg, mode)

  override val keySpace: XZ3IndexKeySpace = new XZ3IndexKeySpaceV2(sft, ZShardStrategy(sft), geom, dtg)
}

object XZ3IndexV2 {

  class XZ3IndexKeySpaceV2(
      sft: SimpleFeatureType,
      sharding: ShardStrategy,
      geomField: String,
      dtgField: String
    ) extends XZ3IndexKeySpace(sft, sharding, geomField, dtgField) {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // noinspection ScalaDeprecation
    override protected val sfc: XZ3SFC = sft.getZ3Interval match {
      case TimePeriod.Year => new org.locationtech.geomesa.curve.LegacyYearXZ3SFC(sft.getXZPrecision)
      case p => XZ3SFC(sft.getXZPrecision, p)
    }
  }
}
