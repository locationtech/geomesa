/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3.legacy

import org.locationtech.geomesa.curve.{TimePeriod, Z3SFC}
import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV6.Z3IndexKeySpaceV6
import org.locationtech.geomesa.index.index.z3.{Z3Index, Z3IndexKeySpace}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// legacy yearly epoch z curve
class Z3IndexV6 protected (
    ds: GeoMesaDataStore[_],
    sft: SimpleFeatureType,
    version: Int,
    geom: String,
    dtg: String,
    mode: IndexMode
  ) extends Z3Index(ds, sft, version, geom, dtg, mode) {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, dtg: String, mode: IndexMode) =
    this(ds, sft, 6, geom, dtg, mode)

  override val keySpace: Z3IndexKeySpace = new Z3IndexKeySpaceV6(sft, ZShardStrategy(sft), geom, dtg)
}

object Z3IndexV6 {

  class Z3IndexKeySpaceV6(
      sft: SimpleFeatureType,
      sharding: ShardStrategy,
      geomField: String,
      dtgField: String
    ) extends Z3IndexKeySpace(sft, sharding, geomField, dtgField) {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // noinspection ScalaDeprecation
    override protected val sfc: Z3SFC = sft.getZ3Interval match {
      case TimePeriod.Year => new org.locationtech.geomesa.curve.LegacyYearZ3SFC()
      case p => Z3SFC(p)
    }
  }
}
