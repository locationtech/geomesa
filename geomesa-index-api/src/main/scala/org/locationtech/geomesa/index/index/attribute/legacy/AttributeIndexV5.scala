/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute.legacy

import java.util.Date

import org.locationtech.geomesa.index.api.IndexKeySpace
import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.DateIndexKeySpace
import org.locationtech.geomesa.index.index.z2.XZ2IndexKeySpace
import org.locationtech.geomesa.index.index.z2.legacy.Z2IndexV3.Z2IndexKeySpaceV3
import org.locationtech.geomesa.index.index.z3.XZ3IndexKeySpace
import org.locationtech.geomesa.index.index.z3.legacy.Z3IndexV4.Z3IndexKeySpaceV4
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.jts.geom.{Geometry, Point}
import org.opengis.feature.simple.SimpleFeatureType

// attribute index with legacy z-curves as secondary index scheme
class AttributeIndexV5 protected (ds: GeoMesaDataStore[_],
                                  sft: SimpleFeatureType,
                                  version: Int,
                                  attribute: String,
                                  secondaries: Seq[String],
                                  mode: IndexMode)
    extends AttributeIndexV6(ds, sft, version, attribute, secondaries, mode) {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, attribute: String, secondaries: Seq[String], mode: IndexMode) =
    this(ds, sft, 5, attribute, secondaries, mode)

  override val tieredKeySpace: Option[IndexKeySpace[_, _]] = {
    lazy val fail =
      new IllegalArgumentException(s"No key space matched tiering for attributes: ${secondaries.mkString(", ")}")
    val bindings = secondaries.map(a => (a, sft.getDescriptor(a).getType.getBinding))
    if (secondaries.isEmpty) {
      None
    } else if (secondaries.lengthCompare(1) == 0) {
      val (attribute, binding) = bindings.head
      if (classOf[Date].isAssignableFrom(binding)) {
        Some(new DateIndexKeySpace(sft, attribute))
      } else if (classOf[Point].isAssignableFrom(binding)) {
        Some(new Z2IndexKeySpaceV3(sft, Array.empty, NoShardStrategy, attribute))
      } else if (classOf[Geometry].isAssignableFrom(binding)) {
        Some(new XZ2IndexKeySpace(sft, NoShardStrategy, attribute))
      } else {
        throw fail
      }
    } else if (secondaries.lengthCompare(2) == 0) {
      val (geom, geomBinding) = bindings.head
      val (dtg, dtgBinding) = bindings.last
      if (classOf[Point].isAssignableFrom(geomBinding) && classOf[Date].isAssignableFrom(dtgBinding)) {
        Some(new Z3IndexKeySpaceV4(sft, Array.empty, NoShardStrategy, geom, dtg))
      } else if (classOf[Geometry].isAssignableFrom(geomBinding) && classOf[Date].isAssignableFrom(dtgBinding)) {
        Some(new XZ3IndexKeySpace(sft, NoShardStrategy, geom, dtg))
      } else {
        throw fail
      }
    } else {
      throw fail
    }
  }

}
