/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.index
package z2

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.api.ShardStrategy.Z2ShardStrategy
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode

class XZ2Index protected (ds: GeoMesaDataStore[_], sft: SimpleFeatureType, version: Int, geom: String, mode: IndexMode)
    extends GeoMesaFeatureIndex[XZ2IndexValues, Long](ds, sft, XZ2Index.name, version, Seq(geom), mode)
        with SpatialFilterStrategy[XZ2IndexValues, Long]
        with SpatialIndex[XZ2IndexValues, Long] {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, mode: IndexMode) =
    this(ds, sft, XZ2Index.version, geom, mode)

  override val keySpace: XZ2IndexKeySpace = new XZ2IndexKeySpace(sft, Z2ShardStrategy(sft), geom)

  override val tieredKeySpace: Option[IndexKeySpace[_, _]] = None
}

object XZ2Index extends ConfiguredIndex {

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name = "xz2"
  override val version = 2

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    XZ2IndexKeySpace.supports(sft, attributes)

  override def defaults(sft: SimpleFeatureType): Seq[IndexId] =
    if (sft.nonPoints) { Seq(IndexId(name, version, Seq(sft.getGeomField))) } else { Seq.empty }

  override def defaults(sft: SimpleFeatureType, primary: AttributeDescriptor): Option[IndexId] =
    if (primary.isGeometryWithExtents) { Some(IndexId(name, version, Seq(primary.getLocalName))) } else { None }
}
