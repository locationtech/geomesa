/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z2

import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ConfiguredIndex
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

class Z2Index protected (ds: GeoMesaDataStore[_], sft: SimpleFeatureType, version: Int, val geom: String, mode: IndexMode)
    extends GeoMesaFeatureIndex[Z2IndexValues, Long](ds, sft, Z2Index.name, version, Seq(geom), mode)
        with SpatialFilterStrategy[Z2IndexValues, Long] {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, mode: IndexMode) =
    this(ds, sft, Z2Index.version, geom, mode)

  override val keySpace: Z2IndexKeySpace = new Z2IndexKeySpace(sft, ZShardStrategy(sft), geom)

  override val tieredKeySpace: Option[IndexKeySpace[_, _]] = None
}

object Z2Index extends ConfiguredIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name = "z2"
  override val version = 5

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    Z2IndexKeySpace.supports(sft, attributes)

  override def defaults(sft: SimpleFeatureType): Seq[Seq[String]] =
    if (sft.isPoints) { Seq(Seq(sft.getGeomField)) } else { Seq.empty }
}
