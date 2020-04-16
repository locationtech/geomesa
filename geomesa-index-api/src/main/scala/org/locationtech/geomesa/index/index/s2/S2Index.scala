/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.s2

import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ConfiguredIndex
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

/**
  * @param ds data store
  * @param sft simple feature type stored in this index
  * @param version version of the index
  * @param geom geom attribute to index
  * @param mode mode of the index (read/write/both)
  */
class S2Index protected (ds: GeoMesaDataStore[_], sft: SimpleFeatureType, version: Int, geom: String, mode: IndexMode)
    extends GeoMesaFeatureIndex[S2IndexValues, Long](ds, sft, S2Index.name, version, Seq(geom), mode)
        with SpatialFilterStrategy[S2IndexValues, Long] {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geom: String, mode: IndexMode) =
    this(ds, sft, S2Index.version, geom, mode)

  override val keySpace: S2IndexKeySpace = new S2IndexKeySpace(sft, ZShardStrategy(sft), geom)

  override val tieredKeySpace: Option[IndexKeySpace[_, _]] = None
}

object S2Index extends ConfiguredIndex {

  override val name = "s2"
  override val version = 1

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    S2IndexKeySpace.supports(sft, attributes)

  override def defaults(sft: SimpleFeatureType): Seq[Seq[String]] = Seq.empty
}
