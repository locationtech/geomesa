/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3

import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ConfiguredIndex
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

class XZ3Index protected (ds: GeoMesaDataStore[_],
                          sft: SimpleFeatureType,
                          version: Int,
                          val geom: String,
                          val dtg: String,
                          mode: IndexMode)
    extends GeoMesaFeatureIndex[XZ3IndexValues, Z3IndexKey](ds, sft, XZ3Index.name, version, Seq(geom, dtg), mode)
        with SpatioTemporalFilterStrategy[XZ3IndexValues, Z3IndexKey] {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geomField: String, dtgField: String, mode: IndexMode) =
    this(ds, sft, XZ3Index.version, geomField, dtgField, mode)

  override val keySpace: XZ3IndexKeySpace = new XZ3IndexKeySpace(sft, ZShardStrategy(sft), geom, dtg)

  override val tieredKeySpace: Option[IndexKeySpace[_, _]] = None
}

object XZ3Index extends ConfiguredIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val name = "xz3"
  override val version = 2

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    XZ3IndexKeySpace.supports(sft, attributes)

  override def defaults(sft: SimpleFeatureType): Seq[Seq[String]] =
    if (sft.nonPoints && sft.getDtgField.isDefined) { Seq(Seq(sft.getGeomField, sft.getDtgField.get)) } else { Seq.empty }
}
