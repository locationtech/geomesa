/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.s3

import org.locationtech.geomesa.index.api.ShardStrategy.ZShardStrategy
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ConfiguredIndex
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

/**
  * @author sunyabo 2019年08月01日 09:25
  * @version V1.0
  */
class S3Index protected (ds: GeoMesaDataStore[_],
                         sft: SimpleFeatureType,
                         version: Int,
                         val geom: String,
                         val dtg: String,
                         mode: IndexMode)
  extends GeoMesaFeatureIndex[S3IndexValues, S3IndexKey](ds, sft, S3Index.name, version, Seq(geom, dtg), mode)
    with SpatioTemporalFilterStrategy[S3IndexValues, S3IndexKey] {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, geomField: String, dtgField: String, mode: IndexMode) =
    this(ds, sft, S3Index.version, geomField, dtgField, mode)

  /**
    * Primary key space used by this index
    *
    * @return
    */
  override def keySpace: IndexKeySpace[S3IndexValues, S3IndexKey] = new S3IndexKeySpace(sft, ZShardStrategy(sft), geom, dtg)

  /**
    * Tiered key space beyond the primary one, if any
    *
    * @return
    */
  override def tieredKeySpace: Option[IndexKeySpace[_, _]] = None
}

object S3Index extends ConfiguredIndex {

  override val name = "s3"
  override val version = 1

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    S3IndexKeySpace.supports(sft, attributes)

  override def defaults(sft: SimpleFeatureType): Seq[Seq[String]] = Seq.empty
  }
}
