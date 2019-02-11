/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.id

import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ConfiguredIndex
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

class IdIndex protected (ds: GeoMesaDataStore[_], sft: SimpleFeatureType, version: Int, mode: IndexMode)
    extends GeoMesaFeatureIndex[Set[Array[Byte]], Array[Byte]](ds, sft, IdIndex.name, version, Seq.empty, mode)
        with IdFilterStrategy[Set[Array[Byte]], Array[Byte]] {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, mode: IndexMode) =
    this(ds, sft, IdIndex.version, mode)

  override val keySpace: IdIndexKeySpace = new IdIndexKeySpace(sft)

  override val tieredKeySpace: Option[IndexKeySpace[_, _]] = None
}

object IdIndex extends ConfiguredIndex {

  override val name = "id"
  override val version = 4

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    IdIndexKeySpace.supports(sft, attributes)

  override def defaults(sft: SimpleFeatureType): Seq[Seq[String]] = Seq(Seq.empty)
}
