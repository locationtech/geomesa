/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.index.id

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, IndexKeySpace}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.ConfiguredIndex
import org.locationtech.geomesa.index.strategies.IdFilterStrategy
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode

class IdIndex protected (ds: GeoMesaDataStore[_], sft: SimpleFeatureType, version: Int, mode: IndexMode)
    extends GeoMesaFeatureIndex[Set[Array[Byte]], Array[Byte]](ds, sft, IdIndex.name, version, Seq.empty, mode)
        with IdFilterStrategy[Set[Array[Byte]], Array[Byte]] {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, mode: IndexMode) =
    this(ds, sft, IdIndex.version, mode)

  override val keySpace: IdIndexKeySpace = new IdIndexKeySpace(sft)

  override val tieredKeySpace: Option[IndexKeySpace[_, _]] = None

  override def toString: String = getClass.getSimpleName
}

object IdIndex extends ConfiguredIndex {

  import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.EnableFidIndex

  override val name = "id"
  override val version = 4

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    IdIndexKeySpace.supports(sft, attributes)

  override def defaultIndicesFor(sft: SimpleFeatureType): Seq[IndexId] = {
    if (Option(sft.getUserData.get(EnableFidIndex)).exists(_.toString.equalsIgnoreCase("false"))) {
      Seq.empty
    } else {
      Seq(IndexId(name, version, Seq.empty))
    }
  }

  override def indexFor(sft: SimpleFeatureType, primary: AttributeDescriptor): Option[IndexId] = None
}
