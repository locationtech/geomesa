/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.api

import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

trait GeoMesaIndexManager[O <: GeoMesaDataStore[O, F, W, Q], F <: WrappedFeature, W, Q] {

  private lazy val indexMap = AllIndices.map(i => (i.name, i.version) -> i).toMap

  // note: keep in priority order for running full table scans
  def AllIndices: Seq[GeoMesaFeatureIndex[O, F, W, Q]]

  def CurrentIndices: Seq[GeoMesaFeatureIndex[O, F, W, Q]]

  def lookup: Map[(String, Int), GeoMesaFeatureIndex[O, F, W, Q]] = indexMap

  def indices(sft: SimpleFeatureType, mode: IndexMode): Seq[GeoMesaFeatureIndex[O, F, W, Q]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val withMode = sft.getIndices.filter { case (_, _, m) => m.supports(mode) }
    // filter the list of all indices so that we maintain priority order
    AllIndices.filter(i => withMode.exists { case (n, v, _) => i.name == n && i.version == v})
  }

  def index(identifier: String): GeoMesaFeatureIndex[O, F, W, Q] = {
    val Array(n, v) = identifier.split(":")
    indexMap(n, v.toInt)
  }

  /**
    * Set index metadata in simple feature type user data.
    *
    * @param sft simple feature type
    */
  def setIndices(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    // remove the enabled indices after fetch so we don't persist them
    val enabled = Option(sft.getUserData.remove(SimpleFeatureTypes.Configs.ENABLED_INDICES))
    val indices: Seq[GeoMesaFeatureIndex[O, F, W, Q]] = enabled match {
      case None => CurrentIndices
      case Some(e) => e.toString.split(",").map(_.trim).flatMap(n => CurrentIndices.find(_.name.equalsIgnoreCase(n)))
    }
    val supported = indices.collect { case i if i.supports(sft) => (i.name, i.version, IndexMode.ReadWrite) }
    sft.setIndices(supported)
  }
}
