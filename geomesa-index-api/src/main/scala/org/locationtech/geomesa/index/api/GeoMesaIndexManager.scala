/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

/**
  * Manages available indices and versions. @see GeoMesaFeatureIndex
  */
trait GeoMesaIndexManager[O <: GeoMesaDataStore[O, F, W], F <: WrappedFeature, W] {

  private lazy val indexMap = AllIndices.map(i => (i.name, i.version) -> i).toMap

  // note: keep in priority order for running full table scans
  // also consider the order of feature validation in
  // org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
  def AllIndices: Seq[GeoMesaFeatureIndex[O, F, W]]

  def CurrentIndices: Seq[GeoMesaFeatureIndex[O, F, W]]

  def lookup: Map[(String, Int), GeoMesaFeatureIndex[O, F, W]] = indexMap

  def indices(sft: SimpleFeatureType,
              idx: Option[String] = None,
              mode: IndexMode = IndexMode.Any): Seq[GeoMesaFeatureIndex[O, F, W]] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val withMode = sft.getIndices.filter { case (_, _, m) => m.supports(mode) }
    // filter the list of all indices so that we maintain priority order
    val filtered = AllIndices.filter(i => withMode.exists { case (n, v, _) => i.name == n && i.version == v})
    idx match {
      case None => filtered
      case Some(i) if i.indexOf(":") == -1 => filtered.filter(_.name.equalsIgnoreCase(i))
      case Some(i) => filtered.filter(_.identifier.equalsIgnoreCase(i))
    }
  }

  def index(identifier: String): GeoMesaFeatureIndex[O, F, W] = {
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
    val enabled = Option(sft.getUserData.remove(SimpleFeatureTypes.Configs.ENABLED_INDICES)).filter(_.toString.length > 0)
    val indices: Seq[(String, Int, IndexMode)] = enabled match {
      case None => CurrentIndices.collect { case i if i.supports(sft) => (i.name, i.version, IndexMode.ReadWrite) }
      case Some(e) =>
        e.toString.split(",").map(_.trim).map { name =>
          val index = CurrentIndices.find(_.name.equalsIgnoreCase(name)).orElse(Try(this.index(name)).toOption).getOrElse {
            throw new IllegalArgumentException(s"Configured index '$name' does not exist")
          }
          if (index.supports(sft)) {
            (index.name, index.version, IndexMode.ReadWrite)
          } else {
            throw new IllegalArgumentException(s"Configured index '$name' does not support the " +
                s"schema ${SimpleFeatureTypes.encodeType(sft)}")
          }
        }

    }
    if (indices.isEmpty) {
      throw new IllegalArgumentException("There are no available indices that support the schema " +
          s"${SimpleFeatureTypes.encodeType(sft)}")
    }
    sft.setIndices(indices)
  }
}
