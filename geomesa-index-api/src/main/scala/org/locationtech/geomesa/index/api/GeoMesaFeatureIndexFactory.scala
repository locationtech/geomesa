/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import java.util.ServiceLoader

import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.DefaultFeatureIndexFactory
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.ENABLED_INDICES
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Factory for feature index implementations
  */
trait GeoMesaFeatureIndexFactory {

  /**
    * Default indices for a feature type, based on user data hints and the schema attributes
    *
    * @param sft simple feature type
    * @return
    */
  def indices(sft: SimpleFeatureType, hint: Option[String] = None): Seq[IndexId]

  /**
    * Gets the names and versions of available indices, based on the schema attributes. Can be used with
    * 'geomesa.indices.enabled' to specify default indices of the given type
    *
    * @param sft simple feature type
    * @return
    */
  def available(sft: SimpleFeatureType): Seq[(String, Int)]

  /**
    * Create an index instance
    *
    * @param ds data store
    * @param sft simple feature type
    * @param index index identifier
    * @tparam T index filter values binding
    * @tparam U index key binding
    * @return
    */
  def create[T, U](ds: GeoMesaDataStore[_], sft: SimpleFeatureType, index: IndexId): Option[GeoMesaFeatureIndex[T, U]]
}

object GeoMesaFeatureIndexFactory {

  import scala.collection.JavaConverters._

  private val factories =
    ServiceLoader.load(classOf[GeoMesaFeatureIndexFactory]).asScala.toList :+ DefaultFeatureIndexFactory

  /**
    * Indices for a feature type, based on user data hints and the schema attributes
    *
    * @param sft simple feature type
    * @return
    */
  def indices(sft: SimpleFeatureType): Seq[IndexId] = {
    Option(sft.getUserData.get(ENABLED_INDICES).asInstanceOf[String]).filter(_.length > 0) match {
      case None => factories.flatMap(_.indices(sft)).distinct
      case Some(enabled) =>
        enabled.split(",").flatMap { hint =>
          val ids = factories.flatMap(_.indices(sft, Some(hint)))
          if (ids.isEmpty) {
            throw new IllegalArgumentException(s"Configured index '$hint' does not exist or does not support " +
                s"the schema ${SimpleFeatureTypes.encodeType(sft)}")
          }
          ids
        }
    }
  }

  /**
    * Gets the names of available indices, based on the schema attributes. Can be used with 'geomesa.indices.enabled'
    * to specify default indices of the given type
    *
    * @param sft simple feature type
    * @return
    */
  def available(sft: SimpleFeatureType): Seq[(String, Int)] = factories.flatMap(_.available(sft)).distinct

  /**
    * Create an index instance
    *
    * @param ds data store
    * @param sft simple feature type
    * @param indices index identifiers
    * @return
    */
  def create(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, indices: Seq[IndexId]): Seq[GeoMesaFeatureIndex[_, _]] = {
    indices.map { index =>
      factories.flatMap(_.create(ds, sft, index).toSeq) match {
        case Seq(i) => i
        case Nil    => throw new IllegalArgumentException(s"No index found using identifier '$index'")
        case _      => throw new IllegalArgumentException(s"Multiple indices found using identifier '$index'")
      }
    }
  }
}
