/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import java.time.ZonedDateTime

import org.locationtech.geomesa.filter.{Bounds, FilterValues}
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.opengis.feature.simple.SimpleFeatureType

package object index {

  trait NamedIndex {

    /**
      * The name used to identify the index
      */
    def name: String

    /**
      * Current version of the index
      *
      * @return
      */
    def version: Int
  }

  /**
    * Trait for helping to determine the attributes for an index based on the simple feature type
    */
  trait ConfiguredIndex extends NamedIndex {

    /**
      * Supports the specified attributes for the schema
      *
      * @param sft simple feature type
      * @param attributes attributes to index
      * @return
      */
    def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean

    /**
      * Gets the default attributes that could be used with this index
      *
      * @param sft simple feature type
      * @return groups of attributes that could be used with this index
      */
    def defaults(sft: SimpleFeatureType): Seq[Seq[String]]
  }

  /**
   * Marker trait for spatial indices
   */
  trait SpatialIndex[T <: SpatialIndexValues, U] extends GeoMesaFeatureIndex[T, U]

  /**
   * Index values with a spatial component
   */
  trait SpatialIndexValues {
    def spatialBounds: Seq[(Double, Double, Double, Double)]
  }

  /**
   * Marker trait for temporal indices
   */
  trait TemporalIndex[T <: TemporalIndexValues, U] extends GeoMesaFeatureIndex[T, U]

  /**
   * Index values with a temporal component
   */
  trait TemporalIndexValues {
    def intervals: FilterValues[Bounds[ZonedDateTime]]
  }

  /**
   * Marker trait for spatio-temporal indices
   */
  trait SpatioTemporalIndex[T <: SpatialIndexValues with TemporalIndexValues, U]
      extends SpatialIndex[T, U] with TemporalIndex[T, U]

  trait LegacyTableNaming[T, U] extends GeoMesaFeatureIndex[T, U] {

    protected val fallbackTableNameKey: String = s"tables.$name.name"

    abstract override def deleteTableNames(partition: Option[String]): Seq[String] = {
      val deleted = super.deleteTableNames(partition)
      if (partition.isEmpty) {
        ds.metadata.scan(sft.getTypeName, fallbackTableNameKey, cache = false).foreach { case (k, _) =>
          ds.metadata.remove(sft.getTypeName, k)
        }
      }
      deleted
    }

    abstract override def getTableNames(partition: Option[String] = None): Seq[String] = {
      val names = super.getTableNames(partition)
      if (partition.isEmpty) {
        names ++ ds.metadata.scan(sft.getTypeName, fallbackTableNameKey).map(_._2)
      } else {
        names
      }
    }
  }
}
