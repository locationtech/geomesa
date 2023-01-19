/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Conversions to/from index keys
  *
  * @tparam T values extracted from a filter and used for creating ranges - extracted geometries, z-ranges, etc
  * @tparam U a single key space index value, e.g. Long for a z-value, etc
  */
trait IndexKeySpace[T, U] {

  /**
    * Simple feature type being indexed
    *
    * @return
    */
  def sft: SimpleFeatureType

  /**
    * The attributes used to create the index keys
    *
    * @return
    */
  def attributes: Seq[String]

  /**
    * Length of an index key. If static (general case), will return a Right with the length. If dynamic,
    * will return Left with a function to determine the length from a given (row, offset, length)
    *
    * @return
    */
  def indexKeyByteLength: Either[(Array[Byte], Int, Int) => Int, Int]

  /**
    * Table sharing
    *
    * @return
    */
  def sharing: Array[Byte]

  /**
    * Strategy for sharding
    *
    * @return
    */
  def sharding: ShardStrategy

  /**
    * Index key from the attributes of a simple feature
    *
    * @param feature simple feature with cached values
    * @param tier tier bytes
    * @param id feature id bytes
    * @param lenient if input values should be strictly checked, or normalized instead
    * @return
    */
  def toIndexKey(feature: WritableFeature, tier: Array[Byte], id: Array[Byte], lenient: Boolean = false): RowKeyValue[U]

  /**
    * Extracts values out of the filter used for range and push-down predicate creation
    *
    * @param filter query filter
    * @param explain explainer
    * @return
    */
  def getIndexValues(filter: Filter, explain: Explainer): T

  /**
    * Creates ranges over the index keys
    *
    * @param values index values @see getIndexValues
    * @param multiplier hint for how many times the ranges will be multiplied. can be used to
    *                   inform the number of ranges generated
    * @return
    */
  def getRanges(values: T, multiplier: Int = 1): Iterator[ScanRange[U]]

  /**
    * Creates bytes from ranges
    *
    * @param ranges typed scan ranges. @see `getRanges`
    * @param tier will the ranges have tiered ranges appended, or not
    * @return
    */
  def getRangeBytes(ranges: Iterator[ScanRange[U]], tier: Boolean = false): Iterator[ByteRange]

  /**
    * Determines if the ranges generated by `getRanges` are sufficient to fulfill the query,
    * or if additional filtering needs to be done
    *
    * @param config data store config
    * @param values index values @see getIndexValues
    * @param hints query hints
    * @return
    */
  def useFullFilter(values: Option[T], config: Option[GeoMesaDataStoreConfig], hints: Hints): Boolean
}

object IndexKeySpace {

  /**
    * Factory for creating key spaces
    *
    * @tparam T values extracted from a filter and used for creating ranges - extracted geometries, z-ranges, etc
    * @tparam U a single key space index value, e.g. Long for a z-value, etc
    */
  trait IndexKeySpaceFactory[T, U] {
    def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean
    def apply(sft: SimpleFeatureType, attributes: Seq[String], tier: Boolean): IndexKeySpace[T, U]
  }
}
