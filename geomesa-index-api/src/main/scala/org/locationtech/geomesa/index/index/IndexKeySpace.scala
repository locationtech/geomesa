/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.locationtech.geomesa.index.utils.Explainer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Conversions to/from index keys, without any shards, table sharing, etc.
  */
trait IndexKeySpace[T] {

  /**
    * Can be used with the simple feature type or not
    *
    * @param sft simple feature type
    * @return
    */
  def supports(sft: SimpleFeatureType): Boolean

  /**
    * Length of an index key
    *
    * @return
    */
  def indexKeyLength: Int

  /**
    * Index key from the attributes of a simple feature
    *
    * @param sft simple feature type
    * @param lenient if input values should be strictly checked, or normalized instead
    * @return
    */
  def toIndexKey(sft: SimpleFeatureType, lenient: Boolean = false): (SimpleFeature) => Array[Byte]

  /**
    * Creates ranges over the index keys
    *
    * @param sft simple feature type
    * @param values index values @see getIndexValues
    * @return
    */
  def getRanges(sft: SimpleFeatureType, values: T): Iterator[(Array[Byte], Array[Byte])]

  /**
    * Extracts values out of the filter used for range and push-down predicate creation
    *
    * @param sft simple feature type
    * @param filter query filter
    * @param explain explainer
    * @return
    */
  def getIndexValues(sft: SimpleFeatureType, filter: Filter, explain: Explainer): T
}
