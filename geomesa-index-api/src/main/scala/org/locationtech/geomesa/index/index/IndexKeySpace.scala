/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
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

  protected val processingValues: ThreadLocal[T] = new ThreadLocal[T]

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
    * @return
    */
  def toIndexKey(sft: SimpleFeatureType): (SimpleFeature) => Array[Byte]

  /**
    * Ranges over the index keys. Calling this method will populate
    * currentProcessingValues for the current thread.
    *
    * <b>Make sure to call `clearProcessingValues()` in a try/finally block around this method.</b>
    *
    * @param sft simple feature type
    * @param filter spatio(temporal) filter to evaluate
    * @param explain explainer
    * @return
    */
  def getRanges(sft: SimpleFeatureType, filter: Filter, explain: Explainer): Iterator[(Array[Byte], Array[Byte])]

  /**
    * Hook back into values extracted during range processing.
    * Processing values will be populated after a call to getRanges,
    * and should be cleared afterwards.
    *
    * @return values extracted during range processing, if any
    */
  def currentProcessingValues: Option[T] = Option(processingValues.get)

  /**
    * Clears any existing processing value
    */
  def clearProcessingValues(): Unit = processingValues.remove()
}
