/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait SimpleFeatureIndex {

  def sft: SimpleFeatureType

  /**
    * Insert a simple feature into the index
    *
    * @param feature feature
    */
  def insert(feature: SimpleFeature): Unit

  /**
    * Insert multiple features into the index
    *
    * @param features features
    */
  def insert(features: Iterable[SimpleFeature]): Unit

  /**
    * Update an existing feature
    *
    * @param feature new feature
    * @return old feature, if any
    */
  def update(feature: SimpleFeature): SimpleFeature

  /**
    * Remove an existing feature
    *
    * @param id feature ID
    * @return removed feature, if any
    */
  def remove(id: String): SimpleFeature

  /**
    * Return a feature by feature ID
    *
    * @param id feature id
    * @return feature, if it exists
    */
  def get(id: String): SimpleFeature

  /**
    * Query features
    *
    * @param filter filter
    * @return
    */
  def query(filter: Filter): Iterator[SimpleFeature]
}
