/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature

/**
  * Plan for querying a GeoMesaDataStore
  *
  * @tparam DS type of this data store
  * @tparam F wrapper around a simple feature - used for caching write calculations
  * @tparam W write result - feature writers will transform simple features into these
  */
trait QueryPlan[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W] {

  /**
    * Filter this query plan is satisfying
    *
    * @return
    */
  def filter: FilterStrategy[DS, F, W]

  /**
    * May return duplicate simple features or not
    *
    * @return
    */
  def hasDuplicates: Boolean = false

  /**
    * Optional reduce step for simple features coming back
    *
    * @return
    */
  def reduce: Option[(CloseableIterator[SimpleFeature]) => CloseableIterator[SimpleFeature]] = None

  /**
    * Runs the query plain against the underlying database, returning the raw entries
    *
    * @param ds data store - provides connection object and metadata
    * @return
    */
  def scan(ds: DS): CloseableIterator[SimpleFeature]

  /**
    * Explains details on how this query plan will be executed
    *
    * @param explainer explainer to use for explanation
    * @param prefix prefix for explanation lines, used for nesting explanations
    */
  def explain(explainer: Explainer, prefix: String = ""): Unit
}
