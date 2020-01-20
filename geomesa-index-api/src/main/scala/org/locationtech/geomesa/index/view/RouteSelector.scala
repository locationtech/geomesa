/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.data.{DataStore, Query}
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Routes queries to one of a set of stores
  */
trait RouteSelector {

  /**
    * Initialize this instance with the datastore to select from
    *
    * @param stores stores and configuration maps
    */
  def init(stores: Seq[(DataStore, java.util.Map[String, _ <: AnyRef])]): Unit

  /**
    * Route a query to a particular store. If no store is selected, query will return empty
    *
    * @param sft simple feature type
    * @param query query
    * @return
    */
  def route(sft: SimpleFeatureType, query: Query): Option[DataStore]
}
