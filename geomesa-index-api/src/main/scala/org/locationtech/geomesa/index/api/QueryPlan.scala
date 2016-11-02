/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.api

import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature

trait QueryPlan[DS <: GeoMesaDataStore[DS, F, W, QueryResult], F <: WrappedFeature, W, QueryResult] {

  def filter: FilterStrategy[DS, F, W, QueryResult]

  def entriesToFeatures: (QueryResult) => SimpleFeature
  def hasDuplicates: Boolean = false
  def reduce: Option[(CloseableIterator[SimpleFeature]) => CloseableIterator[SimpleFeature]] = None

  def scan(ds: DS): CloseableIterator[QueryResult]
  def explain(explainer: Explainer, prefix: String = ""): Unit
}
