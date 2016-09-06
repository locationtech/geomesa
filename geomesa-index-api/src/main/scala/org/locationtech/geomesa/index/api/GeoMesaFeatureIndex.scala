/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.index.api

import org.geotools.factory.Hints
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait GeoMesaFeatureIndex[Ops <: HasGeoMesaStats, FeatureWrapper, Result, Plan] {

  type TypedFilterStrategy = FilterStrategy[Ops, FeatureWrapper, Result, Plan]

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

  /**
    * Is the index compatible with the given feature type
    *
    * @param sft simple feature type
    * @return
    */
  def supports(sft: SimpleFeatureType): Boolean

  /**
    * Configure the index upon initial creation
    *
    * @param sft simple feature type
    * @param ops operations
    */
  def configure(sft: SimpleFeatureType, ops: Ops): Unit

  /**
    * Creates a function to write a feature to the index
    *
    * @param sft simple feature type
    * @return
    */
  def writer(sft: SimpleFeatureType, ops: Ops): (FeatureWrapper) => Result

  /**
    * Creates a function to delete a feature from the index
    *
    * @param sft simple feature type
    * @return
    */
  def remover(sft: SimpleFeatureType, ops: Ops): (FeatureWrapper) => Result

  /**
    * Deletes all features from the index
    *
    * @param sft simple feature type
    * @param ops operations
    */
  def removeAll(sft: SimpleFeatureType, ops: Ops): Unit

  /**
    * Gets options for a 'simple' filter, where each OR is on a single attribute, e.g.
    *   (bbox1 OR bbox2) AND dtg
    *   bbox AND dtg AND (attr1 = foo OR attr = bar)
    * not:
    *   bbox OR dtg
    *
    * Because the inputs are simple, each one can be satisfied with a single query filter.
    * The returned values will each satisfy the query.
    *
    * @param filter input filter
    * @return sequence of options, any of which can satisfy the query
    */
  def getFilterStrategy(sft: SimpleFeatureType, filter: Filter): Seq[TypedFilterStrategy]

  /**
    * Gets the estimated cost of running the query. In general, this is the estimated
    * number of features that will have to be scanned.
    */
  def getCost(sft: SimpleFeatureType,
              ops: Option[Ops],
              filter: TypedFilterStrategy,
              transform: Option[SimpleFeatureType]): Long

  /**
    * Plans the query
    */
  def getQueryPlan(sft: SimpleFeatureType,
                   ops: Ops,
                   filter: TypedFilterStrategy,
                   hints: Hints,
                   explain: Explainer = ExplainNull): Plan

  /**
    * Trims off the $ of the object name - assumes implementations will be objects
    *
    * @return
    */
  override def toString = getClass.getSimpleName.split("\\$").last

}