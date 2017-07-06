/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.filter.visitor.BindingFilterVisitor
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait QueryRunner {

  def runQuery(sft: SimpleFeatureType,
               query: Query,
               explain: Explainer = new ExplainLogging): CloseableIterator[SimpleFeature]

  /**
    * Configure the query - set hints, transforms, etc.
    *
    * @param original query to configure
    * @param sft simple feature type associated with the query
    */
  protected [geomesa] def configureQuery(sft: SimpleFeatureType, original: Query): Query = {
    val query = new Query(original) // note: this ends up sharing a hints object between the two queries

    // set query hints - we need this in certain situations where we don't have access to the query directly
    QueryPlanner.threadedHints.get.foreach { hints =>
      hints.foreach { case (k, v) => query.getHints.put(k, v) }
      // clear any configured hints so we don't process them again
      QueryPlanner.threadedHints.clear()
    }

    // handle any params passed in through geoserver
    ViewParams.setHints(sft, query)

    // set transformations in the query
    QueryPlanner.setQueryTransforms(query, sft)
    // set return SFT in the query
    query.getHints.put(QueryHints.Internal.RETURN_SFT, getReturnSft(sft, query.getHints))

    query
  }

  /**
    * Updates the filter in the query by binding values, optimizing predicates, etc
    *
    * @param sft simple feature type
    * @param query query
    */
  private [geomesa] def optimizeFilter(sft: SimpleFeatureType, query: Query): Unit = {
    if (query.getFilter != null && query.getFilter != Filter.INCLUDE) {
      query.setFilter(optimizeFilter(sft, query.getFilter))
    }
  }

  /**
    * Optimizes the filter - extension point for subclasses
    *
    * @param sft simple feature poinnt
    * @param filter filter
    * @return optimized filter
    */
  protected def optimizeFilter(sft: SimpleFeatureType, filter: Filter): Filter = {
    // bind the literal values to the appropriate type, so that it isn't done every time the filter is evaluated
    // important: do this before running through the QueryPlanFilterVisitor, otherwise can mess with IDL handling
    filter.accept(new BindingFilterVisitor(sft), null).asInstanceOf[Filter]
    // update the filter to remove namespaces, handle null property names, and tweak topological filters
      .accept(new QueryPlanFilterVisitor(sft), null).asInstanceOf[Filter]
  }

  /**
    * Provides the simple feature type that will be returned from a query. Extension point for subclasses
    *
    * @param sft simple feature type
    * @param hints query hints, which have been configured using @see configureQuery
    * @return simple feature that will be returned from a query using the hints
    */
  protected [geomesa] def getReturnSft(sft: SimpleFeatureType, hints: Hints): SimpleFeatureType = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    hints.getTransformSchema.getOrElse(sft)
  }
}
