/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.typesafe.scalalogging.Logger
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.{FilterHelper, andFilters, ff}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.iterators.{DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.slf4j.LoggerFactory

trait QueryRunner {

  /**
    * Execute a query
    *
    * @param sft simple feature type
    * @param query query to run
    * @param explain explain output
    * @return
    */
  def runQuery(sft: SimpleFeatureType,
               query: Query,
               explain: Explainer = new ExplainLogging): CloseableIterator[SimpleFeature]

  /**
    * Hook for query interceptors
    *
    * @return
    */
  protected def interceptors: QueryInterceptorFactory

  /**
    * Configure the query - set hints, transforms, etc.
    *
    * @param original query to configure
    * @param sft simple feature type associated with the query
    */
  protected [geomesa] def configureQuery(sft: SimpleFeatureType, original: Query): Query = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val query = new Query(original) // note: this ends up sharing a hints object between the two queries

    // query rewriting
    interceptors(sft).foreach { interceptor =>
      interceptor.rewrite(query)
      QueryRunner.logger.trace(s"Query rewritten by $interceptor to: $query")
    }

    // set query hints - we need this in certain situations where we don't have access to the query directly
    QueryPlanner.threadedHints.get.foreach { hints =>
      hints.foreach { case (k, v) => query.getHints.put(k, v) }
      // clear any configured hints so we don't process them again
      QueryPlanner.threadedHints.clear()
    }

    // handle any params passed in through geoserver
    ViewParams.setHints(query)

    // set transformations in the query
    QueryPlanner.setQueryTransforms(query, sft)
    // set return SFT in the query
    query.getHints.put(QueryHints.Internal.RETURN_SFT, getReturnSft(sft, query.getHints))

    // set sorting in the query
    QueryPlanner.setQuerySort(sft, query)
    QueryPlanner.setMaxFeatures(query)

    // add the bbox from the density query to the filter, if there is no more restrictive filter
    query.getHints.getDensityEnvelope.foreach { env =>
      val geoms = FilterHelper.extractGeometries(query.getFilter, sft.getGeomField)
      if (geoms.isEmpty || geoms.exists(g => !env.contains(g.getEnvelopeInternal))) {
        val bbox = ff.bbox(ff.property(sft.getGeometryDescriptor.getLocalName), env.asInstanceOf[ReferencedEnvelope])
        if (query.getFilter == Filter.INCLUDE) {
          query.setFilter(bbox)
        } else {
          query.setFilter(andFilters(Seq(query.getFilter, bbox)))
        }
      }
    }

    if (query.getFilter != null && query.getFilter != Filter.INCLUDE) {
      // bind the literal values to the appropriate type, so that it isn't done every time the filter is evaluated
      // update the filter to remove namespaces, handle null property names, and tweak topological filters
      // replace filters with 'fast' implementations where possible
      query.setFilter(FastFilterFactory.optimize(sft, query.getFilter))
    }

    query
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
    if (hints.isBinQuery) {
      BinaryOutputEncoder.BinEncodedSft
    } else if (hints.isArrowQuery) {
      org.locationtech.geomesa.arrow.ArrowEncodedSft
    } else if (hints.isDensityQuery) {
      DensityScan.DensitySft
    } else if (hints.isStatsQuery) {
      StatsScan.StatsSft
    } else {
      hints.getTransformSchema.getOrElse(sft)
    }
  }
}

object QueryRunner {

  private val logger = Logger(LoggerFactory.getLogger(classOf[QueryRunner]))

  // used for configuring input queries
  private val default: QueryRunner = new QueryRunner {
    override protected val interceptors: QueryInterceptorFactory = QueryInterceptorFactory.empty()
    override def runQuery(sft: SimpleFeatureType,
                          query: Query,
                          explain: Explainer): CloseableIterator[SimpleFeature] = throw new NotImplementedError
  }

  def configureDefaultQuery(sft: SimpleFeatureType, original: Query): Query = default.configureQuery(sft, original)
}
