/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.store.DataFeatureCollection
import org.geotools.data.{FeatureReader, Query, Transaction}
import org.geotools.feature.visitor.{BoundsVisitor, MaxVisitor, MinVisitor}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.index.process.GeoMesaProcessVisitor
import org.opengis.feature.FeatureVisitor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.PropertyName
import org.opengis.util.ProgressListener

/**
  * Feature collection implementation
  */
class GeoMesaFeatureCollection(private [geotools] val source: GeoMesaFeatureSource,
                               private [geotools] val query: Query)
    extends DataFeatureCollection(GeoMesaFeatureCollection.nextId) with LazyLogging {

  private val open = new AtomicBoolean(false)

  override def getSchema: SimpleFeatureType = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    if (!open.get) {
      // once opened the query will already be configured by the query planner,
      // otherwise we have to compute it here
      source.runner.configureQuery(source.getSchema, query)
    }
    query.getHints.getReturnSft
  }

  override protected def openIterator(): java.util.Iterator[SimpleFeature] = {
    val iter = super.openIterator()
    open.set(true)
    iter
  }

  override def accepts(visitor: FeatureVisitor, progress: ProgressListener): Unit =
    visitor match {
      case v: BoundsVisitor => v.reset(source.ds.stats.getBounds(source.getSchema, query.getFilter))

      case v: MinVisitor if v.getExpression.isInstanceOf[PropertyName] =>
        val attribute = v.getExpression.asInstanceOf[PropertyName].getPropertyName
        minMax(attribute, exact = false).orElse(minMax(attribute, exact = true)) match {
          case Some((min, _)) => v.setValue(min)
          case None           => super.accepts(visitor, progress)
        }

      case v: MaxVisitor if v.getExpression.isInstanceOf[PropertyName] =>
        val attribute = v.getExpression.asInstanceOf[PropertyName].getPropertyName
        minMax(attribute, exact = false).orElse(minMax(attribute, exact = true)) match {
          case Some((_, max)) => v.setValue(max)
          case None           => super.accepts(visitor, progress)
        }

      case v: GeoMesaProcessVisitor => v.execute(source, query)

      case v =>
        logger.debug(s"Using unoptimized method for visiting '${v.getClass.getName}'")
        super.accepts(visitor, progress)
    }

  private def minMax(attribute: String, exact: Boolean): Option[(Any, Any)] =
    source.ds.stats.getAttributeBounds[Any](source.getSchema, attribute, query.getFilter, exact).map(_.bounds)

  override def reader(): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    source.ds match {
      case gm: GeoMesaDataStore[_] => gm.getFeatureReader(source.sft, query)
      case ds => ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    }
  }

  override def getBounds: ReferencedEnvelope = source.getBounds(query)

  override def getCount: Int = {
    if (!open.get) {
      // once opened the query will already be configured by the query planner,
      // otherwise do it here
      source.runner.configureQuery(source.getSchema, query)
    }
    source.getCount(query)
  }

  // note: this shouldn't return -1 (as opposed to FeatureSource.getCount), but we still don't return a valid
  // size unless exact counts are enabled
  override def size: Int = {
    val count = getCount
    if (count < 0) { 0 } else { count }
  }
}

object GeoMesaFeatureCollection {
  private val oneUp = new AtomicLong(0)
  def nextId: String = s"GeoMesaFeatureCollection-${oneUp.getAndIncrement()}"
}
