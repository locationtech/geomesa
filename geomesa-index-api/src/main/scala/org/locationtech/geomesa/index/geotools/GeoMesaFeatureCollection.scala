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
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.data.store.DataFeatureCollection
import org.geotools.data.{FeatureReader, Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.feature.FeatureCollection
import org.geotools.feature.collection.{DecoratingFeatureCollection, DecoratingSimpleFeatureCollection}
import org.geotools.feature.visitor.{BoundsVisitor, MaxVisitor, MinVisitor}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.util.NullProgressListener
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection.GeoMesaFeatureVisitingCollection
import org.locationtech.geomesa.index.process.GeoMesaProcessVisitor
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.FeatureVisitor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.sort.SortBy
import org.opengis.util.ProgressListener

import scala.annotation.tailrec

/**
  * Feature collection implementation
  */
class GeoMesaFeatureCollection(source: GeoMesaFeatureSource, original: Query)
    extends GeoMesaFeatureVisitingCollection(source, source.ds.stats, original) {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private val open = new AtomicBoolean(false)

  // copy of the original query, so that we don't modify hints/sorts/etc
  private val query = {
    val copy = new Query(original)
    copy.setHints(new Hints(original.getHints))
    copy
  }

  // configured version of the query, with hints set
  // once opened the query will already be configured by the query planner
  private lazy val configured = {
    if (!open.get) {
      source.runner.configureQuery(source.sft, query)
    }
    query
  }

  override def getSchema: SimpleFeatureType = configured.getHints.getReturnSft

  override protected def openIterator(): java.util.Iterator[SimpleFeature] = {
    val iter = super.openIterator()
    open.set(true)
    iter
  }

  override def reader(): FeatureReader[SimpleFeatureType, SimpleFeature] =
    source.ds.getFeatureReader(query, Transaction.AUTO_COMMIT)

  override def subCollection(filter: Filter): SimpleFeatureCollection = {
    val merged = new Query(original)
    merged.setHints(new Hints(original.getHints))
    val filters = Seq(merged.getFilter, filter).filter(_ != Filter.INCLUDE)
    FilterHelper.filterListAsAnd(filters).foreach(merged.setFilter)
    new GeoMesaFeatureCollection(source, merged)
  }

  override def sort(order: SortBy): SimpleFeatureCollection = {
    val merged = new Query(original)
    merged.setHints(new Hints(original.getHints))
    if (merged.getSortBy == null) {
      merged.setSortBy(Array(order))
    } else {
      merged.setSortBy(merged.getSortBy :+ order)
    }
    new GeoMesaFeatureCollection(source, merged)
  }

  override def getBounds: ReferencedEnvelope = source.getBounds(query)

  override def getCount: Int = source.getCount(configured)

  // note: this shouldn't return -1 (as opposed to FeatureSource.getCount), but we still don't return a valid
  // size unless exact counts are enabled
  override def size: Int = {
    val count = getCount
    if (count < 0) { 0 } else { count }
  }

  override def contains(o: Any): Boolean = {
    o match {
      case f: SimpleFeature =>
        val sub = subCollection(org.locationtech.geomesa.filter.ff.id(f.getIdentifier))
        WithClose(CloseableIterator(sub.features()))(_.nonEmpty)

      case _ => false
    }
  }

  override def containsAll(collection: java.util.Collection[_]): Boolean = {
    val size = collection.size()
    if (size == 0) { true } else {
      val filters = Seq.newBuilder[Filter]
      filters.sizeHint(size)

      val features = collection.iterator()
      while (features.hasNext) {
        features.next match {
          case f: SimpleFeature => filters += org.locationtech.geomesa.filter.ff.id(f.getIdentifier)
          case _ => return false
        }
      }

      val sub = subCollection(org.locationtech.geomesa.filter.orFilters(filters.result))
      WithClose(CloseableIterator(sub.features()))(_.length) == size
    }
  }
}

object GeoMesaFeatureCollection extends LazyLogging {

  private val oneUp = new AtomicLong(0)

  def nextId: String = s"GeoMesaFeatureCollection-${oneUp.getAndIncrement()}"

  /**
    * Attempts to visit the feature collection in an optimized manner. This will unwrap any decorating
    * feature collections that may interfere with the `accepts` method.
    *
    * Note that generally this may not be a good idea - collections are presumably wrapped for a reason.
    * However, our visitation functionality keeps being broken by changes in GeoServer, so we're being
    * defensive here.
    *
    * @param collection feature collection
    * @param visitor visitor
    * @param progress progress monitor
    */
  def visit(
      collection: FeatureCollection[SimpleFeatureType, SimpleFeature],
      visitor: FeatureVisitor,
      progress: ProgressListener = new NullProgressListener): Unit = {
    val unwrapped = if (collection.isInstanceOf[GeoMesaFeatureVisitingCollection]) { collection } else {
      try { unwrap(collection) } catch {
        case e: Throwable => logger.debug("Error trying to unwrap feature collection:", e); collection
      }
    }
    unwrapped.accepts(visitor, progress)
  }

  /**
    * Attempts to remove any decorating feature collections
    *
    * @param collection collection
    * @param level level of collections that have been removed, used to detect potentially infinite looping
    * @return
    */
  @tailrec
  private def unwrap(
      collection: FeatureCollection[SimpleFeatureType, SimpleFeature],
      level: Int = 1): FeatureCollection[SimpleFeatureType, SimpleFeature] = {

    // noinspection TypeCheckCanBeMatch
    val unwrapped = if (collection.isInstanceOf[DecoratingSimpleFeatureCollection]) {
      getDelegate(collection, classOf[DecoratingSimpleFeatureCollection])
    } else if (collection.isInstanceOf[DecoratingFeatureCollection[SimpleFeatureType, SimpleFeature]]) {
      getDelegate(collection, classOf[DecoratingFeatureCollection[SimpleFeatureType, SimpleFeature]])
    } else {
      logger.debug(s"Unable to unwrap feature collection $collection of class ${collection.getClass.getName}")
      return collection
    }

    logger.debug(s"Unwrapped feature collection $collection of class ${collection.getClass.getName} to " +
        s"$unwrapped of class ${unwrapped.getClass.getName}")

    if (unwrapped.isInstanceOf[GeoMesaFeatureVisitingCollection]) {
      unwrapped
    } else if (level > 9) {
      logger.debug("Aborting feature collection unwrapping after 10 iterations")
      unwrapped
    } else {
      unwrap(unwrapped, level + 1)
    }
  }

  /**
    * Uses reflection to access the protected field 'delegate' in decorating feature collection classes
    *
    * @param collection decorating feature collection
    * @param clas decorating feature base class
    * @return
    */
  private def getDelegate(
      collection: FeatureCollection[SimpleFeatureType, SimpleFeature],
      clas: Class[_]): FeatureCollection[SimpleFeatureType, SimpleFeature] = {
    val m = clas.getDeclaredField("delegate")
    m.setAccessible(true)
    m.get(collection).asInstanceOf[FeatureCollection[SimpleFeatureType, SimpleFeature]]
  }

  /**
    * Base class for handling feature visitors
    *
    * @param source feature source
    * @param stats geomesa stat hook
    * @param query query
    */
  abstract class GeoMesaFeatureVisitingCollection(source: SimpleFeatureSource, stats: GeoMesaStats, query: Query)
      extends DataFeatureCollection(nextId) with LazyLogging {

    override def accepts(visitor: FeatureVisitor, progress: ProgressListener): Unit = {
      visitor match {
        case v: BoundsVisitor => v.reset(stats.getBounds(source.getSchema, query.getFilter))

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
          logger.warn(s"Using unoptimized method for visiting '${v.getClass.getName}'")
          super.accepts(visitor, progress)
      }
    }

    private def minMax(attribute: String, exact: Boolean): Option[(Any, Any)] =
      stats.getAttributeBounds[Any](source.getSchema, attribute, query.getFilter, exact).map(_.bounds)
  }
}
