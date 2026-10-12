/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data._
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader.HasGeoMesaFeatureReader
import org.locationtech.geomesa.index.stats.GeoMesaStats.{GeoMesaStatWriter, StatUpdater}
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.impl._
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats, Stat}
import org.locationtech.geomesa.index.view.MergedDataStoreView.{MergedSource, MergedStats, TaskDecorator}
import org.locationtech.geomesa.index.view.MergedQueryRunner.{DataStoreQueryable, Queryable}
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.text.DateParsing

import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.Date
import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.UnaryOperator

/**
 * Merged querying against multiple data stores
 *
 * @param stores delegate stores
 * @param deduplicate deduplicate results across stores
 * @param parallel execute queries in parallel
 * @param taskDecorator parallel task decorator
 * @param namespace namespace
 */
class MergedDataStoreView(
    val stores: Seq[MergedSource[DataStore]],
    deduplicate: Boolean,
    parallel: Boolean,
    taskDecorator: Option[UnaryOperator[Runnable]] = None,
    namespace: Option[String] = None
  ) extends MergedDataStoreSchemas(stores.map(_.source), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {

  require(stores.nonEmpty, "No delegate stores configured")

  private val decorator = new TaskDecorator(taskDecorator)

  private[view] val runner =
    new MergedQueryRunner(this, stores.map(s => s.copy[Queryable](DataStoreQueryable(s.source))), deduplicate, parallel)

  override val stats: GeoMesaStats = new MergedStats(stores, parallel, decorator)

  override def getFeatureSource(name: Name): SimpleFeatureSource = getFeatureSource(name.getLocalPart)

  override def getFeatureSource(typeName: String): SimpleFeatureSource = {
    val sources = stores.map(s => s.copy(s.source.getFeatureSource(typeName)))
    new MergedFeatureSourceView(this, sources, parallel, decorator, getSchema(typeName))
  }

  override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader =
    getFeatureReader(getSchema(query.getTypeName), transaction, query).reader()

  override private[geomesa] def getFeatureReader(
      sft: SimpleFeatureType,
      transaction: Transaction,
      query: Query): GeoMesaFeatureReader =
    GeoMesaFeatureReader(sft, query, runner, None)
}

object MergedDataStoreView {

  import scala.collection.JavaConverters._

  /**
   * Trait for bounding merged stores based on date
   */
  sealed trait DateFilter {
    def toDate(currentDate: Instant): Date
  }

  object DateFilter {

    /**
     * Create a date filter
     *
     * @param offsetOrDate either an offset (PT1D, e.g. a java duration string) or a static date (2026-01-01T00:00:00.000Z)
     * @return
     */
    def apply(offsetOrDate: String): DateFilter = {
      lazy val error =
        new IllegalArgumentException(s"Invalid date filter '$offsetOrDate' - could not parse as a duration nor a date")
      try {
        return RelativeDate(offsetOrDate)
      } catch {
        case e: DateTimeParseException => error.addSuppressed(e)
      }
      try {
        return StaticDate(offsetOrDate)
      } catch {
        case e: DateTimeParseException => error.addSuppressed(e)
      }
      throw error
    }

    case class StaticDate(date: Date) extends DateFilter {
      override def toDate(currentDate: Instant): Date = date
    }

    object StaticDate {
      @throws(classOf[DateTimeParseException])
      def apply(date: String): StaticDate = StaticDate(DateParsing.parseDate(date))
    }

    case class RelativeDate(offset: java.time.Duration) extends DateFilter {
      override def toDate(currentDate: Instant): Date = Date.from(currentDate.plus(offset))
    }

    object RelativeDate {
      @throws(classOf[DateTimeParseException])
      def apply(offset: String): RelativeDate = RelativeDate(java.time.Duration.parse(offset))
    }
  }

  /**
   * A merged object, with associated filters
   *
   * @param source object
   * @param sourceFilter arbitrary filter
   * @param from start date filter
   * @param to end date filter
   * @tparam T type binding
   */
  case class MergedSource[T](source: T, sourceFilter: Option[Filter], from: Option[DateFilter], to: Option[DateFilter]) {

    import FilterHelper.ff

    def copy[U](newObj: U): MergedSource[U] = MergedSource(newObj, sourceFilter, from, to)

    /**
     * The filter to apply to this source, based on the current date
     *
     * @param dtg date attribute used for filtering
     * @param now current timestamp, for consistency across multiple stores
     * @return
     */
    def filter(dtg: String, now: Instant): Option[Filter] = {
      val fromFilter = from.map(d => ff.greaterOrEqual(ff.property(dtg), ff.literal(d.toDate(now))))
      val toFilter = to.map(d => ff.less(ff.property(dtg), ff.literal(d.toDate(now))))
      FilterHelper.filterListAsAnd(sourceFilter.toSeq ++ fromFilter ++ toFilter)
    }
  }

  class TaskDecorator(op: Option[UnaryOperator[Runnable]]) {
    def apply(task: Runnable): Runnable = op.fold(task)(_.apply(task))
  }

  class MergedStats(stores: Seq[MergedSource[DataStore]], parallel: Boolean, decorator: TaskDecorator)
      extends GeoMesaStats {

    private val stats: Seq[MergedSource[GeoMesaStats]] = stores.map {
      case s@MergedSource(stats: HasGeoMesaStats, _, _, _) => s.copy(stats.stats)
      case s => s.copy[GeoMesaStats](new UnoptimizedRunnableStats(s.source))
    }

    override val writer: GeoMesaStatWriter = new MergedStatWriter(stats.map(_.source.writer))

    override def getCount(sft: SimpleFeatureType, filter: Filter, exact: Boolean, queryHints: Hints): Option[Long] = {
      val now = Instant.now() // shared current time passed to all stores for consistency in bounds calculations
      // note: unlike most methods in this class, this will return if any of the merged stores provide a response
      def getSingle(source: MergedSource[GeoMesaStats]): Option[Long] =
        source.source.getCount(sft, mergeFilter(sft, filter, source.filter(sft.getDtgField.orNull, now)), exact, queryHints)

      if (parallel) {
        val results = new CopyOnWriteArrayList[Long]()
        stats.toList.map(s => CachedThreadPool.submit(decorator(() => getSingle(s).foreach(results.add)))).foreach(_.get)
        results.asScala.reduceLeftOption(_ + _)
      } else {
        stats.flatMap(getSingle).reduceLeftOption(_ + _)
      }
    }

    override def getMinMax[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[MinMax[T]] = {
      val now = Instant.now() // shared current time passed to all stores for consistency in bounds calculations
      // note: unlike most methods in this class, this will return if any of the merged stores provide a response
      def getSingle(source: MergedSource[GeoMesaStats]): Option[MinMax[T]] =
        source.source.getMinMax[T](sft, attribute, mergeFilter(sft, filter, source.filter(sft.getDtgField.orNull, now)), exact)

      if (parallel) {
        val results = new CopyOnWriteArrayList[MinMax[T]]()
        stats.toList.map(s => CachedThreadPool.submit(decorator(() => getSingle(s).foreach(results.add)))).foreach(_.get)
        results.asScala.reduceLeftOption(_ + _)
      } else {
        stats.flatMap(getSingle).reduceLeftOption(_ + _)
      }
    }

    override def getEnumeration[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[EnumerationStat[T]] = {
      merge(sft, (stat, f) => stat.getEnumeration[T](sft, attribute, mergeFilter(sft, filter, f), exact))
    }

    override def getFrequency[T](
        sft: SimpleFeatureType,
        attribute: String,
        precision: Int,
        filter: Filter,
        exact: Boolean): Option[Frequency[T]] = {
      merge(sft, (stat, f) => stat.getFrequency[T](sft, attribute, precision, mergeFilter(sft, filter, f), exact))
    }

    override def getTopK[T](
        sft: SimpleFeatureType,
        attribute: String,
        filter: Filter,
        exact: Boolean): Option[TopK[T]] = {
      merge(sft, (stat, f) => stat.getTopK[T](sft, attribute, mergeFilter(sft, filter, f), exact))
    }

    override def getHistogram[T](
        sft: SimpleFeatureType,
        attribute: String,
        bins: Int,
        min: T,
        max: T,
        filter: Filter,
        exact: Boolean): Option[Histogram[T]] = {
      merge(sft, (stat, f) => stat.getHistogram[T](sft, attribute, bins, min, max, mergeFilter(sft, filter, f), exact))
    }

    override def getZ3Histogram(
        sft: SimpleFeatureType,
        geom: String,
        dtg: String,
        period: TimePeriod,
        bins: Int,
        filter: Filter,
        exact: Boolean): Option[Z3Histogram] = {
      merge(sft, (stat, f) => stat.getZ3Histogram(sft, geom, dtg, period, bins, mergeFilter(sft, filter, f), exact))
    }

    override def getStat[T <: Stat](
        sft: SimpleFeatureType,
        query: String,
        filter: Filter,
        exact: Boolean): Option[T] = {
      merge(sft, (stat, f) => stat.getStat(sft, query, mergeFilter(sft, filter, f), exact))
    }

    override def close(): Unit = CloseWithLogging(stats.map(_.source))

    private def merge[T <: Stat](sft: SimpleFeatureType, query: (GeoMesaStats, Option[Filter]) => Option[T]): Option[T] = {
      val dtg = sft.getDtgField.orNull
      val now = Instant.now()
      if (parallel) {
        val results = new CopyOnWriteArrayList[Option[T]]()
        stats.toList.map(s => CachedThreadPool.submit(decorator(() => results.add(query(s.source, s.filter(dtg, now)))))).foreach(_.get)
        results.asScala.reduceLeft((res, next) => for { r <- res; n <- next } yield { (r + n).asInstanceOf[T] })
      } else {
        // lazily evaluate each stat as we only return Some if all the child stores do
        val head = query(stats.head.source, stats.head.filter(dtg, now))
        stats.tail.foldLeft(head) { case (result, stat) =>
          for { r <- result; n <- query(stat.source, stat.filter(dtg, now)) } yield { (r + n).asInstanceOf[T] }
        }
      }
    }
  }

  private class MergedStatWriter(writers: Seq[GeoMesaStatWriter]) extends GeoMesaStatWriter {
    override def analyze(sft: SimpleFeatureType): Seq[Stat] = {
      writers.map(_.analyze(sft)).reduceLeft[Seq[Stat]] { case (left, right) =>
        left.zip(right).map { case (l, r) => l + r }
      }
    }

    override def updater(sft: SimpleFeatureType): StatUpdater = new MergedStatUpdater(writers.map(_.updater(sft)))

    override def rename(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit =
      writers.foreach(_.rename(sft, previous))

    override def clear(sft: SimpleFeatureType): Unit = writers.foreach(_.clear(sft))
  }

  private class MergedStatUpdater(updaters: Seq[StatUpdater]) extends StatUpdater {
    override def add(sf: SimpleFeature): Unit = updaters.foreach(_.add(sf))
    override def remove(sf: SimpleFeature): Unit = updaters.foreach(_.remove(sf))
    override def flush(): Unit = updaters.foreach(_.flush())
    override def close(): Unit = CloseWithLogging(updaters)
  }
}
