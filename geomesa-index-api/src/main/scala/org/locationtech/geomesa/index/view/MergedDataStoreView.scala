/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.{Bounds, FilterHelper, FilterValues, andFilters}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader.HasGeoMesaFeatureReader
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedSource.{MergedSourceSft, TypeOption}
import org.locationtech.geomesa.index.view.MergedDataStoreView.{MergedSource, TaskDecorator}
import org.locationtech.geomesa.index.view.MergedQueryRunner.{DataStoreQueryable, Queryable}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.DateParsing

import java.time.format.DateTimeParseException
import java.time.{Instant, ZonedDateTime}
import java.util.Date
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
    new MergedQueryRunner(this, stores.map(s => s.copy[Queryable](DataStoreQueryable(s.source))), deduplicate, parallel, decorator)

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

object MergedDataStoreView extends LazyLogging {

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
   * @param sourceFilter arbitrary filter, keyed by type name
   * @param from start date filter, keyed by type name
   * @param to end date filter, keyed by type name
   * @tparam T type binding
   */
  case class MergedSource[T](source: T, sourceFilter: TypeOption[Filter], from: TypeOption[DateFilter], to: TypeOption[DateFilter]) {

    import FilterHelper.ff

    import scala.collection.JavaConverters._

    /**
     * The filter to apply to this source, based on the current date
     *
     * @param query query
     * @param now current timestamp, for consistency across multiple stores
     * @param ev feature type loader
     * @return
     */
    def filter(query: Query, now: Instant)(implicit ev: MergedSourceSft[T]): Query = {
      val q = new Query(query)
      q.setFilter(filter(query.getTypeName, query.getFilter, now))
      q
    }

    /**
     * The filter to apply to this source, based on the current date
     *
     * @param typeName feature type name
     * @param base user-supplied filter
     * @param now current timestamp, for consistency across multiple stores
     * @param ev feature type loader
     * @return
     */
    def filter(typeName: String, base: Filter, now: Instant)(implicit ev: MergedSourceSft[T]): Filter = {
      val dtg = dtgField(ev, typeName)
      val fromFilter = from(typeName).map(d => ff.greaterOrEqual(ff.property(dtg), ff.literal(d.toDate(now))))
      val toFilter = to(typeName).map(d => ff.less(ff.property(dtg), ff.literal(d.toDate(now))))
      val storeFilter = FilterHelper.filterListAsAnd(sourceFilter(typeName).toSeq ++ fromFilter ++ toFilter)
      storeFilter match {
        case None => base
        case Some(f) if base == Filter.INCLUDE => f
        case Some(f) if dtg == null => andFilters(Seq(base, f))
        case Some(f) =>
          // check for disjoint dates between the store filter and the query filter
          // note: don't cache this call, as it can contain things like `currentDate()` that will change per invocation
          val disjoint = Some(FilterHelper.extractIntervals(f, dtg)).filter(_.nonEmpty).flatMap { left =>
            val right = FilterHelper.extractIntervals(base, dtg)
            val merged = FilterValues.and[Bounds[ZonedDateTime]](Bounds.intersection)(left, right)
            if (merged.disjoint) {
              logger.debug(s"Suppressing query with filter (${ECQL.toCQL(base)}) AND (${ECQL.toCQL(f)})")
              Some(Filter.EXCLUDE)
            } else {
              None
            }
          }
          disjoint.getOrElse(andFilters(Seq(base, f)))
      }
    }

    private def dtgField(ev: MergedSourceSft[T], typeName: String): String = {
      val sft = ev.sft(source, typeName)
      def firstDtg(): Option[String] =
        sft.getAttributeDescriptors.asScala.collectFirst {
          case d if classOf[Date].isAssignableFrom(d.getType.getBinding) => d.getLocalName
        }
      sft.getDtgField.orElse(firstDtg()).getOrElse {
        throw new IllegalArgumentException(
          s"Schema '$typeName' does not have a date field for filtering: ${SimpleFeatureTypes.encodeType(sft)}")
      }
    }
  }

  object MergedSource {

    def apply[T](source: T): MergedSource[T] = MergedSource(source, TypeOption.empty, TypeOption.empty, TypeOption.empty)

    /**
     * Per feature-type lookup
     *
     * @param byTypeName map of type names to values
     * @param default default for type names not specified in the map
     * @tparam T type binding
     */
    class TypeOption[T](byTypeName: Map[String, T], default: Option[T]) {
      def apply(typeName: String): Option[T] = byTypeName.get(typeName).orElse(default)
    }

    object TypeOption {
      def apply[T](byTypeName: Map[String, T], default: T): TypeOption[T] = new TypeOption[T](byTypeName, Option(default))
      def apply[T](default: T): TypeOption[T] = new TypeOption[T](Map.empty, Option(default))
      def apply[T](byTypeName: Map[String, T]): TypeOption[T] = new TypeOption[T](byTypeName, None)
      def empty[T]: TypeOption[T] = new TypeOption(Map.empty, None)
    }

    /**
     * Helper for extracting a feature type from a merged source
     *
     * @tparam T type binding
     */
    trait MergedSourceSft[T] {
      def sft(source: T, typeName: String): SimpleFeatureType
    }

    implicit object DataSourceSft extends MergedSourceSft[DataStore] {
      override def sft(source: DataStore, typeName: String): SimpleFeatureType = source.getSchema(typeName)
    }

    implicit object FeatureSourceSft extends MergedSourceSft[SimpleFeatureSource] {
      override def sft(source: SimpleFeatureSource, typeName: String): SimpleFeatureType = {
        val sft = source.getSchema
        require(sft != null && sft.getTypeName == typeName)
        sft
      }
    }

    implicit object QueryableSft extends MergedSourceSft[Queryable] {
      override def sft(source: Queryable, typeName: String): SimpleFeatureType = source.sft(typeName)
    }
  }

  class TaskDecorator(op: Option[UnaryOperator[Runnable]]) {
    def apply(task: Runnable): Runnable = op.fold(task)(_.apply(task))
  }
}
