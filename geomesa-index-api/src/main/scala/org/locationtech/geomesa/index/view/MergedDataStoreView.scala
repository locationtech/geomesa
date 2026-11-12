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
import org.locationtech.geomesa.index.view.MergedDataStoreView.{FilterProvider, MergedSource, TaskDecorator}
import org.locationtech.geomesa.index.view.MergedDataStoreView.MergedSource.{DataSourceSft, MergedSourceSft, TypeOption}
import org.locationtech.geomesa.index.view.MergedQueryRunner.{DataStoreQueryable, Queryable}
import org.locationtech.geomesa.security.HasAuthorizationsProvider
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.DateParsing

import java.io.Closeable
import java.time.format.DateTimeParseException
import java.time.{Instant, ZonedDateTime}
import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.UnaryOperator

/**
 * Merged querying against multiple data stores
 *
 * @param stores delegate stores
 * @param deduplicate deduplicate results across stores
 * @param parallel execute queries in parallel
 * @param namespace namespace
 * @param filterProvider optional operation-scoped filtering and delegate selection
 */
class MergedDataStoreView(
    val stores: Seq[MergedSource[DataStore]],
    deduplicate: Boolean,
    parallel: Boolean,
    namespace: Option[String] = None,
    filterProvider: Option[FilterProvider] = None
  ) extends MergedDataStoreSchemas(stores.map(_.source), namespace) with HasGeoMesaFeatureReader with HasGeoMesaStats {

  require(stores.nonEmpty, "No delegate stores configured")
  filterProvider.foreach(_.validate(stores))

  private val disposed = new AtomicBoolean(false)

  private val delegates = stores.map { source =>
    source.source match {
      case store: HasAuthorizationsProvider if source.decorator.isEmpty =>
        source.copy(decorator = new TaskDecorator(Some(store.authProvider)))
      case _ => source
    }
  }

  private[view] val runner =
    new MergedQueryRunner(this, delegates.map(s => s.copy[Queryable](DataStoreQueryable(s.source))), deduplicate, parallel)

  private val mergedStats = new MergedStats(delegates, parallel)
  override val stats: GeoMesaStats = filterProvider match {
    case None => mergedStats
    case Some(_) => new MergedOperationStats(snapshot, mergedStats.writer, () => mergedStats.close())
  }

  override def getTypeNames: Array[String] = filterProvider match {
    case None => super.getTypeNames.filter(n => stores.forall(_.acceptsType(n)))
    case Some(_) =>
      stores.flatMap(_.source.getTypeNames).distinct.filter { name =>
        val selected = select(name)
        selected.nonEmpty && selected.forall(_.source.getTypeNames.contains(name))
      }.sorted.toArray
  }

  override def getSchema(typeName: String): SimpleFeatureType = filterProvider match {
    case None => if (stores.forall(_.acceptsType(typeName))) { super.getSchema(typeName) } else { null }
    case Some(_) =>
      val selected = select(typeName)
      if (selected.isEmpty || selected.exists(s => !s.source.getTypeNames.contains(typeName))) { null } else {
        plan(selected).getSchema(typeName)
      }
  }

  override def getFeatureSource(name: Name): SimpleFeatureSource = getFeatureSource(name.getLocalPart)

  override def getFeatureSource(typeName: String): SimpleFeatureSource = filterProvider match {
    case Some(_) =>
      require(getSchema(typeName) != null, s"Unknown merged feature type '$typeName'")
      new MergedOperationFeatureSource(this, typeName, () => snapshot(typeName), () => getSchema(typeName),
        () => plan(select(typeName)).getFeatureSource(typeName).getQueryCapabilities)
    case None =>
      require(delegates.forall(_.acceptsType(typeName)), s"Unknown merged feature type '$typeName'")
      val sources = delegates.map(s => s.copy(s.source.getFeatureSource(typeName)))
      new MergedFeatureSourceView(this, sources, parallel, getSchema(typeName))
  }

  override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader = {
    val copy = new Query(query)
    getFeatureReader(getSchema(copy.getTypeName), transaction, copy).reader()
  }

  override private[geomesa] def getFeatureReader(
      sft: SimpleFeatureType,
      transaction: Transaction,
      query: Query): GeoMesaFeatureReader = {
    ensureOpen()
    require(sft != null && sft.getTypeName == query.getTypeName, "Feature type and query must agree")
    val copy = new Query(query)
    filterProvider match {
      case None =>
        require(delegates.forall(_.acceptsType(copy.getTypeName)), s"Unknown merged feature type '${copy.getTypeName}'")
        GeoMesaFeatureReader(sft, copy, runner, None)
      case Some(_) =>
        val view = snapshot(copy.getTypeName)
        view.getFeatureReader(view.getSchema(copy.getTypeName), transaction, copy)
    }
  }

  override def dispose(): Unit = if (disposed.compareAndSet(false, true)) {
    try { super.dispose() } finally { filterProvider.foreach(_.close()) }
  }

  private[view] def ensureOpen(): Unit = require(!disposed.get(), "Merged view has been disposed")

  private def select(typeName: String): Seq[MergedSource[DataStore]] = {
    ensureOpen()
    require(typeName != null, "A merged feature type is required")
    val selected = delegates.filter(_.acceptsType(typeName))
    filterProvider.fold(selected)(_.select(typeName, selected))
  }

  private[view] def snapshot(typeName: String): MergedDataStoreView = filterProvider match {
    case None => this
    case Some(provider) =>
      val now = Instant.now()
      val selected = select(typeName)
      require(selected.nonEmpty, s"Unknown merged feature type '$typeName'")
      val filters = provider.filters(typeName, selected, now)
      require(filters.size == selected.size, "Filter provider must return one filter per selected delegate")
      val resolved = selected.zip(filters).map { case (source, filter) =>
        require(filter != null, "Filter provider returned a null filter")
        // Resolve configured relative dates against the same time as the provider, once per operation.
        val combined = FilterHelper.filterListAsAnd(
          Seq(source.filter(typeName, Filter.INCLUDE, now)(DataSourceSft), filter).filter(_ != Filter.INCLUDE)).getOrElse(Filter.INCLUDE)
        source.copy(sourceFilter = TypeOption(combined), from = TypeOption.empty, to = TypeOption.empty,
          acceptsType = _ == typeName)
      }
      plan(resolved)
  }

  private def plan(selected: Seq[MergedSource[DataStore]]): MergedDataStoreView =
    new MergedDataStoreView(selected, deduplicate, parallel, namespace) {
      // Private operation plans never own the public view's delegates.
      override def dispose(): Unit = {}
    }
}

object MergedDataStoreView extends LazyLogging {

  /**
   * Resolve fixed routing filters once per operation. Collections retain the resolved filters for all their
   * readers, statistics and derived collections. The view owns this provider and closes it on disposal.
   */
  trait FilterProvider extends Closeable {
    /** Validate configuration before the view takes ownership of delegates and this provider. */
    def validate(stores: Seq[MergedSource[DataStore]]): Unit = {}

    /** Select delegates for schema discovery and routing, without resolving dynamic coverage. */
    def select(typeName: String, stores: Seq[MergedSource[DataStore]]): Seq[MergedSource[DataStore]] = stores

    /** Return one fixed filter per selected delegate, in the same order. Existing store filters are ANDed in. */
    def filters(typeName: String, stores: Seq[MergedSource[DataStore]], now: Instant): Seq[Filter]

    override def close(): Unit = {}
  }

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
   * @param acceptsType feature-type selection predicate
   * @tparam T type binding
   */
  case class MergedSource[T](
      source: T,
      sourceFilter: TypeOption[Filter] = TypeOption.empty,
      from: TypeOption[DateFilter] = TypeOption.empty,
      to: TypeOption[DateFilter] = TypeOption.empty,
      decorator: TaskDecorator = new TaskDecorator(None),
      acceptsType: String => Boolean = _ => true) {

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
      val dtg = dtgField(ev, typeName, from(typeName).isDefined || to(typeName).isDefined)
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

    private def dtgField(ev: MergedSourceSft[T], typeName: String, required: Boolean): String = {
      val sft = ev.sft(source, typeName)
      def firstDtg(): Option[String] =
        sft.getAttributeDescriptors.asScala.collectFirst {
          case d if classOf[Date].isAssignableFrom(d.getType.getBinding) => d.getLocalName
        }
      sft.getDtgField.orElse(firstDtg()).getOrElse {
        if (required) {
          throw new IllegalArgumentException(
            s"Schema '$typeName' does not have a date field for filtering: ${SimpleFeatureTypes.encodeType(sft)}")
        }
        null
      }
    }
  }

  object MergedSource {

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
    private[view] def isEmpty: Boolean = op.isEmpty
    def apply(task: Runnable): Runnable = op.fold(task)(_.apply(task))
  }
}
