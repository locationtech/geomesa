/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.api.filter.expression.PropertyName
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.{Bounds, FilterHelper, FilterValues}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.locationtech.geomesa.utils.text.DateParsing

import java.io.Closeable
import java.time.format.DateTimeParseException
import java.time.{Duration, Instant, ZonedDateTime}
import java.util.Date
import java.util.concurrent.ConcurrentHashMap

/**
 * Provides filter for stores in a merged view
 */
trait MergedFilterProvider extends Closeable {

  import MergedFilterProvider.QueryContext

  import scala.collection.JavaConverters._

  /**
   * Initialize this provider based on provided configuration
   *
   * @param config config
   */
  def init(config: Map[String, String]): Unit

  /**
   * Create a filter for the given feature type
   *
   * @param sft feature type
   * @param context shared context for a given query
   * @return
   */
  def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter]

  /**
   * Gets the default dtg field from the schema, throwing an exception if it is not found
   *
   * @param sft simple feature type
   * @return
   */
  protected def dtgField(sft: SimpleFeatureType): String = {
    sft.getDtgField.orElse(firstDtg(sft)).getOrElse {
      throw new IllegalArgumentException(
        s"Schema '${sft.getTypeName}' does not have a date field for filtering: ${SimpleFeatureTypes.encodeType(sft)}")
    }
  }

  private def firstDtg(sft: SimpleFeatureType): Option[String] = {
    sft.getAttributeDescriptors.asScala.collectFirst {
      case d if classOf[Date].isAssignableFrom(d.getType.getBinding) => d.getLocalName
    }
  }
}

object MergedFilterProvider {

  import FilterHelper.ff

  /**
   * No filter
   *
   * @return
   */
  def none: MergedFilterProvider = NoProvider

  /**
   * Create a static filter
   *
   * @param filter filter
   * @return
   */
  def static(filter: Filter): MergedFilterProvider = StaticProvider(Option(filter))

  /**
   * Create an inclusive 'from' date filter, e.g. `dtg >= date`
   *
   * @param offsetOrDate either a static date string, or an ISO-8601 duration (such as '-P1D')
   * @return
   */
  def from(offsetOrDate: String): MergedFilterProvider =
    date(offsetOrDate, FromOffsetProvider.apply, FromDateProvider.apply)

  /**
   * Create an exclusive 'to' date filter, e.g. `dtg < date`
   *
   * @param offsetOrDate either a static date string, or an ISO-8601 duration (such as '-P1D')
   * @return
   */
  def to(offsetOrDate: String): MergedFilterProvider =
    date(offsetOrDate, ToOffsetProvider.apply, ToDateProvider.apply)

  /**
   * Merge multiple filters into a single predicate, joined by AND
   *
   * @param providers providers to join
   * @return
   */
  def and(providers: Seq[MergedFilterProvider]): MergedFilterProvider = {
    if (providers.isEmpty) {
      none
    } else if (providers.size == 1) {
      providers.head
    } else {
      AndProvider(providers)
    }
  }

  /**
   * Create a date or offset provider
   *
   * @param offsetOrDate offset or date string
   * @param offsetProvider offset provider, used if offsetOrDate is an offset
   * @param dateProvider date provider, used if offsetOrDate is a date
   * @return
   */
  private def date(
      offsetOrDate: String,
      offsetProvider: Duration => MergedFilterProvider,
      dateProvider: Date => MergedFilterProvider): MergedFilterProvider = {
    lazy val error =
      new IllegalArgumentException(s"Invalid date filter '$offsetOrDate' - could not parse as a duration nor a date")
    try { return offsetProvider(Duration.parse(offsetOrDate)) } catch {
      case e: DateTimeParseException => error.addSuppressed(e)
    }
    try { return dateProvider(DateParsing.parseDate(offsetOrDate)) } catch {
      case e: DateTimeParseException => error.addSuppressed(e)
    }
    throw error
  }

  /**
   * Shared context across a given query
   *
   * @param base original filter supplied to the query
   * @param now timestamp query was started
   * @param state shared state
   */
  case class QueryContext(
    base: Filter,
    now: Instant = Instant.now(),
    state: ConcurrentHashMap[String, Any] = new ConcurrentHashMap[String, Any]
  )

  /**
   * Provider that supports per-feature-type filters
   *
   * @param byType providers, keyed by feature type name
   * @param default default provider, for types not in the byType map
   */
  case class TypedProvider(byType: Map[String, MergedFilterProvider], default: Option[MergedFilterProvider])
      extends MergedFilterProvider {
    override def init(config: Map[String, String]): Unit = {}
    override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] =
      byType.get(sft.getTypeName).orElse(default).flatMap(_.apply(sft, context))
    override def close(): Unit = CloseQuietly.raise(default ++ byType.values)
  }

  private case class AndProvider(providers: Seq[MergedFilterProvider]) extends MergedFilterProvider {
    override def init(config: Map[String, String]): Unit = {}
    override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] =
      FilterHelper.filterListAsAnd(providers.flatMap(_.apply(sft, context)))
    override def close(): Unit = CloseQuietly.raise(providers)
  }

  private case class StaticProvider(filter: Option[Filter]) extends MergedFilterProvider {
    override def init(config: Map[String, String]): Unit = {}
    override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] = filter
    override def close(): Unit = {}
  }

  private case class FromOffsetProvider(offset: java.time.Duration) extends DateProvider {
    override protected def filter(dtg: PropertyName, now: Instant): Filter =
      ff.greaterOrEqual(dtg, ff.literal(Date.from(now.plus(offset))))
  }

  private case class ToOffsetProvider(offset: java.time.Duration) extends DateProvider {
    override protected def filter(dtg: PropertyName, now: Instant): Filter =
      ff.less(dtg, ff.literal(Date.from(now.plus(offset))))
  }

  private case class FromDateProvider(date: Date) extends DateProvider {
    override protected def filter(dtg: PropertyName, now: Instant): Filter = ff.greaterOrEqual(dtg, ff.literal(date))
  }

  private case class ToDateProvider(date: Date) extends DateProvider {
    override protected def filter(dtg: PropertyName, now: Instant): Filter = ff.less(dtg, ff.literal(date))
  }

  private trait DateProvider extends MergedFilterProvider with LazyLogging {

    protected def filter(dtg: PropertyName, now: Instant): Filter

    override def init(config: Map[String, String]): Unit = {}

    override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] = {
      val dtg = dtgField(sft)
      val dtgFilter = filter(ff.property(dtg), context.now)
      // check for disjoint dates between the store filter and the query filter
      // note: don't cache this call, as it can contain things like `currentDate()` that will change per invocation
      val disjoint = Some(FilterHelper.extractIntervals(dtgFilter, dtg)).filter(_.nonEmpty).flatMap { left =>
        val right = FilterHelper.extractIntervals(context.base, dtg)
        val merged = FilterValues.and[Bounds[ZonedDateTime]](Bounds.intersection)(left, right)
        if (merged.disjoint) {
          logger.debug(s"Suppressing query with filter (${ECQL.toCQL(context.base)}) AND (${ECQL.toCQL(dtgFilter)})")
          Some(Filter.EXCLUDE)
        } else {
          None
        }
      }
      disjoint.orElse(Some(dtgFilter))
    }

    override def close(): Unit = {}
  }

  private case object NoProvider extends MergedFilterProvider {
    override def init(config: Map[String, String]): Unit = {}
    override def apply(sft: SimpleFeatureType, context: QueryContext): Option[Filter] = None
    override def close(): Unit = {}
  }
}