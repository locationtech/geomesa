/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.{ChronoField, ChronoUnit, TemporalAdjusters, WeekFields}
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.SimplifiedFilter
import org.locationtech.geomesa.fs.storage.api.{NamedOptions, PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.locationtech.geomesa.utils.text.DateParsing
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

case class DateTimeScheme(formatter: DateTimeFormatter, stepUnit: ChronoUnit, step: Int, dtg: String, dtgIndex: Int)
    extends PartitionScheme {

  import ChronoUnit._

  import FilterHelper.ff
  import org.locationtech.geomesa.filter.{andOption, isTemporalFilter, partitionSubFilters}

  // note: `truncatedTo` is only valid up to DAYS, other units require additional steps
  private val truncate: ZonedDateTime => ZonedDateTime = stepUnit match {
    case NANOS | MICROS | MILLIS | SECONDS | MINUTES | HOURS | DAYS =>
      dt => dt.truncatedTo(stepUnit)

    case MONTHS =>
      val adjuster = TemporalAdjusters.firstDayOfMonth()
      dt => dt.`with`(adjuster).truncatedTo(DAYS)

    case YEARS =>
      val adjuster = TemporalAdjusters.firstDayOfYear()
      dt => dt.`with`(adjuster).truncatedTo(DAYS)

    case _ =>
      dt => DateParsing.parse(formatter.format(dt), formatter) // fall back to format and re-parse
  }

  // TODO This may not be the best way to calculate max depth...
  // especially if we are going to use other separators
  override val depth: Int = formatter.toString.count(_ == '/') + 1

  override val pattern: String = formatter.toString

  override def getPartitionName(feature: SimpleFeature): String =
    formatter.format(toInstant(feature.getAttribute(dtgIndex).asInstanceOf[Date]).atZone(ZoneOffset.UTC))

  override def getSimplifiedFilters(filter: Filter, partition: Option[String]): Option[Seq[SimplifiedFilter]] = {
    getCoveringPartitions(filter).map { case (covered, intersecting) =>
      val result = Seq.newBuilder[SimplifiedFilter]

      if (covered.nonEmpty) {
        // remove the temporal filter that we've already accounted for in our covered partitions
        val coveredFilter = andOption(partitionSubFilters(filter, isTemporalFilter(_, dtg))._2)
        result += SimplifiedFilter(coveredFilter.getOrElse(Filter.INCLUDE), covered, partial = false)
      }
      if (intersecting.nonEmpty) {
        result += SimplifiedFilter(filter, intersecting.distinct, partial = false)
      }

      partition match {
        case None => result.result
        case Some(p) =>
          val matched = result.result.find(_.partitions.contains(p))
          matched.map(_.copy(partitions = Seq(p))).toSeq
      }
    }
  }

  override def getIntersectingPartitions(filter: Filter): Option[Seq[String]] =
    getCoveringPartitions(filter).map { case (covered, intersecting) => covered ++ intersecting }

  override def getCoveringFilter(partition: String): Filter = {
    val zdt = DateParsing.parse(partition, formatter)
    val start = DateParsing.format(zdt)
    val end = DateParsing.format(zdt.plus(1, stepUnit))
    ff.and(ff.greaterOrEqual(ff.property(dtg), ff.literal(start)), ff.less(ff.property(dtg), ff.literal(end)))
  }

  private def getCoveringPartitions(filter: Filter): Option[(Seq[String], Seq[String])] = {
    val bounds = FilterHelper.extractIntervals(filter, dtg, handleExclusiveBounds = false)
    if (bounds.disjoint) {
      Some((Seq.empty, Seq.empty))
    } else if (bounds.isEmpty || !bounds.forall(_.isBoundedBothSides)) {
      None
    } else {
      // there should be no duplicates in covered partitions, as our bounds will not overlap,
      // but there may be multiple partial intersects with a given partition
      val covered = ListBuffer.empty[String]
      val intersecting = ListBuffer.empty[String]

      bounds.values.foreach { bound =>
        // note: we verified both sides are bounded above
        val lower = bound.lower.value.get
        val upper = bound.upper.value.get
        val start = truncate(lower)
        // `stepUnit.between` claims to be upper endpoint exclusive, but doesn't seem to be...
        // if exclusive end, subtract one milli so we don't search the whole next partition if it's on the endpoint
        // note: we only support millisecond resolution
        val end = truncate(if (bound.upper.inclusive) { upper } else { upper.minus(1, MILLIS) })
        val steps = stepUnit.between(start, end).toInt

        // do our endpoints match the partition boundary, or do we need to apply a filter to the first/last partition?
        val coveringFirst = bound.lower.inclusive && lower == start
        val coveringLast =
          (bound.upper.exclusive && upper == end.plus(1, stepUnit)) ||
              (bound.upper.inclusive && !upper.isBefore(end.plus(1, stepUnit).minus(1, MILLIS)))

        if (steps == 0) {
          if (coveringFirst && coveringLast) {
            covered += formatter.format(start)
          } else {
            intersecting += formatter.format(start)
          }
        } else {
          // add the first partition
          if (coveringFirst) {
            covered += formatter.format(start)
          } else {
            intersecting += formatter.format(start)
          }

          @tailrec
          def addSteps(step: Int, current: ZonedDateTime): Unit = {
            if (step == steps) {
              // last partition
              if (coveringLast) {
                covered += formatter.format(current)
              } else {
                intersecting += formatter.format(current)
              }
            } else {
              covered += formatter.format(current)
              addSteps(step + 1, current.plus(1, stepUnit))
            }
          }

          addSteps(1, start.plus(1, stepUnit))
        }
      }

      Some((covered, intersecting))
    }
  }
}

object DateTimeScheme {

  val Name = "datetime"

  def apply(format: String, stepUnit: ChronoUnit, step: Int, dtg: String, dtgIndex: Int): DateTimeScheme =
    DateTimeScheme(DateTimeFormatter.ofPattern(format), stepUnit, step, dtg, dtgIndex)

  object Config {
    val DateTimeFormatOpt: String = "datetime-format"
    val StepUnitOpt      : String = "step-unit"
    val StepOpt          : String = "step"
    val DtgAttribute     : String = "dtg-attribute"
  }

  object Formats {

    def apply(name: String): Option[Format] = all.find(_.name.equalsIgnoreCase(name))

    case class Format private[Formats] (name: String, formatter: DateTimeFormatter, unit: ChronoUnit)

    private[Formats] object Format {
      def apply(name: String, format: String, unit: ChronoUnit): Format =
        Format(name, DateTimeFormatter.ofPattern(format), unit)
    }

    val Minute       : Format = Format("minute",        "yyyy/MM/dd/HH/mm", ChronoUnit.MINUTES)
    val Hourly       : Format = Format("hourly",        "yyyy/MM/dd/HH",    ChronoUnit.HOURS  )
    val Daily        : Format = Format("daily",         "yyyy/MM/dd",       ChronoUnit.DAYS   )
    val Monthly      : Format = Format("monthly",       "yyyy/MM",          ChronoUnit.MONTHS )
    val JulianMinute : Format = Format("julian-minute", "yyyy/DDD/HH/mm",   ChronoUnit.MINUTES)
    val JulianHourly : Format = Format("julian-hourly", "yyyy/DDD/HH",      ChronoUnit.HOURS  )
    val JulianDaily  : Format = Format("julian-daily",  "yyyy/DDD",         ChronoUnit.DAYS   )

    // java.time doesn't seem to have a way to parse back out a year/week format...
    // to get around that we have to define the default day of week in the formatter
    val Weekly: Format = {
      val formatter =
        new DateTimeFormatterBuilder()
          .appendValue(WeekFields.ISO.weekBasedYear(), 4)
          .appendLiteral("/W")
          .appendValue(WeekFields.ISO.weekOfWeekBasedYear(), 2)
          .parseDefaulting(ChronoField.DAY_OF_WEEK, 1)
          .toFormatter()
      Format("weekly", formatter, ChronoUnit.WEEKS)
    }

    private val all = Seq(Minute, Hourly, Daily, Weekly, Monthly, JulianMinute, JulianHourly, JulianDaily)
  }

  class DateTimePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, config: NamedOptions): Option[PartitionScheme] = {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      lazy val step = config.options.get(Config.StepOpt).map(_.toInt).getOrElse(1)
      lazy val dtg = config.options.get(Config.DtgAttribute).orElse(sft.getDtgField).getOrElse {
        throw new IllegalArgumentException(s"DateTime scheme requires valid attribute '${Config.DtgAttribute}'")
      }
      lazy val dtgIndex = Some(sft.indexOf(dtg)).filter(_ != -1).getOrElse {
        throw new IllegalArgumentException(s"Attribute '$dtg' does not exist in feature type ${sft.getTypeName}")
      }

      if (config.name == Name) {
        val unit = config.options.get(Config.StepUnitOpt).map(c => ChronoUnit.valueOf(c.toUpperCase)).getOrElse {
          throw new IllegalArgumentException(s"DateTime scheme requires valid unit '${Config.StepUnitOpt}'")
        }
        val format = config.options.getOrElse(Config.DateTimeFormatOpt,
          throw new IllegalArgumentException(s"DateTime scheme requires valid format '${Config.DateTimeFormatOpt}'"))
        require(!format.endsWith("/"), "Format cannot end with a slash")
        val formatter = try { DateTimeFormatter.ofPattern(format) } catch {
          case NonFatal(e) => throw new IllegalArgumentException(s"Invalid date format '$format':", e)
        }
        Some(DateTimeScheme(formatter, unit, step, dtg, dtgIndex))
      } else {
        Formats(config.name).map(f => DateTimeScheme(f.formatter, f.unit, step, dtg, dtgIndex))
      }
    }
  }
}