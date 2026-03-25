/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionFilter, PartitionRange, RangeBuilder, SinglePartition}
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.text.DateParsing

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.{ChronoField, ChronoUnit, TemporalAdjusters, WeekFields}
import java.time.{DayOfWeek, ZoneOffset, ZonedDateTime}
import java.util.Date
import scala.util.control.NonFatal

/**
 * Legacy scheme, used for converter storage
 *
 * @param formatter date format
 * @param pattern pattern used by the date format
 * @param stepUnit time unit
 * @param step how many units per partition
 * @param dtg date attribute
 * @param dtgIndex date attribute index
 */
case class HierarchicalDateTimeScheme(
    formatter: DateTimeFormatter,
    pattern: String,
    stepUnit: ChronoUnit,
    step: Int,
    dtg: String,
    dtgIndex: Int
  ) extends PartitionScheme {

  import FilterHelper.ff
  import HierarchicalDateTimeScheme.Config

  import ChronoUnit._

  private val truncateToPartitionStart: ZonedDateTime => ZonedDateTime = {
    // note: `truncatedTo` is only valid up to DAYS, other units require additional steps
    val truncate: ZonedDateTime => ZonedDateTime = stepUnit match {
      case NANOS | MICROS | MILLIS | SECONDS | MINUTES | HOURS | DAYS =>
        dt => dt.truncatedTo(stepUnit)

      case WEEKS =>
        val adjuster = TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY) // note: java day_of_week defines monday as the first day
        dt => dt.`with`(adjuster).truncatedTo(DAYS)

      case MONTHS =>
        val adjuster = TemporalAdjusters.firstDayOfMonth()
        dt => dt.`with`(adjuster).truncatedTo(DAYS)

      case YEARS =>
        val adjuster = TemporalAdjusters.firstDayOfYear()
        dt => dt.`with`(adjuster).truncatedTo(DAYS)

      case _ =>
        throw new IllegalArgumentException(s"${HierarchicalDateTimeScheme.Config.StepUnitOpt} $stepUnit is not supported")
    }
    if (step == 1) {
      truncate
    } else {
      val chronoField = stepUnit match {
        case NANOS   => ChronoField.NANO_OF_SECOND
        case MICROS  => ChronoField.MICRO_OF_SECOND
        case MILLIS  => ChronoField.MILLI_OF_SECOND
        case SECONDS => ChronoField.SECOND_OF_MINUTE
        case MINUTES => ChronoField.MINUTE_OF_HOUR
        case HOURS   => ChronoField.HOUR_OF_DAY
        case DAYS    => ChronoField.DAY_OF_YEAR
        case WEEKS   => WeekFields.ISO.weekOfWeekBasedYear()
        case MONTHS  => ChronoField.MONTH_OF_YEAR
        case YEARS   => ChronoField.YEAR
        case _ => throw new IllegalArgumentException(s"${HierarchicalDateTimeScheme.Config.StepUnitOpt} $stepUnit is not supported")
      }
      val min = math.max(chronoField.range().getMinimum, 0).toInt // account for 1-based fields like {month, week, day}_of_year
      truncate.andThen { dt =>
        val steps = dt.get(chronoField) - min
        val remainder = steps % step
        if (remainder == 0) { dt } else { dt.`with`(chronoField, steps + min - remainder ) }
      }
    }
  }

  // TODO This may not be the best way to calculate max depth...
  // especially if we are going to use other separators
  val depth: Int = pattern.count(_ == '/') + 1

  override val name: String =
    Seq(
      HierarchicalDateTimeScheme.Name,
      s"${Config.DateTimeFormatOpt}=$pattern",
      s"${Config.StepUnitOpt}=${stepUnit.name()}",
      s"${Config.StepOpt}=$step",
      s"${Config.DtgAttribute}=$dtg"
    ).mkString(":")

  override def getPartition(feature: SimpleFeature): String = {
    val dt = ZonedDateTime.ofInstant(feature.getAttribute(dtgIndex).asInstanceOf[Date].toInstant, ZoneOffset.UTC)
    formatter.format(truncateToPartitionStart(dt))
  }

  override def getIntersectingPartitions(filter: Filter): Option[Seq[PartitionFilter]] = {
    val bounds = FilterHelper.extractIntervals(filter, dtg)
    if (bounds.isEmpty) {
      None
    } else if (bounds.disjoint) {
      Some(Seq.empty)
    } else {
      val rangeFilter = Some(filter)
      // TODO we should be able to remove some of the filters
      val builder = new RangeBuilder()
      bounds.values.foreach { bound =>
        if (bound.isEquals) {
          builder += SinglePartition(name, formatter.format(truncateToPartitionStart(bound.lower.value.get)))
        } else {
          val lower = bound.lower.value.fold("")(v => formatter.format(truncateToPartitionStart(v)))
          val upper = bound.upper.value.fold("zzz" /*TODO*/) { v =>
            val partition = formatter.format(truncateToPartitionStart(v))
            if (bound.upper.exclusive && formatter.format(truncateToPartitionStart(v.minus(1, ChronoUnit.MILLIS))) < partition) {
              partition
            } else {
              formatter.format(truncateToPartitionStart(v.plus(step, stepUnit)))
            }
          }
          builder += PartitionRange(name, lower, upper)
        }
      }
      Some(Seq(PartitionFilter(builder.result(), rangeFilter)))
    }
  }

  override def getCoveringFilter(partition: String): Filter = {
    val zdt = DateParsing.parse(partition, formatter)
    val start = DateParsing.format(zdt)
    val end = DateParsing.format(zdt.plus(step, stepUnit))
    ff.and(ff.greaterOrEqual(ff.property(dtg), ff.literal(start)), ff.less(ff.property(dtg), ff.literal(end)))
  }
}

object HierarchicalDateTimeScheme extends PartitionSchemeFactory {

  val Name = "datetime"

  def apply(format: String, stepUnit: ChronoUnit, step: Int, dtg: String, dtgIndex: Int): HierarchicalDateTimeScheme =
    HierarchicalDateTimeScheme(DateTimeFormatter.ofPattern(format), format, stepUnit, step, dtg, dtgIndex)

  object Config {
    val DateTimeFormatOpt: String = "datetime-format"
    val StepUnitOpt      : String = "step-unit"
    val StepOpt          : String = "step"
    val DtgAttribute     : String = "dtg-attribute"
  }

  object Formats {

    def apply(name: String): Option[Format] = all.find(_.name.equalsIgnoreCase(name))

    case class Format private[Formats](name: String, formatter: DateTimeFormatter, pattern: String, unit: ChronoUnit)

    private[Formats] object Format {
      def apply(name: String, format: String, unit: ChronoUnit): Format =
        Format(name, DateTimeFormatter.ofPattern(format), format, unit)
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
      Format("weekly", formatter, "YYYY/'W'ww", ChronoUnit.WEEKS)
    }

    private val all = Seq(Minute, Hourly, Daily, Weekly, Monthly, JulianMinute, JulianHourly, JulianDaily)
  }

  override def load(sft: SimpleFeatureType, scheme: String): Option[HierarchicalDateTimeScheme] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val opts = SchemeOpts(scheme)

    lazy val step = opts.opts.get(Config.StepOpt).map(_.toInt).getOrElse(1)
    lazy val dtg = opts.opts.get(Config.DtgAttribute).orElse(sft.getDtgField).getOrElse {
      throw new IllegalArgumentException(s"DateTime scheme requires valid attribute '${Config.DtgAttribute}'")
    }
    lazy val dtgIndex = Some(sft.indexOf(dtg)).filter(_ != -1).getOrElse {
      throw new IllegalArgumentException(s"Attribute '$dtg' does not exist in feature type ${sft.getTypeName}")
    }

    if (opts.name == Name) {
      val unit = opts.opts.get(Config.StepUnitOpt).map(c => ChronoUnit.valueOf(c.toUpperCase)).getOrElse {
        throw new IllegalArgumentException(s"DateTime scheme requires valid unit '${Config.StepUnitOpt}'")
      }
      val format = opts.opts.getOrElse(Config.DateTimeFormatOpt,
        throw new IllegalArgumentException(s"DateTime scheme requires valid format '${Config.DateTimeFormatOpt}'"))
      require(!format.endsWith("/"), "Format cannot end with a slash")
      val formatter = try { DateTimeFormatter.ofPattern(format) } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid date format '$format':", e)
      }
      Some(HierarchicalDateTimeScheme(formatter, format, unit, step, dtg, dtgIndex))
    } else {
      Formats(opts.name).map(f => HierarchicalDateTimeScheme(f.formatter, f.pattern, f.unit, step, dtg, dtgIndex))
    }
  }
}
