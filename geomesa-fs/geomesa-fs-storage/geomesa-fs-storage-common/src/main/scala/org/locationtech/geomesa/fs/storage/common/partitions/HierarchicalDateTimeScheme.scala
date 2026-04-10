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
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionRange, RangeBuilder}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionKey
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

  val depth: Int = pattern.count(_ == '/') + 1

  override val name: String =
    HierarchicalDateTimeScheme.Name +
      s":${Config.DateTimeFormatOpt}=$pattern" +
      s":${Config.StepUnitOpt}=${stepUnit.name()}" +
      s":${Config.StepOpt}=$step" +
      s":${Config.DtgAttribute}=$dtg"

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val dt = ZonedDateTime.ofInstant(feature.getAttribute(dtgIndex).asInstanceOf[Date].toInstant, ZoneOffset.UTC)
    PartitionKey(name, formatter.format(truncateToPartitionStart(dt)))
  }

  override def getRangesForFilter(filter: Filter): Option[Seq[PartitionRange]] = {
    getBounds(filter).map { bounds =>
      val builder = new RangeBuilder()
      bounds.foreach { bound =>
        val lower = bound.lower.value.fold("")(v => formatter.format(truncateToPartitionStart(v)))
        val upper = bound.upper.value.fold(HierarchicalDateTimeScheme.UnboundedUpper) { v =>
          val partition = formatter.format(truncateToPartitionStart(v))
          if (bound.upper.exclusive && formatter.format(truncateToPartitionStart(v.minus(1, ChronoUnit.MILLIS))) < partition) {
            partition
          } else {
            formatter.format(truncateToPartitionStart(v.plus(step, stepUnit)))
          }
        }
        builder += PartitionRange(name, lower, upper)
      }
      builder.result()
    }
  }

  override def getPartitionsForFilter(filter: Filter): Option[Seq[PartitionKey]] = {
    getBounds(filter).map { bounds =>
      bounds.flatMap { bound =>
        if (!bound.isBoundedBothSides) {
          throw new IllegalArgumentException(s"Can't enumerate an unbounded filter: ${ECQL.toCQL(filter)}")
        }
        val lower = bound.lower.value.get
        // `stepUnit.between` claims to be upper endpoint exclusive, but doesn't seem to be...
        val steps = stepUnit.between(lower, bound.upper.value.get).toInt / step
        Seq.tabulate(steps)(i => PartitionKey(name, formatter.format(lower.plus(step * i, stepUnit))))
      }
    }
  }

  override def getCoveringFilter(partition: PartitionKey): Filter = {
    val zdt = DateParsing.parse(partition.value, formatter)
    val start = DateParsing.format(zdt)
    val end = DateParsing.format(zdt.plus(step, stepUnit))
    ff.and(ff.greaterOrEqual(ff.property(dtg), ff.literal(start)), ff.less(ff.property(dtg), ff.literal(end)))
  }

  private def getBounds(filter: Filter): Option[Seq[Bounds[ZonedDateTime]]] = {
    val bounds = FilterHelper.extractIntervals(filter, dtg)
    if (bounds.isEmpty) {
      None
    } else if (bounds.disjoint) {
      Some(Seq.empty)
    } else {
      Some(bounds.values)
    }
  }
}

object HierarchicalDateTimeScheme extends PartitionSchemeFactory {

  val Name = "datetime"

  private val UnboundedUpper = "zzz"

  def apply(format: String, stepUnit: ChronoUnit, step: Int, dtg: String, dtgIndex: Int): HierarchicalDateTimeScheme =
    HierarchicalDateTimeScheme(DateTimeFormatter.ofPattern(format), format, stepUnit, step, dtg, dtgIndex)

  object Config {
    val DateTimeFormatOpt: String = "datetime-format"
    val StepUnitOpt      : String = "step-unit"
    val StepOpt          : String = "step"
    val DtgAttribute     : String = "dtg-attribute"
  }

  private object Formats {

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
