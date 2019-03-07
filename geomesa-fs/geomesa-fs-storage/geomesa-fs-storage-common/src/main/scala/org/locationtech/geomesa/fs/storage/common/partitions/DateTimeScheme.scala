/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalAdjusters}
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.{Collections, Date, Optional}

import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.{FilterPartitions, PartitionScheme, PartitionSchemeFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.annotation.tailrec

class DateTimeScheme(fmtStr: String,
                     stepUnit: ChronoUnit,
                     step: Int,
                     dtg: String,
                     leaf: Boolean) extends PartitionScheme {

  import ChronoUnit._

  private val fmt = DateTimeFormatter.ofPattern(fmtStr)

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
      dt => ZonedDateTime.parse(fmt.format(dt), fmt) // fall back to format and re-parse
  }

  override def getName: String = DateTimeScheme.Name

  override def getPartition(feature: SimpleFeature): String =
    fmt.format(feature.getAttribute(dtg).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC))

  override def getFilterPartitions(filter: Filter): Optional[java.util.List[FilterPartitions]] = {
    val bounds = FilterHelper.extractIntervals(filter, dtg, handleExclusiveBounds = false)
    if (bounds.disjoint) {
      Optional.of(Collections.emptyList())
    } else if (bounds.isEmpty || !bounds.forall(_.isBoundedBothSides)) {
      Optional.empty()
    } else {
      // there should be no duplicates in covered partitions, as our bounds will not overlap,
      // but there may be multiple partial intersects with a given partition
      val covered = new java.util.ArrayList[String]()
      val partial = new java.util.HashSet[String]()

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
            covered.add(fmt.format(start))
          } else {
            partial.add(fmt.format(start))
          }
        } else {
          // add the first partition
          if (coveringFirst) {
            covered.add(fmt.format(start))
          } else {
            partial.add(fmt.format(start))
          }

          @tailrec
          def addSteps(step: Int, current: ZonedDateTime): Unit = {
            if (step == steps) {
              // last partition
              if (coveringLast) {
                covered.add(fmt.format(current))
              } else {
                partial.add(fmt.format(current))
              }
            } else {
              covered.add(fmt.format(current))
              addSteps(step + 1, current.plus(1, stepUnit))
            }
          }

          addSteps(1, start.plus(1, stepUnit))
        }
      }

      val result = new java.util.ArrayList[FilterPartitions](2)

      if (!covered.isEmpty) {
        import org.locationtech.geomesa.filter.{andOption, isTemporalFilter, partitionSubFilters}

        // remove the temporal filter that we've already accounted for in our covered partitions
        val coveredFilter = andOption(partitionSubFilters(filter, isTemporalFilter(_, dtg))._2)
        result.add(new FilterPartitions(coveredFilter.getOrElse(Filter.INCLUDE), covered, false))
      }
      if (!partial.isEmpty) {
        result.add(new FilterPartitions(filter, new java.util.ArrayList(partial), false))
      }

      Optional.of(result)
    }
  }

  // TODO This may not be the best way to calculate max depth...
  // especially if we are going to use other separators
  override def getMaxDepth: Int = fmtStr.count(_ == '/')

  override def isLeafStorage: Boolean = leaf

  override def getOptions: java.util.Map[String, String] = {
    import DateTimeScheme.Config._

    import scala.collection.JavaConverters._

    Map(
      DtgAttribute      -> dtg,
      DateTimeFormatOpt -> fmtStr,
      StepUnitOpt       -> stepUnit.toString,
      StepOpt           -> step.toString,
      LeafStorage       -> leaf.toString
    ).asJava
  }

  override def equals(other: Any): Boolean = other match {
    case that: DateTimeScheme => that.getOptions.equals(getOptions)
    case _ => false
  }

  override def hashCode(): Int = getOptions.hashCode()
}

object DateTimeScheme {

  val Name = "datetime"

  object Config {
    val DateTimeFormatOpt: String = "datetime-format"
    val StepUnitOpt      : String = "step-unit"
    val StepOpt          : String = "step"
    val DtgAttribute     : String = "dtg-attribute"
    val LeafStorage      : String = LeafStorageConfig
  }

  object Formats {

    private val all = Seq(Minute, Hourly, Daily, Weekly, Monthly, JulianMinute, JulianHourly, JulianDaily)

    def apply(name: String): Option[Format] = all.find(_.name.equalsIgnoreCase(name))

    sealed case class Format private [Formats] (name: String, format: String, unit: ChronoUnit)

    object Minute       extends Format("minute",        "yyyy/MM/dd/HH/mm", ChronoUnit.MINUTES)
    object Hourly       extends Format("hourly",        "yyyy/MM/dd/HH",    ChronoUnit.HOURS  )
    object Daily        extends Format("daily",         "yyyy/MM/dd",       ChronoUnit.DAYS   )
    object Weekly       extends Format("weekly",        "yyyy/ww",          ChronoUnit.WEEKS  )
    object Monthly      extends Format("monthly",       "yyyy/MM",          ChronoUnit.MONTHS )
    object JulianMinute extends Format("julian-minute", "yyyy/DDD/HH/mm",   ChronoUnit.MINUTES)
    object JulianHourly extends Format("julian-hourly", "yyyy/DDD/HH",      ChronoUnit.HOURS  )
    object JulianDaily  extends Format("julian-daily",  "yyyy/DDD",         ChronoUnit.DAYS   )
  }

  class DateTimePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(name: String,
                      sft: SimpleFeatureType,
                      options: java.util.Map[String, String]): Optional[PartitionScheme] = {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      lazy val leaf = Option(options.get(Config.LeafStorage)).forall(java.lang.Boolean.parseBoolean)
      lazy val step = Option(options.get(Config.StepOpt)).map(_.toInt).getOrElse(1)
      lazy val dtg = {
        val field = Option(options.get(Config.DtgAttribute)).orElse(sft.getDtgField).getOrElse {
          throw new IllegalArgumentException(s"DateTime scheme requires valid attribute '${Config.DtgAttribute}'")
        }
        if (sft.indexOf(field) == -1) {
          throw new IllegalArgumentException(s"Attribute '$field' does not exist in simple feature type ${sft.getTypeName}")
        }
        field
      }

      if (name == Name) {
        val unit = Option(options.get(Config.StepUnitOpt)).map(c => ChronoUnit.valueOf(c.toUpperCase)).getOrElse {
          throw new IllegalArgumentException(s"DateTime scheme requires valid unit '${Config.StepUnitOpt}'")
        }
        val format = Option(options.get(Config.DateTimeFormatOpt)).getOrElse {
          throw new IllegalArgumentException(s"DateTime scheme requires valid format '${Config.DateTimeFormatOpt}'")
        }
        require(!format.endsWith("/"), "Format cannot end with a slash")

        Optional.of(new DateTimeScheme(format, unit, step, dtg, leaf))
      } else {
        import org.locationtech.geomesa.utils.conversions.JavaConverters._
        Formats(name).map(f => new DateTimeScheme(f.format, f.unit, step, dtg, leaf)).asJava
      }
    }
  }
}