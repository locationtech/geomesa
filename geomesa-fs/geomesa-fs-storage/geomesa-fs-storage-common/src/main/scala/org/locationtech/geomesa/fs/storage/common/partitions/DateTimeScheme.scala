/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.{Date, Optional}

import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class DateTimeScheme(fmtStr: String,
                     stepUnit: ChronoUnit,
                     step: Int,
                     dtg: String,
                     leaf: Boolean) extends PartitionScheme {

  private val MinDateTime = ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  private val MaxDateTime = ZonedDateTime.of(9999, 12, 31, 23, 59, 59, 999000000, ZoneOffset.UTC)

  private val fmt = DateTimeFormatter.ofPattern(fmtStr)

  override def getName: String = DateTimeScheme.Name

  override def getPartition(feature: SimpleFeature): String =
    fmt.format(feature.getAttribute(dtg).asInstanceOf[Date].toInstant.atZone(ZoneOffset.UTC))

  override def getPartitions(filter: Filter): java.util.List[String] = {
    import scala.collection.JavaConverters._

    val bounds = FilterHelper.extractIntervals(filter, dtg, handleExclusiveBounds = true)
    val intervals = bounds.values.map { b =>
      (b.lower.value.getOrElse(MinDateTime), b.upper.value.getOrElse(MaxDateTime))
    }

    intervals.flatMap { case (start, end) =>
      val count = stepUnit.between(start, end).toInt + 1
      Seq.tabulate(count)(i => fmt.format(start.plus(step * i, stepUnit)))
    }.asJava
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
    val DateTimeFormatOpt   = "datetime-format"
    val StepUnitOpt         = "step-unit"
    val StepOpt             = "step"
    val DtgAttribute        = "dtg-attribute"
    val LeafStorage: String = SpatialPartitionSchemeConfig.LeafStorage
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