/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.pathfilter

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.PathFilter
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.Bounds.Bound
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.fs.storage.api.NamedOptions

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.regex.Pattern
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class DtgPathFiltering(attribute: String, pattern: Pattern, format: DateTimeFormatter, buffer: Duration)
    extends PathFiltering with LazyLogging {

  def apply(filter: Filter): PathFilter = {
    val filterIntervals = FilterHelper.extractIntervals(filter, attribute, handleExclusiveBounds = true)
    path => try {
      val time = parseDtg(path.getName).toInstant
      val millis = buffer.toMillis
      val lower = ZonedDateTime.ofInstant(time.minusMillis(millis), ZoneOffset.UTC)
      val upper = ZonedDateTime.ofInstant(time.plusMillis(millis), ZoneOffset.UTC)
      val buffered = Bounds(Bound.inclusive(lower), Bound.inclusive(upper))
      val included = filterIntervals.exists(bounds => bounds.intersects(buffered))
      logger.whenDebugEnabled {
        if (included) {
          logger.debug(s"Including path ${path.getName} for filter $filter")
        } else {
          logger.debug(s"Excluding path ${path.getName} for filter $filter")
        }
      }
      included
    } catch {
      case NonFatal(ex) =>
        logger.warn(s"Failed to evaluate filter for path '${path.getName}'", ex)
        true
    }
  }

  private def parseDtg(name: String): ZonedDateTime = {
    Option(name)
      .map(pattern.matcher)
      .filter(_.matches)
      .filter(_.groupCount > 0)
      .map(_.group(1))
      .map(ZonedDateTime.parse(_, format))
      .getOrElse {
        throw new IllegalArgumentException(s"Failed to parse ${classOf[ZonedDateTime].getName} " +
          s"from file name '$name' for pattern '$pattern' and format '$format'")
      }
  }

  override def toString: String = {
    s"${this.getClass.getName}(attribute = $attribute, pattern = $pattern, format = $format, buffer = $buffer)"
  }
}

object DtgPathFiltering extends LazyLogging {

  val Name = "dtg"

  object Config {
    val Attribute = "attribute"
    val Pattern = "pattern"
    val Format = "format"
    val Buffer = "buffer"
  }

  class DtgPathFilteringFactory extends PathFilteringFactory {
    override def load(config: NamedOptions): Option[PathFiltering] = {
      if (config.name != Name) { None } else {
        val attribute = config.options.getOrElse(Config.Attribute, null)
        require(attribute != null, s"$Name path filter requires a dtg attribute config '${Config.Attribute}'")
        val patternConfig = config.options.getOrElse(Config.Pattern, null)
        require(patternConfig != null, s"$Name path filter requires a dtg pattern config '${Config.Pattern}'")
        val formatConfig = config.options.getOrElse(Config.Format, null)
        require(formatConfig != null, s"$Name path filter requires a dtg format config '${Config.Format}'")
        val bufferConfig = config.options.getOrElse(Config.Buffer, null)
        require(bufferConfig != null, s"$Name path filter requires a buffer duration config '${Config.Buffer}'")

        val pattern = Pattern.compile(patternConfig)
        val format = DateTimeFormatter.ofPattern(formatConfig).withZone(ZoneOffset.UTC)
        val buffer = Duration.apply(bufferConfig)
        val pathFiltering = new DtgPathFiltering(attribute, pattern, format, buffer)
        logger.info(s"Loaded PathFiltering: $pathFiltering")
        Some(pathFiltering)
      }
    }
  }
}
