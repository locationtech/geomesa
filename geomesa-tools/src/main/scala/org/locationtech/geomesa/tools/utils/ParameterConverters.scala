/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import com.beust.jcommander.ParameterException
import com.beust.jcommander.converters.BaseConverter
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.Period
import org.joda.time.format.PeriodFormat
import org.locationtech.geomesa.tools.utils.DataFormats.DataFormat
import org.opengis.filter.Filter

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

object ParameterConverters {

  class HintConverter(name: String) extends BaseConverter[java.util.Map[String, String]](name) {
    override def convert(value: String): java.util.Map[String, String] = {
      try {
        val map = new java.util.HashMap[String, String]()
        value.split(";").foreach { part =>
          val Array(k, v) = part.split("=")
          map.put(k.trim, v.trim)
        }
        map
      } catch {
        case NonFatal(e) => throw new ParameterException(getErrorString(value, s"hint map: $e"))
      }
    }
  }

  class PeriodConverter(name: String) extends BaseConverter[Period](name) {
    private val format = PeriodFormat.getDefault
    override def convert(value: String): Period = {
      try {
        format.parsePeriod(value)
      } catch {
        case NonFatal(e) => throw new ParameterException(getErrorString(value, s"period: $e"))
      }
    }
  }

  class DurationConverter(name: String) extends BaseConverter[Duration](name) {
    override def convert(value: String): Duration = {
      try {
        Duration(value)
      } catch {
        case NonFatal(e) => throw new ParameterException(getErrorString(value, s"duration: $e"))
      }
    }
  }

  class FilterConverter(name: String) extends BaseConverter[Filter](name) {
    override def convert(value: String): Filter = {
      try {
        ECQL.toFilter(value)
      } catch {
        case NonFatal(e) => throw new ParameterException(getErrorString(value, s"filter: $e"))
      }
    }
  }

  class DataFormatConverter(name: String) extends BaseConverter[DataFormat](name) {
    override def convert(value: String): DataFormat = {
      try {
        DataFormats.values.find(_.toString.equalsIgnoreCase(value)).getOrElse {
          throw new ParameterException(s"Invalid format '$value'. Valid values are " +
              DataFormats.values.map(_.toString.toLowerCase).mkString("'", "', '", "'"))
        }
      } catch {
        case NonFatal(e) => throw new ParameterException(getErrorString(value, s"format: $e"))
      }
    }
  }
}
