/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import java.util.regex.Pattern

import com.typesafe.scalalogging.LazyLogging
import org.joda.time.{Period, Duration}
import org.joda.time.format.PeriodFormatterBuilder

import scala.util.{Failure, Success, Try}

object Suffixes {

  object Time extends LazyLogging {
    val format = new PeriodFormatterBuilder()
      .appendDays().appendSuffix("d")
      .appendHours().appendSuffix("h")
      .appendMinutes().appendSuffix("m")
      .appendSeconds().appendSuffix("s")
      .appendMillis().appendSuffix("ms")
      .toFormatter

    def duration(s: String): Option[Duration] = {
      Try[Duration](Period.parse(s.toLowerCase, format).toStandardDuration)
        .orElse(Try(Duration.millis(s.toLong)))
      match {
        case Success(d) => Some(d)
        case Failure(e) =>
          logger.error(s"Unable to parse duration $s", e)
          None
      }
    }

    def millis(s: String): Option[Long]  = duration(s).map(_.getMillis)
    def seconds(s: String): Option[Long] = duration(s).map(_.getStandardSeconds)
    def minutes(s: String): Option[Long] = duration(s).map(_.getStandardMinutes)
    def hours(s: String): Option[Long]   = duration(s).map(_.getStandardHours)
    def days(s: String): Option[Long]    = duration(s).map(_.getStandardDays)
  }

  object Memory extends LazyLogging {
    val memPattern = Pattern.compile("(\\d+)([kmgt]?)b?")

    def bytes(s: String): Option[Long] = {
      val m = memPattern.matcher(s.toLowerCase.trim)
      if (m.matches() && m.groupCount() == 2) {
        Try {
          val num: Long = m.group(1).toLong
          val suf = m.group(2)
          val mult: Long = suf match {
            case "k" => 1024l
            case "m" => 1024l * 1024l
            case "g" => 1024l * 1024l * 1024l
            case "t" => 1024l * 1024l * 1024l * 1024l
            case _ => 1l
          }
          (num, mult)
        } match {
          case Success((num, mult)) =>
            val res = num*mult
            if (res > 0) Some(res)
            else {
              logger.error(s"Arithmetic overflow parsing '$s'")
              None
            }
          case Failure(e) =>
            logger.error(s"Error parsing memory property from input '$s': ${e.getMessage}")
            None
        }
      } else {
        logger.error(s"Unable to match memory pattern from input '$s'")
        None
      }
    }
  }
}
