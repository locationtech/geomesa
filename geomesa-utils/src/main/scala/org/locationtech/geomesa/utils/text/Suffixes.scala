/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

object Suffixes {

  object Time extends LazyLogging {

    // noinspection ScalaDeprecation
    def duration(s: String): Try[Duration] =
      Try(Duration(s))
          .orElse(Try(Duration(s.toLong, TimeUnit.MILLISECONDS)))
          .orElse(jodaMinute(s))

    def millis(s: String): Try[Long]  = duration(s).map(_.toMillis)
    def seconds(s: String): Try[Long] = duration(s).map(_.toMillis * 1000)
    def minutes(s: String): Try[Long] = duration(s).map(_.toMinutes)
    def hours(s: String): Try[Long]   = duration(s).map(_.toHours)
    def days(s: String): Try[Long]    = duration(s).map(_.toDays)

    // provide back compatibility with joda period parsing, which accepts 'm' for minutes
    @deprecated("joda parsing")
    private def jodaMinute(s: String): Try[Duration] = {
      if (s == null) { Failure(new NullPointerException()) } else {
        val replaced = s.replaceAll("m", "min") // scala parsing requires 'min', 'mins', 'minute' or 'minutes'
        val res = Try(Duration(replaced))
        if (res.isSuccess) {
          logger.warn("Parsed duration using deprecated minute parsing. Please update duration value: " +
              s"'$s' successfully parsed as '$replaced'")
        }
        res
      }
    }
  }

  object Memory extends LazyLogging {
    private val memPattern = Pattern.compile("(\\d+)([kmgt]?)b?")

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
