/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import java.util.Locale

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal
import scala.util.{Success, Try}

object DurationParsing {

  // values taken from scala.concurrent.duration.Duration
  private val plusInfValues = Seq("Inf", "PlusInf", "+Inf")
  private val minusInfValues = Seq("MinusInf", "-Inf")

  /**
    * Parse a duration value in a case-insensitive fashion
    *
    * @param value string value
    * @throws java.lang.Exception if parsing fails
    * @return
    */
  @throws(classOf[Exception])
  def caseInsensitive(value: String): Duration = {
    if (value == null) {
      throw new NullPointerException("value is null")
    }
    try {
      // if regular parsing works, just return that
      Duration(value)
    } catch {
      case NonFatal(e) =>
        // try parsing lower case - that will match non-infinite strings
        Try(Duration(value.toLowerCase(Locale.US))) match {
          case Success(d) => d
          // compare plus/minus inf separately
          case _ if plusInfValues.exists(_.equalsIgnoreCase(value)) => Duration.Inf
          case _ if minusInfValues.exists(_.equalsIgnoreCase(value)) => Duration.MinusInf
          // throw the original exception if nothing works
          case _ => throw e
        }
    }
  }
}
