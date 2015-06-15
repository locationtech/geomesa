/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.util

import com.typesafe.scalalogging.slf4j.Logging

trait TryLoggingFailure {
  self: Logging =>

  def tryLoggingFailures[A](a: => A) = {
    try {
      a
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        throw e
    }
  }
}
