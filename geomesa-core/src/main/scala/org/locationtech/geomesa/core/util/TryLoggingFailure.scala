package org.locationtech.geomesa.core.util

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
