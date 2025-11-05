/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.metrics

import com.typesafe.scalalogging.StrictLogging

trait MethodProfiling {

  protected def profile[R](onComplete: Long => Unit)(code: => R): R = {
    val start = System.currentTimeMillis
    val result: R = code
    onComplete(System.currentTimeMillis - start)
    result
  }

  protected def profile[R](onComplete: (R, Long) => Unit)(code: => R): R = {
    val start = System.currentTimeMillis
    val result: R = code
    onComplete(result, System.currentTimeMillis - start)
    result
  }
}

trait DebugLogProfiling extends MethodProfiling with StrictLogging {
  protected def profile[R](message: String)(code: => R): R =
    profile(time => logger.debug(s"$message in ${time}ms"))(code)
}
