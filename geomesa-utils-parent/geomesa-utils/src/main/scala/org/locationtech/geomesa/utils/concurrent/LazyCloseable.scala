/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.concurrent

import org.locationtech.geomesa.utils.io.IsCloseable

import java.io.Closeable

class LazyCloseable[T: IsCloseable](create: => T) extends Closeable {

  @volatile
  private var initialized = false

  lazy val instance: T = {
    initialized = true
    create
  }

  override def close(): Unit = {
    if (initialized) {
      implicitly[IsCloseable[T]].close(instance).get
    }
  }
}
