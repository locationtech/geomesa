/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
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
