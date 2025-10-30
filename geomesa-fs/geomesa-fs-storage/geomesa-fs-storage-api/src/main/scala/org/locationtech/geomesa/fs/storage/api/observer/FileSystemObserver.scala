/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api.observer

import org.geotools.api.feature.simple.SimpleFeature

import java.io.{Closeable, Flushable}

/**
 * Observer trait
 */
trait FileSystemObserver extends Closeable with Flushable {

  /**
   * Write a feature
   *
   * @param feature feature
   */
  def apply(feature: SimpleFeature): Unit
}
