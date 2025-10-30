/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.observer

import org.geotools.api.feature.simple.SimpleFeature

/**
 * Marker trait for writer hooks
 */
@deprecated("Moved to org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserver")
trait FileSystemObserver extends org.locationtech.geomesa.fs.storage.api.observer.FileSystemObserver {

  override def apply(feature: SimpleFeature): Unit = write(feature)

  def write(feature: SimpleFeature): Unit
}
