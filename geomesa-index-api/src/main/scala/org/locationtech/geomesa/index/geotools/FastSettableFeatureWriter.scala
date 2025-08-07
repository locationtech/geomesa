/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import org.geotools.api.data.SimpleFeatureWriter
import org.locationtech.geomesa.features.FastSettableFeature

/**
 * Mixin trait that allows optimizations on feature writes
 */
trait FastSettableFeatureWriter extends SimpleFeatureWriter {

  override def next(): FastSettableFeature
}
