/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.exporters

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Feature exporter that discards all output (i.e. write to /dev/null)
 */
object NullExporter extends FeatureExporter {

  override def start(sft: SimpleFeatureType): Unit = {}

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    features.foreach(_ => count += 1)
    Some(count)
  }

  override def close(): Unit = {}
}
