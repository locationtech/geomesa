/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.features.exporters

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.Closeable

/**
 * Exports features in various formats. Usage pattern is:
 *
 *  <ol>
 *    <li>start()</li>
 *    <li>export() - 0 to n times</li>
 *    <li>close()</li>
 *  </ol>
 */
trait FeatureExporter extends Closeable {

  /**
    * Start the export
    *
    * @param sft simple feature type
    */
  def start(sft: SimpleFeatureType): Unit

  /**
    * Export a batch of features
    *
    * @param features features to export
    * @return count of features exported, if available
    */
  def export(features: Iterator[SimpleFeature]): Option[Long]
}
