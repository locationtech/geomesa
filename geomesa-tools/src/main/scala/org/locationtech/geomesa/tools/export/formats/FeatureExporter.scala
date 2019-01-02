/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.Closeable

import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Exports features in various formats. Usage pattern is:
  *
  *   start()
  *   export() - 0 to n times
  *   close()
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
