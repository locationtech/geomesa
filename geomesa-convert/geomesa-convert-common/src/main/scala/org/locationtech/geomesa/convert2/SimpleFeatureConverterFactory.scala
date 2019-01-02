/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.io.InputStream

import com.typesafe.config.Config
import org.opengis.feature.simple.SimpleFeatureType

trait SimpleFeatureConverterFactory {

  /**
    * Create a simple feature converter, if possible from this configuration
    *
    * @param sft simple feature type
    * @param conf config
    * @return
    */
  def apply(sft: SimpleFeatureType, conf: Config): Option[SimpleFeatureConverter]

  /**
    * Infer a configuration and simple feature type from an input stream, if possible
    *
    * @param is input
    * @param sft simple feature type, if known ahead of time
    * @return
    */
  def infer(is: InputStream, sft: Option[SimpleFeatureType] = None): Option[(SimpleFeatureType, Config)] = None
}
