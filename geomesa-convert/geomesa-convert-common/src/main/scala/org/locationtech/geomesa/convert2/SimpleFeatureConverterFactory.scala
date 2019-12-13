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
import com.typesafe.scalalogging.LazyLogging
import org.opengis.feature.simple.SimpleFeatureType

trait SimpleFeatureConverterFactory extends LazyLogging {

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
  @deprecated("Use infer(InputStream, Option[SimpleFeatureType], Option[String])")
  def infer(is: InputStream, sft: Option[SimpleFeatureType]): Option[(SimpleFeatureType, Config)] = None

  /**
    * Infer a configuration and simple feature type from an input stream, if possible
    *
    * @param is input
    * @param sft simple feature type, if known ahead of time
    * @param path file path, if there is a file available
    * @return
    */
  def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType] = None,
      path: Option[String] = None): Option[(SimpleFeatureType, Config)] = {
    // noinspection ScalaDeprecation
    val result = infer(is, sft) // delegate to old method for back compatibility
    if (result.isDefined) {
      logger.warn("Using deprecated infer method - please implement new API")
    }
    result
  }
}
