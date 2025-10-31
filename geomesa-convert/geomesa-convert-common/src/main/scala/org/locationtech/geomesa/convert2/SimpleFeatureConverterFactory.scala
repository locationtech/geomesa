/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType

import java.io.InputStream
import scala.util.{Failure, Try}

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
   * Infer a configuration and simple feature type from an input stream, if possible.
   *
   * @param is input
   * @param sft simple feature type, if known ahead of time
   * @param hints implementation specific hints about the input
   * @return
   */
  def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = {
    Failure(new UnsupportedOperationException("Not implemented"))
  }
}
