/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext

import java.io.InputStream
import scala.util.{Failure, Success, Try}

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
    * @param path file path, if there is a file available
    * @return
    */
  @deprecated("replaced with `infer(InputStream, Option[SimpleFeatureType], Map[String, Any])`")
  def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType] = None,
      path: Option[String] = None): Option[(SimpleFeatureType, Config)] = None

  /**
   * Infer a configuration and simple feature type from an input stream, if possible.
   *
   * The default implementation delegates to the deprecated `infer` method to help back-compatibility, but
   * should be overridden by implementing classes
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
    infer(is, sft, hints.get(EvaluationContext.InputFilePathKey).map(_.toString)) match {
      case Some(result) => Success(result)
      case None => Failure(new RuntimeException("Could not infer converter from input data"))
    }
  }
}
