/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.io.{Closeable, InputStream}

import com.codahale.metrics.Counter
import com.typesafe.config.Config
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.locationtech.geomesa.convert.EvaluationContext.DelegatingEvaluationContext
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Converts input streams into simple features. SimpleFeatureConverters should be thread-safe. However,
 * a given EvaluationContext should only be used in a single thread at once.
 */
trait SimpleFeatureConverter extends Closeable with LazyLogging {

  import scala.collection.JavaConverters._

  /**
    * Result feature type
    */
  def targetSft: SimpleFeatureType

  /**
   * Process an input stream into simple features.
   *
   * This method should be thread-safe, as long as different evaluation contexts are used for each request.
   *
   * @param is input
   * @param ec evaluation context
   * @return
   */
  def process(is: InputStream, ec: EvaluationContext = createEvaluationContext()): CloseableIterator[SimpleFeature]

  /**
   * Create a context used for local state while processing. A context object is not thread-safe, and should
   * only be used in one thread at a time.
   *
   * @param globalParams global key-values to make accessible through the evaluation context
   * @return
   */
  def createEvaluationContext(globalParams: Map[String, Any] = Map.empty): EvaluationContext

  /**
   * Create a context used for local state while processing. A context object is not thread-safe, and should
   * only be used in one thread at a time.
   *
   * @param globalParams global key-values to make accessible through the evaluation context
   * @param success counter for tracking successful conversions
   * @param failure counter for tracking failed conversions
   * @return
   */
  def createEvaluationContext(globalParams: Map[String, Any], success: Counter, failure: Counter): EvaluationContext =
    // TODO remove default impl in next major release
    new DelegatingEvaluationContext(createEvaluationContext(globalParams))(success, failure)

  /**
   * Java API for `createEvaluationContext`
   *
   * @param globalParams global key-values to make accessible through the evaluation context
   * @return
   */
  final def createEvaluationContext(globalParams: java.util.Map[String, Any]): EvaluationContext =
    createEvaluationContext(globalParams.asScala.toMap)
}

object SimpleFeatureConverter extends StrictLogging {

  val factories: List[SimpleFeatureConverterFactory] = ServiceLoader.load[SimpleFeatureConverterFactory]()

  logger.debug(s"Found ${factories.size} factories: ${factories.map(_.getClass.getName).mkString(", ")}")

  /**
    * Create a converter
    *
    * @param sft simple feature type
    * @param config converter configuration
    * @return
    */
  def apply(sft: SimpleFeatureType, config: Config): SimpleFeatureConverter = {
    factories.toStream.flatMap(_.apply(sft, config)).headOption.getOrElse {
      throw new IllegalArgumentException(s"Cannot find factory for ${sft.getTypeName}")
    }
  }

  /**
    * Create a converter by name
    *
    * @param typeName simple feature type name
    * @param converterName converter name
    * @return
    */
  def apply(typeName: String, converterName: String): SimpleFeatureConverter = {
    val sft = SimpleFeatureTypeLoader.sftForName(typeName).getOrElse {
      throw new IllegalArgumentException(s"Unable to load SimpleFeatureType for typeName '$typeName'")
    }
    apply(sft, converterName)
  }

  /**
    * Create a converter by name
    *
    * @param sft simple feature type
    * @param converterName converter name
    * @return
    */
  def apply(sft: SimpleFeatureType, converterName: String): SimpleFeatureConverter = {
    val converter = ConverterConfigLoader.configForName(converterName).getOrElse {
      throw new IllegalArgumentException(s"Unable to load converter config for converter named '$converterName'")
    }
    apply(sft, converter)
  }

  /**
    * Infer a converter based on a data sample
    *
    * @param is input stream to convert
    * @param sft simple feature type, if known
    * @return
    */
  def infer(
      is: () => InputStream,
      sft: Option[SimpleFeatureType],
      path: Option[String] = None): Option[(SimpleFeatureType, Config)] = {
    factories.foldLeft[Option[(SimpleFeatureType, Config)]](None) { (res, f) =>
      res.orElse(WithClose(is())(in => f.infer(in, sft, path)))
    }
  }
}
