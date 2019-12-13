/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.io.{Closeable, InputStream}

import com.typesafe.config.Config
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.locationtech.geomesa.convert
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Converts input streams into simple features
  */
trait SimpleFeatureConverter extends Closeable with LazyLogging {

  import scala.collection.JavaConverters._

  /**
    * Result feature type
    */
  def targetSft: SimpleFeatureType

  /**
    * Process an input stream into simple features
    *
    * @param is input
    * @param ec evaluation context
    * @return
    */
  def process(is: InputStream, ec: EvaluationContext = createEvaluationContext()): CloseableIterator[SimpleFeature]

  /**
    * Create a context used for local state while processing
    *
    * @param globalParams global key-values to make accessible through the evaluation context
    * @return
    */
  def createEvaluationContext(globalParams: Map[String, Any] = Map.empty): EvaluationContext = {
    logger.warn(s"createEvaluationContext not implemented, using deprecated method: ${getClass.getName}")
    // noinspection ScalaDeprecation
    createEvaluationContext(globalParams, Map.empty, new DefaultCounter)
  }

  /**
    * Java API for `createEvaluationContext`
    *
    * @param globalParams global params, accessible in the converter for each input
    * @return
    */
  final def createEvaluationContext(globalParams: java.util.Map[String, Any]): EvaluationContext =
    createEvaluationContext(globalParams.asScala.toMap)

  /**
    * Creates a context used for local state while processing
    */
  // noinspection ScalaDeprecation
  @deprecated
  def createEvaluationContext(
      globalParams: Map[String, Any],
      caches: Map[String, EnrichmentCache],
      counter: Counter): EvaluationContext = {
    val keys = globalParams.keys.toIndexedSeq
    val values = keys.map(globalParams.apply).toArray
    EvaluationContext(keys, values, counter, caches)
  }
}

object SimpleFeatureConverter extends StrictLogging {

  val factories: List[SimpleFeatureConverterFactory] = ServiceLoader.load[SimpleFeatureConverterFactory]()

  // noinspection ScalaDeprecation
  private val factoriesV1 = ServiceLoader.load[convert.SimpleFeatureConverterFactory[_]]()

  logger.debug(s"Found ${factories.size + factoriesV1.size} factories: " +
      (factories ++ factoriesV1).map(_.getClass.getName).mkString(", "))

  /**
    * Create a converter
    *
    * @param sft simple feature type
    * @param config converter configuration
    * @return
    */
  def apply(sft: SimpleFeatureType, config: Config): SimpleFeatureConverter = {
    factories.toStream.flatMap(_.apply(sft, config)).headOption.getOrElse {
      factoriesV1.toStream.filter(_.canProcess(config)).map(_.buildConverter(sft, config)).headOption match {
        case None => throw new IllegalArgumentException(s"Cannot find factory for ${sft.getTypeName}")
        case Some(v1) =>
          logger.warn(s"Wrapping deprecated converter of class ${v1.getClass.getName}, converter will not be closed")
          new SimpleFeatureConverterWrapper(v1)
      }
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

  // noinspection ScalaDeprecation
  class SimpleFeatureConverterWrapper(converter: convert.SimpleFeatureConverter[_]) extends SimpleFeatureConverter {

    override def targetSft: SimpleFeatureType = converter.targetSFT

    override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] =
      converter.process(is, ec)

    override def createEvaluationContext(globalParams: Map[String, Any]): EvaluationContext =
      converter.createEvaluationContext(globalParams, new DefaultCounter)

    override def createEvaluationContext(globalParams: Map[String, Any],
                                         caches: Map[String, EnrichmentCache],
                                         counter: Counter): EvaluationContext = {
      converter.createEvaluationContext(globalParams, counter)
    }

    override def close(): Unit = converter.close()
  }
}
