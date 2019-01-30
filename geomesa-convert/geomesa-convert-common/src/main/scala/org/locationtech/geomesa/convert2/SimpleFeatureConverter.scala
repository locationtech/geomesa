/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.io.{Closeable, InputStream}
import java.util.ServiceLoader

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.locationtech.geomesa.convert
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Converts input streams into simple features
  */
trait SimpleFeatureConverter extends Closeable {

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
    * Creates a context used for processing
    */
  def createEvaluationContext(globalParams: Map[String, Any] = Map.empty,
                              caches: Map[String, EnrichmentCache] = Map.empty,
                              counter: Counter = new DefaultCounter): EvaluationContext = {
    val keys = globalParams.keys.toIndexedSeq
    val values = keys.map(globalParams.apply).toArray
    EvaluationContext(keys, values, counter, caches)
  }
}

object SimpleFeatureConverter extends StrictLogging {

  import scala.collection.JavaConverters._

  val factories: List[SimpleFeatureConverterFactory] =
    ServiceLoader.load(classOf[SimpleFeatureConverterFactory]).asScala.toList

  // noinspection ScalaDeprecation
  private val factoriesV1 = ServiceLoader.load(classOf[convert.SimpleFeatureConverterFactory[_]]).asScala.toList

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
    val converters = factories.iterator.flatMap(_.apply(sft, config))
    if (converters.hasNext) { converters.next } else {
      val convertersV1 = factoriesV1.iterator.filter(_.canProcess(config)).map(_.buildConverter(sft, config))
      if (convertersV1.hasNext) {
        val v1 = convertersV1.next
        logger.warn(s"Wrapping deprecated converter of class ${v1.getClass.getName}, converter will not be closed")
        new SimpleFeatureConverterWrapper(v1)
      } else {
        throw new IllegalArgumentException(s"Cannot find factory for ${sft.getTypeName}")
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
  def infer(is: () => InputStream, sft: Option[SimpleFeatureType]): Option[(SimpleFeatureType, Config)] = {
    factories.foldLeft[Option[(SimpleFeatureType, Config)]](None) { (res, f) =>
      res.orElse(WithClose(is())(in => f.infer(in, sft)))
    }
  }

  // noinspection ScalaDeprecation
  class SimpleFeatureConverterWrapper(converter: convert.SimpleFeatureConverter[_]) extends SimpleFeatureConverter {

    override def targetSft: SimpleFeatureType = converter.targetSFT

    override def process(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] =
      converter.process(is, ec)

    override def createEvaluationContext(globalParams: Map[String, Any],
                                         caches: Map[String, EnrichmentCache],
                                         counter: Counter): EvaluationContext = {
      converter.createEvaluationContext(globalParams, counter)
    }

    override def close(): Unit = converter.close()
  }
}
