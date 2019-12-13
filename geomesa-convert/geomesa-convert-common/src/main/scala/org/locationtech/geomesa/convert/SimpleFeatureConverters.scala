/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, ServiceLoader}

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert2
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig}
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypeLoader
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Simplified API to build SimpleFeatureType converters
  */
@deprecated("Replaced with org.locationtech.geomesa.convert2.SimpleFeatureConverter")
object SimpleFeatureConverters extends LazyLogging {

  import scala.collection.JavaConverters._

  private val factories = ServiceLoader.load(classOf[convert2.SimpleFeatureConverterFactory]).asScala.toList

  private val factoriesV1 = ServiceLoader.load(classOf[SimpleFeatureConverterFactory[_]]).asScala.toList

  logger.debug(s"Found ${factories.size + factoriesV1.size} factories: " +
      (factories ++ factoriesV1).map(_.getClass.getName).mkString(", "))

  @deprecated("Replaced with org.locationtech.geomesa.convert2.SimpleFeatureConverter.apply")
  def build[I](typeName: String, converterName: String): SimpleFeatureConverter[I] = {
    val sft = SimpleFeatureTypeLoader.sftForName(typeName)
      .getOrElse(throw new IllegalArgumentException(s"Unable to load SFT for typeName $typeName"))
    build[I](sft, converterName)
  }

  @deprecated("Replaced with org.locationtech.geomesa.convert2.SimpleFeatureConverter.apply")
  def build[I](sft: SimpleFeatureType, converterName: String): SimpleFeatureConverter[I] =
    ConverterConfigLoader.configForName(converterName).map(build[I](sft, _))
      .getOrElse(throw new IllegalArgumentException(s"Unable to find converter config for converterName $converterName"))

  @deprecated("Replaced with org.locationtech.geomesa.convert2.SimpleFeatureConverter.apply")
  def build[I](sft: SimpleFeatureType, converterConf: Config): SimpleFeatureConverter[I] = {
    factoriesV1.find(_.canProcess(converterConf)).map(_.buildConverter(sft, converterConf)) match {
      case Some(c) => c.asInstanceOf[SimpleFeatureConverter[I]]
      case None =>
        val converters = factories.iterator.flatMap(_.apply(sft, converterConf)).collect {
          case c: AbstractConverter[_, _, _, _] => c.asInstanceOf[AbstractConverter[_, _ <: ConverterConfig, _, _]]
        }
        if (converters.hasNext) { new SimpleFeatureConverterWrapper(converters.next) } else {
          throw new IllegalArgumentException(s"Cannot find converter factory for type ${sft.getTypeName}")
        }
    }
  }

  /**
    * Wrapper to present new converters under the old API
    *
    * @param converter new converter
    * @tparam I type bounds
    */
  class SimpleFeatureConverterWrapper[I](converter: AbstractConverter[_, _ <: ConverterConfig, _, _]) extends
      SimpleFeatureConverter[I] {

    private val open =
      Collections.newSetFromMap(new ConcurrentHashMap[CloseableIterator[SimpleFeature], java.lang.Boolean])

    protected def register(iter: CloseableIterator[SimpleFeature]): Iterator[SimpleFeature] = {
      open.add(iter)
      SelfClosingIterator(iter, { iter.close(); open.remove(iter) })
    }

    override lazy val caches: Map[String, EnrichmentCache] =
      converter.config.caches.map { case (k, v) => (k, EnrichmentCache(v)) }

    override def targetSFT: SimpleFeatureType = converter.targetSft

    override def processInput(is: Iterator[I], ec: EvaluationContext): Iterator[SimpleFeature] =
      is.flatMap(processSingleInput(_, ec))

    override def processSingleInput(i: I, ec: EvaluationContext): Iterator[SimpleFeature] = {
      i match {
        case s: String => process(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)), ec)
        case b: Array[Byte] => process(new ByteArrayInputStream(b), ec)
        case _ => throw new NotImplementedError()
      }
    }

    override def process(is: InputStream, ec: EvaluationContext): Iterator[SimpleFeature] =
      register(converter.process(is, ec))

    override def createEvaluationContext(globalParams: Map[String, Any], counter: Counter): EvaluationContext =
      converter.createEvaluationContext(globalParams, Map.empty, counter)

    override def close(): Unit = {
      open.asScala.foreach(CloseWithLogging.apply)
      caches.values.foreach(CloseWithLogging.apply)
      CloseWithLogging(converter)
    }
  }
}
