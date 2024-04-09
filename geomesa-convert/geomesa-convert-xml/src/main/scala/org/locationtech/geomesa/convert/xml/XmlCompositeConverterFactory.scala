/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.xml.XmlConverter.{XmlConfig, XmlOptions}
import org.locationtech.geomesa.convert.xml.XmlConverterFactory.XmlConfigConvert
import org.locationtech.geomesa.convert2.AbstractConverterFactory.ConverterConfigConvert
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, ParsingConverter, SimpleFeatureConverter, SimpleFeatureConverterFactory}
import org.w3c.dom.Element
import pureconfig.{ConfigConvert, ConfigSource}

import java.io.InputStream
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

class XmlCompositeConverterFactory extends SimpleFeatureConverterFactory with LazyLogging {

  import XmlCompositeConverterFactory.TypeToProcess

  import scala.collection.JavaConverters._

  override def apply(sft: SimpleFeatureType, conf: Config): Option[SimpleFeatureConverter] = {
    if (!conf.hasPath("type") || conf.getString("type") != TypeToProcess) { None } else {
      val defaults =
        AbstractConverterFactory.standardDefaults(conf, logger)
            .withFallback(ConfigFactory.load("xml-converter-defaults"))
      try {
        implicit val configConvert: ConverterConfigConvert[XmlConfig] = XmlConfigConvert
        implicit val optionsConvert: ConfigConvert[XmlOptions] = XmlConverterFactory.XmlOptionsConvert
        val xsd = ConfigSource.fromConfig(defaults).loadOrThrow[XmlConfig].xsd
        val options = ConfigSource.fromConfig(defaults).loadOrThrow[XmlOptions]
        val typeToProcess = ConfigValueFactory.fromAnyRef(XmlConverterFactory.TypeToProcess)
        val delegates = defaults.getConfigList("converters").asScala.map { base =>
          val conf = base.withFallback(defaults).withValue("type", typeToProcess)
          val converter = SimpleFeatureConverter(sft, conf) match {
            case c: ParsingConverter[Element] => c
            case c => throw new IllegalArgumentException(s"Expected XmlConverter but got: $c")
          }
          val predicate = Predicate(conf.getString("predicate"))
          (predicate, converter)
        }
        Some(new XmlCompositeConverter(sft, xsd, options.encoding, options.lineMode, options.errorMode, delegates.toSeq))
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid configuration: ${e.getMessage}")
      }
    }
  }

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = Failure(new NotImplementedError())
}

object XmlCompositeConverterFactory {
  val TypeToProcess = "composite-xml"
}
