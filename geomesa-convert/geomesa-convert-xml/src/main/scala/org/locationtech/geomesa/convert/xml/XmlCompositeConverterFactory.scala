/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.convert.xml.XmlConverter.{XmlConfig, XmlOptions}
import org.locationtech.geomesa.convert.xml.XmlConverterFactory.XmlConfigConvert
import org.locationtech.geomesa.convert2.AbstractConverterFactory.ConverterConfigConvert
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, ParsingConverter, SimpleFeatureConverter, SimpleFeatureConverterFactory}
import org.opengis.feature.simple.SimpleFeatureType
import org.w3c.dom.Element
import pureconfig.ConfigConvert

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
        val xsd = pureconfig.loadConfigOrThrow[XmlConfig](defaults).xsd
        val options = pureconfig.loadConfigOrThrow[XmlOptions](defaults)
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
        Some(new XmlCompositeConverter(sft, xsd, options.encoding, options.lineMode, options.errorMode, delegates))
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid configuration: ${e.getMessage}")
      }
    }
  }
}

object XmlCompositeConverterFactory {
  val TypeToProcess = "composite-xml"
}
