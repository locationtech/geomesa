/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.json

import com.google.gson.JsonElement
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert2.AbstractConverter.BasicOptions
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, ParsingConverter, SimpleFeatureConverter, SimpleFeatureConverterFactory}
import pureconfig.{ConfigConvert, ConfigSource}

import java.io.InputStream
import scala.util.{Failure, Try}
import scala.util.control.NonFatal

class JsonCompositeConverterFactory extends SimpleFeatureConverterFactory with LazyLogging {

  import JsonCompositeConverterFactory.TypeToProcess

  import scala.collection.JavaConverters._

  override def apply(sft: SimpleFeatureType, conf: Config): Option[SimpleFeatureConverter] = {
    if (!conf.hasPath("type") || conf.getString("type") != TypeToProcess) { None } else {
      val defaults = AbstractConverterFactory.standardDefaults(conf, logger)
      try {
        implicit val convert: ConfigConvert[BasicOptions] = AbstractConverterFactory.BasicOptionsConvert
        val options = ConfigSource.fromConfig(defaults).loadOrThrow[BasicOptions]
        val typeToProcess = ConfigValueFactory.fromAnyRef(JsonConverterFactory.TypeToProcess)
        val delegates = defaults.getConfigList("converters").asScala.map { base =>
          val conf = base.withFallback(defaults).withValue("type", typeToProcess)
          val converter = SimpleFeatureConverter(sft, conf) match {
            case c: ParsingConverter[JsonElement] => c
            case c => throw new IllegalArgumentException(s"Expected JsonConverter but got: $c")
          }
          val predicate = Predicate(conf.getString("predicate"))
          (predicate, converter)
        }
        Some(new JsonCompositeConverter(sft, options.encoding, options.errorMode, delegates.toSeq))
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

object JsonCompositeConverterFactory {
  val TypeToProcess = "composite-json"
}
