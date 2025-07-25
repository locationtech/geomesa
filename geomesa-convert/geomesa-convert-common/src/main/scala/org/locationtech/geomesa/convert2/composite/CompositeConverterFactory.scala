/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.composite

import com.typesafe.config.{Config, ConfigValueFactory}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.ConverterConfigLoader
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.convert2.{SimpleFeatureConverter, SimpleFeatureConverterFactory}

class CompositeConverterFactory extends SimpleFeatureConverterFactory {

  import scala.collection.JavaConverters._

  override def apply(sft: SimpleFeatureType, conf: Config): Option[SimpleFeatureConverter] = {
    if (!conf.hasPath("type") || conf.getString("type") != "composite-converter") { None } else {
      val converters: Seq[(Predicate, SimpleFeatureConverter)] =
        conf.getConfigList("converters").asScala.map { c =>
          val pred = Predicate(c.getString("predicate"))
          val converterName = c.getString("converter")
          val converter = if (conf.hasPath(converterName)) {
            val subconfig = conf.getConfig(converterName) // load from local conf (within composite converter)
            val withName =
              if (subconfig.hasPath(ConverterConfigLoader.ConverterNameKey)) {
                subconfig
              } else {
                subconfig.withValue(ConverterConfigLoader.ConverterNameKey, ConfigValueFactory.fromAnyRef(converterName))
              }
            SimpleFeatureConverter(sft, withName)
          } else {
            SimpleFeatureConverter(sft, converterName) // load from a global named reference
          }
          (pred, converter)
        }.toSeq
      Some(new CompositeConverter(sft, converters))
    }
  }
}
