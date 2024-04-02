/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro.registry

import com.typesafe.config.Config
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.avro.registry.AvroSchemaRegistryConverter.AvroSchemaRegistryConfig
import org.locationtech.geomesa.convert.avro.registry.AvroSchemaRegistryConverterFactory.AvroSchemaRegistryConfigConvert
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicFieldConvert, BasicOptionsConvert, ConverterConfigConvert, OptionConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import pureconfig.ConfigObjectCursor
import pureconfig.error.ConfigReaderFailures

import java.io.InputStream
import scala.util.{Failure, Try}

class AvroSchemaRegistryConverterFactory
    extends AbstractConverterFactory[AvroSchemaRegistryConverter, AvroSchemaRegistryConfig, BasicField, BasicOptions](
      "avro-schema-registry", AvroSchemaRegistryConfigConvert, BasicFieldConvert, BasicOptionsConvert) {

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = Failure(new NotImplementedError())
}

object AvroSchemaRegistryConverterFactory {

  object AvroSchemaRegistryConfigConvert extends ConverterConfigConvert[AvroSchemaRegistryConfig] with OptionConvert {

    override protected def decodeConfig(
        cur: ConfigObjectCursor,
        `type`: String,
        idField: Option[Expression],
        caches: Map[String, Config],
        userData: Map[String, Expression]): Either[ConfigReaderFailures, AvroSchemaRegistryConfig] = {
      for { schemaRegistry <- cur.atKey("schema-registry").right.flatMap(_.asString).right } yield {
        AvroSchemaRegistryConfig(`type`, schemaRegistry, idField, caches, userData)
      }
    }

    override protected def encodeConfig(config: AvroSchemaRegistryConfig, base: java.util.Map[String, AnyRef]): Unit =
      base.put("schema-registry", config.schemaRegistry)
  }
}
