/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.avro

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.avro.AvroConverter.AvroConfig
import org.locationtech.geomesa.convert.avro.AvroConverterFactory.AvroConfigConvert
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicFieldConvert, BasicOptionsConvert, ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, OptionConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import pureconfig.ConfigObjectCursor
import pureconfig.error.{ConfigReaderFailures, FailureReason}

class AvroConverterFactory extends AbstractConverterFactory[AvroConverter, AvroConfig, BasicField, BasicOptions] {

  override protected val typeToProcess: String = "avro"

  override protected implicit def configConvert: ConverterConfigConvert[AvroConfig] = AvroConfigConvert
  override protected implicit def fieldConvert: FieldConvert[BasicField] = BasicFieldConvert
  override protected implicit def optsConvert: ConverterOptionsConvert[BasicOptions] = BasicOptionsConvert
}

object AvroConverterFactory {

  object AvroConfigConvert extends ConverterConfigConvert[AvroConfig] with OptionConvert {

    override protected def decodeConfig(cur: ConfigObjectCursor,
                                        `type`: String,
                                        idField: Option[Expression],
                                        caches: Map[String, Config],
                                        userData: Map[String, Expression]): Either[ConfigReaderFailures, AvroConfig] = {

      def schemaOrFile(schema: Option[String],
                       schemaFile: Option[String]): Either[ConfigReaderFailures, Either[String, String]] = {
        (schema, schemaFile) match {
          case (Some(s), None) => Right(Left(s))
          case (None, Some(s)) => Right(Right(s))
          case _ =>
            val reason = new FailureReason {
              override val description: String = "Exactly one of 'schema' or 'schema-file' must be defined"
            }
            cur.failed(reason)
        }
      }

      for {
        schema     <- optional(cur, "schema").right
        schemaFile <- optional(cur, "schema-file").right
        either     <- schemaOrFile(schema, schemaFile).right
      } yield {
        AvroConfig(`type`, either, idField, caches, userData)
      }
    }

    override protected def encodeConfig(config: AvroConfig, base: java.util.Map[String, AnyRef]): Unit = {
      config.schema match {
        case Left(s)  => base.put("schema", s)
        case Right(s) => base.put("schema-file", s)
      }
    }
  }
}
