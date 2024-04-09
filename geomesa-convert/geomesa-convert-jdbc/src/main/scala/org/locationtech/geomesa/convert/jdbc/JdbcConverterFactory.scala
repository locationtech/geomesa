/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.jdbc

import com.typesafe.config.Config
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.jdbc.JdbcConverter.JdbcConfig
import org.locationtech.geomesa.convert.jdbc.JdbcConverterFactory.JdbcConfigConvert
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicFieldConvert, BasicOptionsConvert, ConverterConfigConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import pureconfig.ConfigObjectCursor
import pureconfig.error.ConfigReaderFailures

import java.io.InputStream
import scala.util.{Failure, Try}

class JdbcConverterFactory extends AbstractConverterFactory[JdbcConverter, JdbcConfig, BasicField, BasicOptions](
  "jdbc", JdbcConfigConvert, BasicFieldConvert, BasicOptionsConvert) {

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = Failure(new NotImplementedError())
}

object JdbcConverterFactory {

  object JdbcConfigConvert extends ConverterConfigConvert[JdbcConfig] {

    override protected def decodeConfig(
        cur: ConfigObjectCursor,
        `type`: String,
        idField: Option[Expression],
        caches: Map[String, Config],
        userData: Map[String, Expression]): Either[ConfigReaderFailures, JdbcConfig] = {
      for { conn <- cur.atKey("connection").right.flatMap(_.asString).right } yield {
        JdbcConfig(`type`, conn, idField, caches, userData)
      }
    }

    override protected def encodeConfig(config: JdbcConfig, base: java.util.Map[String, AnyRef]): Unit = {
      base.put("connection", config.connection)
    }
  }
}
