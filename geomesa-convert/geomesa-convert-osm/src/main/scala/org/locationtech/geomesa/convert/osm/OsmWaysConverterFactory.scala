/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.osm

import java.util.Locale

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.osm.OsmWaysConverter.OsmWaysConfig
import org.locationtech.geomesa.convert.osm.OsmWaysConverterFactory.OsmWaysConfigConvert
import org.locationtech.geomesa.convert2.AbstractConverter.BasicOptions
import org.locationtech.geomesa.convert2.AbstractConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicOptionsConvert, ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, OptionConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import pureconfig.ConfigObjectCursor
import pureconfig.error.{CannotConvert, ConfigReaderFailures}

import scala.util.control.NonFatal

class OsmWaysConverterFactory
    extends AbstractConverterFactory[OsmWaysConverter, OsmWaysConfig, OsmField, BasicOptions] {

  override protected val typeToProcess = "osm-ways"

  override protected implicit def configConvert: ConverterConfigConvert[OsmWaysConfig] = OsmWaysConfigConvert
  override protected implicit def fieldConvert: FieldConvert[OsmField] = OsmFieldConvert
  override protected implicit def optsConvert: ConverterOptionsConvert[BasicOptions] = BasicOptionsConvert
}

object OsmWaysConverterFactory {

  object OsmWaysConfigConvert extends ConverterConfigConvert[OsmWaysConfig] with OptionConvert {

    override protected def decodeConfig(
        cur: ConfigObjectCursor,
        `type`: String,
        idField: Option[Expression],
        caches: Map[String, Config],
        userData: Map[String, Expression]): Either[ConfigReaderFailures, OsmWaysConfig] = {
      val format = optional(cur, "format").right.flatMap {
        case None => Right(OsmFormat.xml)
        case Some(f) =>
          try { Right(OsmFormat.withName(f.toLowerCase(Locale.US))) } catch {
            case NonFatal(_) =>
              val msg = s"Not a valid OSM format. Valid values are: '${OsmFormat.values.mkString("', '")}'"
              cur.failed(CannotConvert(f, "OsmOptions", msg))
          }
      }

      for { f <- format.right; jdbc <- optional(cur, "jdbc").right } yield {
        OsmWaysConfig(`type`, f, jdbc, idField, caches, userData)
      }
    }

    override protected def encodeConfig(config: OsmWaysConfig, base: java.util.Map[String, AnyRef]): Unit = {
      base.put("format", config.format.toString)
      config.jdbc.foreach(base.put("jdbc", _))
    }
  }
}
