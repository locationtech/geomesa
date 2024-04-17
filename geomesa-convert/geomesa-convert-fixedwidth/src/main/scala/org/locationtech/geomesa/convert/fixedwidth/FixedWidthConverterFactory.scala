/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.fixedwidth

import com.typesafe.config.Config
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.fixedwidth.FixedWidthConverter._
import org.locationtech.geomesa.convert.fixedwidth.FixedWidthConverterFactory.FixedWidthFieldConvert
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicConfigConvert, BasicOptionsConvert, FieldConvert, OptionConvert, PrimitiveConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import pureconfig.ConfigObjectCursor
import pureconfig.error.ConfigReaderFailures

import java.io.InputStream
import scala.util.{Failure, Try}

class FixedWidthConverterFactory
    extends AbstractConverterFactory[FixedWidthConverter, BasicConfig, FixedWidthField, BasicOptions](
      "fixed-width", BasicConfigConvert, FixedWidthFieldConvert, BasicOptionsConvert) {

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = Failure(new NotImplementedError())
}

object FixedWidthConverterFactory {

  object FixedWidthFieldConvert extends FieldConvert[FixedWidthField] with OptionConvert {

    override protected def decodeField(cur: ConfigObjectCursor,
                                       name: String,
                                       transform: Option[Expression]): Either[ConfigReaderFailures, FixedWidthField] = {
      val startCur = cur.atKeyOrUndefined("start")
      val widthCur = cur.atKeyOrUndefined("width")
      if (startCur.isUndefined && widthCur.isUndefined) { Right(DerivedField(name, transform)) } else {
        for {
          start <- PrimitiveConvert.intConfigReader.from(startCur).right
          width <- PrimitiveConvert.intConfigReader.from(widthCur).right
        } yield {
          OffsetField(name, transform, start, width)
        }
      }
    }

    override protected def encodeField(field: FixedWidthField, base: java.util.Map[String, AnyRef]): Unit = {
      field match {
        case OffsetField(_, _, start, width) => base.put("start", Int.box(start)); base.put("width", Int.box(width))
        case _ =>
      }
    }
  }
}
