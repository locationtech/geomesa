/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.fixedwidth

import org.locationtech.geomesa.convert.fixedwidth.FixedWidthConverter._
import org.locationtech.geomesa.convert.fixedwidth.FixedWidthConverterFactory.FixedWidthFieldConvert
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicConfigConvert, BasicOptionsConvert, ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, OptionConvert, PrimitiveConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import pureconfig.ConfigObjectCursor
import pureconfig.error.ConfigReaderFailures

class FixedWidthConverterFactory
    extends AbstractConverterFactory[FixedWidthConverter, BasicConfig, FixedWidthField, BasicOptions] {

  override protected val typeToProcess: String = "fixed-width"

  override protected implicit def configConvert: ConverterConfigConvert[BasicConfig] = BasicConfigConvert
  override protected implicit def fieldConvert: FieldConvert[FixedWidthField] = FixedWidthFieldConvert
  override protected implicit def optsConvert: ConverterOptionsConvert[BasicOptions] = BasicOptionsConvert
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
