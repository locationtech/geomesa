/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import java.nio.charset.Charset

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.ErrorMode.ErrorMode
import org.locationtech.geomesa.convert.ParseMode.ParseMode
import org.locationtech.geomesa.convert.SimpleFeatureValidator
import org.locationtech.geomesa.convert.text.DelimitedTextConverter._
import org.locationtech.geomesa.convert.text.DelimitedTextConverterFactory.{DelimitedTextConfigConvert, DelimitedTextOptionsConvert}
import org.locationtech.geomesa.convert2.AbstractConverter.BasicField
import org.locationtech.geomesa.convert2.AbstractConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicFieldConvert, ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, PrimitiveConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import pureconfig.error.ConfigReaderFailures
import pureconfig.{ConfigObjectCursor, ConfigReader}

class DelimitedTextConverterFactory
    extends AbstractConverterFactory[DelimitedTextConverter, DelimitedTextConfig, BasicField, DelimitedTextOptions] {

  override protected val typeToProcess = "delimited-text"

  override protected implicit def configConvert: ConverterConfigConvert[DelimitedTextConfig] = DelimitedTextConfigConvert
  override protected implicit def fieldConvert: FieldConvert[BasicField] = BasicFieldConvert
  override protected implicit def optsConvert: ConverterOptionsConvert[DelimitedTextOptions] = DelimitedTextOptionsConvert
}

object DelimitedTextConverterFactory {

  object DelimitedTextConfigConvert extends ConverterConfigConvert[DelimitedTextConfig] {

    override protected def decodeConfig(cur: ConfigObjectCursor,
                                        typ: String,
                                        idField: Option[Expression],
                                        caches: Map[String, Config],
                                        userData: Map[String, Expression]): Either[ConfigReaderFailures, DelimitedTextConfig] = {
      for { format <- cur.atKey("format").right.flatMap(_.asString).right } yield {
        DelimitedTextConfig(typ, format, idField, caches, userData)
      }
    }

    override protected def encodeConfig(config: DelimitedTextConfig,
                                        base: java.util.Map[String, AnyRef]): Unit = {
      base.put("format", config.format)
    }

  }

  object DelimitedTextOptionsConvert extends ConverterOptionsConvert[DelimitedTextOptions] {
    override protected def decodeOptions(cur: ConfigObjectCursor,
                                         validators: SimpleFeatureValidator,
                                         parseMode: ParseMode,
                                         errorMode: ErrorMode,
                                         encoding: Charset,
                                         verbose: Boolean): Either[ConfigReaderFailures, DelimitedTextOptions] = {
      def option[T](key: String, reader: ConfigReader[T]): Either[ConfigReaderFailures, Option[T]] = {
        val value = cur.atKeyOrUndefined(key)
        if (value.isUndefined) { Right(None) } else { reader.from(value).right.map(Option.apply) }
      }

      for {
        skipLines <- option("skip-lines", PrimitiveConvert.intConfigReader).right
        quote     <- option("quote", PrimitiveConvert.charConfigReader).right
        escape    <- option("escape", PrimitiveConvert.charConfigReader).right
        delimiter <- option("delimiter", PrimitiveConvert.charConfigReader).right
      } yield {
        DelimitedTextOptions(skipLines, quote, escape, delimiter, validators, parseMode, errorMode, encoding, verbose)
      }
    }

    override protected def encodeOptions(options: DelimitedTextOptions, base: java.util.Map[String, AnyRef]): Unit = {
      options.skipLines.foreach(o => base.put("skip-lines", Int.box(o)))
      options.quote.foreach(o => base.put("quote", Char.box(o)))
      options.escape.foreach(o => base.put("escape", Char.box(o)))
      options.delimiter.foreach(o => base.put("delimiter", Char.box(o)))
    }
  }
}
