/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import java.io.{ByteArrayInputStream, InputStream, StringReader}
import java.nio.charset.{Charset, StandardCharsets}

import com.typesafe.config.Config
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert.Modes.ParseMode
import org.locationtech.geomesa.convert.SimpleFeatureConverters.SimpleFeatureConverterWrapper
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert.text.DelimitedTextConverter._
import org.locationtech.geomesa.convert.text.DelimitedTextConverterFactory.{DelimitedTextConfigConvert, DelimitedTextOptionsConvert}
import org.locationtech.geomesa.convert2.AbstractConverter.BasicField
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicFieldConvert, ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, PrimitiveConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, TypeInference}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import pureconfig.error.ConfigReaderFailures
import pureconfig.{ConfigObjectCursor, ConfigReader}

import scala.util.Try

class DelimitedTextConverterFactory
    extends AbstractConverterFactory[DelimitedTextConverter, DelimitedTextConfig, BasicField, DelimitedTextOptions]
      with org.locationtech.geomesa.convert.SimpleFeatureConverterFactory[String] {

  override protected val typeToProcess = "delimited-text"

  override protected implicit def configConvert: ConverterConfigConvert[DelimitedTextConfig] = DelimitedTextConfigConvert
  override protected implicit def fieldConvert: FieldConvert[BasicField] = BasicFieldConvert
  override protected implicit def optsConvert: ConverterOptionsConvert[DelimitedTextOptions] = DelimitedTextOptionsConvert

  override def infer(is: InputStream, sft: Option[SimpleFeatureType]): Option[(SimpleFeatureType, Config)] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.{RichIterator, RichTraversableLike}

    import scala.collection.JavaConverters._

    val sampleSize = AbstractConverterFactory.inferSampleSize
    val lines = IOUtils.lineIterator(is, StandardCharsets.UTF_8.displayName).asScala.take(sampleSize).toSeq
    // if only a single line, assume that it isn't actually delimited text
    if (lines.lengthCompare(2) < 0) { None } else {
      val results = DelimitedTextConverter.inferences.iterator.flatMap { format =>
        // : Seq[List[String]]
        val rows = lines.flatMap { line =>
          Try(format.parse(new StringReader(line)).iterator().next.iterator.asScala.toList).toOption
        }
        val counts = rows.map(_.length).distinct
        // try to verify that we actually have a delimited file
        // ensure that some lines parsed, that there were at most 3 different col counts, and that there were at least 2 cols
        if (counts.isEmpty || counts.lengthCompare(3) > 0 || counts.max < 2) { Iterator.empty } else {
          val names = sft match {
            case Some(s) =>
              s.getAttributeDescriptors.asScala.map(_.getLocalName)

            case None =>
              val firstRowTypes = TypeInference.infer(Seq(rows.head))
              if (firstRowTypes.exists(_.typed != ObjectType.STRING)) { Seq.empty } else {
                // assume the first row is headers
                rows.head.map(_.replaceAll("[^A-Za-z0-9]+", "_"))
              }
          }
          val types = TypeInference.infer(rows.drop(1), names)
          val schema =
            sft.filter(AbstractConverterFactory.validateInferredType(_, types.map(_.typed)))
                .getOrElse(TypeInference.schema("inferred-delimited-text", types))

          val converterConfig = DelimitedTextConfig(typeToProcess, formats.find(_._2 == format).get._1,
            Some(Expression("md5(string2bytes($0))")), Map.empty, Map.empty)

          val fields = schema.getAttributeDescriptors.asScala.mapWithIndex { case (d, i) =>
            BasicField(d.getLocalName, Some(Expression(types(i).transform(i + 1)))) // 0 is the whole record
          }

          val options = DelimitedTextOptions(None, None, None, None, SimpleFeatureValidator.default,
            ParseMode.Default, ErrorMode(), StandardCharsets.UTF_8, verbose = true)

          val config = configConvert.to(converterConfig)
              .withFallback(fieldConvert.to(fields))
              .withFallback(optsConvert.to(options))
              .toConfig

          Iterator.single((schema, config))
        }
      }

      results.headOption
    }
  }

  // deprecated version one processing, needed to handle skip lines

  override def canProcess(conf: Config): Boolean =
    conf.hasPath("type") && conf.getString("type").equalsIgnoreCase(typeToProcess)

  override def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[String] = {
    val converter = apply(sft, conf).orNull.asInstanceOf[DelimitedTextConverter]
    if (converter == null) {
      throw new IllegalStateException("Could not create converter - did you call canProcess()?")
    }
    // used to handle processSingleInput, which doesn't skip lines
    lazy val skipless = if (converter.options.skipLines.forall(_ < 1)) { converter } else {
      new DelimitedTextConverter(converter.targetSft, converter.config, converter.fields,
        converter.options.copy(skipLines = None))
    }

    new SimpleFeatureConverterWrapper[String](converter) {
      override def processInput(is: Iterator[String], ec: EvaluationContext): Iterator[SimpleFeature] =
        converter.options.skipLines.map(is.drop).getOrElse(is).flatMap(processSingleInput(_, ec))

      override def processSingleInput(i: String, ec: EvaluationContext): Iterator[SimpleFeature] =
        register(skipless.process(new ByteArrayInputStream(i.getBytes(StandardCharsets.UTF_8)), ec))
    }
  }
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
