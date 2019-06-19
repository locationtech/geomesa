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
import org.apache.commons.csv.CSVFormat
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.convert.Modes.{ErrorMode, ParseMode}
import org.locationtech.geomesa.convert.SimpleFeatureConverters.SimpleFeatureConverterWrapper
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert.text.DelimitedTextConverter._
import org.locationtech.geomesa.convert.text.DelimitedTextConverterFactory.{DelimitedTextConfigConvert, DelimitedTextOptionsConvert}
import org.locationtech.geomesa.convert2.AbstractConverter.BasicField
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicFieldConvert, ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, PrimitiveConvert}
import org.locationtech.geomesa.convert2.TypeInference.FunctionTransform
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.transforms.Expression.{LiteralNull, TryExpression}
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, TypeInference}
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import pureconfig.error.ConfigReaderFailures
import pureconfig.{ConfigObjectCursor, ConfigReader}

import scala.util.Try

class DelimitedTextConverterFactory
    extends AbstractConverterFactory[DelimitedTextConverter, DelimitedTextConfig, BasicField, DelimitedTextOptions]
      with org.locationtech.geomesa.convert.SimpleFeatureConverterFactory[String] {

  import scala.collection.JavaConverters._

  override protected val typeToProcess: String = DelimitedTextConverterFactory.TypeToProcess

  override protected implicit def configConvert: ConverterConfigConvert[DelimitedTextConfig] = DelimitedTextConfigConvert
  override protected implicit def fieldConvert: FieldConvert[BasicField] = BasicFieldConvert
  override protected implicit def optsConvert: ConverterOptionsConvert[DelimitedTextOptions] = DelimitedTextOptionsConvert

  override def infer(is: InputStream, sft: Option[SimpleFeatureType]): Option[(SimpleFeatureType, Config)] = {

    val sampleSize = AbstractConverterFactory.inferSampleSize
    val lines = IOUtils.lineIterator(is, StandardCharsets.UTF_8.displayName).asScala.take(sampleSize).toSeq
    // if only a single line, assume that it isn't actually delimited text
    if (lines.lengthCompare(2) < 0) { None } else {
      magic(lines, sft).orElse(DelimitedTextConverter.inferences.flatMap(infer(_, lines, sft)).headOption)
    }
  }

  private def infer(
      format: CSVFormat,
      lines: Seq[String],
      sft: Option[SimpleFeatureType]): Option[(SimpleFeatureType, Config)] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableLike

    // : Seq[List[String]]
    val rows = lines.flatMap { line =>
      Try(format.parse(new StringReader(line)).iterator().next.iterator.asScala.toList).toOption
    }
    val counts = rows.map(_.length).distinct
    // try to verify that we actually have a delimited file
    // ensure that some lines parsed, that there were at most 3 different col counts, and that there were at least 2 cols
    if (counts.isEmpty || counts.lengthCompare(3) > 0 || counts.max < 2) { None } else {
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

      val options = DelimitedTextOptions(None, CharNotSpecified, CharNotSpecified, None,
        SimpleFeatureValidator.default, ParseMode.Default, ErrorMode(), StandardCharsets.UTF_8)

      val config = configConvert.to(converterConfig)
          .withFallback(fieldConvert.to(fields))
          .withFallback(optsConvert.to(options))
          .toConfig

      Some((schema, config))
    }
  }

  private def magic(lines: Seq[String], sft: Option[SimpleFeatureType]): Option[(SimpleFeatureType, Config)] = {
    if (!lines.head.startsWith("id,")) { None } else {
      val attempt = Try {
        val spec = WithClose(Formats.QuotedMinimal.parse(new StringReader(lines.head.drop(3)))) { result =>
          result.iterator().next.asScala.mkString(",")
        }
        val schema = SimpleFeatureTypes.createType("", spec)

        val converterConfig = DelimitedTextConfig(typeToProcess,
          formats.find(_._2 == Formats.QuotedMinimal).get._1, Some(Expression("$1")), Map.empty, Map.empty)

        var i = 1 // 0 is the whole record, 1 is the id
        val fields = schema.getAttributeDescriptors.asScala.map { d =>
          val bindings = ObjectType.selectType(d)
          val transform = bindings.head match {
            case ObjectType.STRING   => TypeInference.IdentityTransform
            case ObjectType.INT      => TypeInference.CastToInt
            case ObjectType.LONG     => TypeInference.CastToLong
            case ObjectType.FLOAT    => TypeInference.CastToFloat
            case ObjectType.DOUBLE   => TypeInference.CastToDouble
            case ObjectType.BOOLEAN  => TypeInference.CastToBoolean
            case ObjectType.DATE     => FunctionTransform("datetime(", ")")
            case ObjectType.UUID     => TypeInference.IdentityTransform
            case ObjectType.LIST     => FunctionTransform(s"parseList('${bindings(1)}',", ")")
            case ObjectType.MAP      => FunctionTransform(s"parseMap('${bindings(1)}->${bindings(2)}',", ")")
            case ObjectType.BYTES    => TypeInference.IdentityTransform
            case ObjectType.GEOMETRY =>
              bindings.drop(1).headOption.getOrElse(ObjectType.GEOMETRY) match {
                case ObjectType.POINT               => FunctionTransform("point(", ")")
                case ObjectType.LINESTRING          => FunctionTransform("linestring(", ")")
                case ObjectType.POLYGON             => FunctionTransform("polygon(", ")")
                case ObjectType.MULTIPOINT          => FunctionTransform("multipoint(", ")")
                case ObjectType.MULTILINESTRING     => FunctionTransform("multilinestring(", ")")
                case ObjectType.MULTIPOLYGON        => FunctionTransform("multipolygon(", ")")
                case ObjectType.GEOMETRY_COLLECTION => FunctionTransform("geometrycollection(", ")")
                case _                              => FunctionTransform("geometry(", ")")
              }

            case _ => throw new IllegalStateException(s"Unexpected binding: $bindings")
          }
          i += 1
          BasicField(d.getLocalName, Some(TryExpression(Expression(transform.apply(i)), LiteralNull)))
        }

        val options = DelimitedTextOptions(Some(1), CharNotSpecified, CharNotSpecified, None,
          SimpleFeatureValidator.default, ParseMode.Default, ErrorMode(), StandardCharsets.UTF_8)

        val config = configConvert.to(converterConfig)
            .withFallback(fieldConvert.to(fields))
            .withFallback(optsConvert.to(options))
            .toConfig

        (sft.getOrElse(schema), config)
      }
      attempt.toOption
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

  val TypeToProcess = "delimited-text"

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
    override protected def decodeOptions(
        cur: ConfigObjectCursor,
        validators: SimpleFeatureValidator,
        parseMode: ParseMode,
        errorMode: ErrorMode,
        encoding: Charset): Either[ConfigReaderFailures, DelimitedTextOptions] = {
      def option[T](key: String, reader: ConfigReader[T]): Either[ConfigReaderFailures, Option[T]] = {
        val value = cur.atKeyOrUndefined(key)
        if (value.isUndefined) { Right(None) } else { reader.from(value).right.map(Option.apply) }
      }

      def optionalChar(key: String): Either[ConfigReaderFailures, OptionalChar] = {
        val value = cur.atKeyOrUndefined(key)
        if (value.isUndefined) { Right(CharNotSpecified) } else {
          PrimitiveConvert.charConfigReader.from(value) match {
            case Right(c) => Right(CharEnabled(c))
            case Left(failures) =>
              if (PrimitiveConvert.stringConfigReader.from(value).right.exists(_.isEmpty)) {
                Right(CharDisabled)
              } else {
                Left(failures)
              }
          }
        }
      }

      for {
        skipLines <- option("skip-lines", PrimitiveConvert.intConfigReader).right
        quote     <- optionalChar("quote").right
        escape    <- optionalChar("escape").right
        delimiter <- option("delimiter", PrimitiveConvert.charConfigReader).right
      } yield {
        DelimitedTextOptions(skipLines, quote, escape, delimiter, validators, parseMode, errorMode, encoding)
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
