/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.text

import com.typesafe.config.Config
import org.apache.commons.csv.CSVFormat
import org.apache.commons.io.IOUtils
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.Modes.{ErrorMode, ParseMode}
import org.locationtech.geomesa.convert.text.DelimitedTextConverter._
import org.locationtech.geomesa.convert.text.DelimitedTextConverterFactory.{DelimitedTextConfigConvert, DelimitedTextOptionsConvert}
import org.locationtech.geomesa.convert2
import org.locationtech.geomesa.convert2.AbstractConverter.BasicField
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{BasicFieldConvert, ConverterConfigConvert, ConverterOptionsConvert, PrimitiveConvert}
import org.locationtech.geomesa.convert2.TypeInference.{FunctionTransform, PathWithValues, TypeWithPath}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.transforms.Expression.{LiteralNull, TryExpression}
import org.locationtech.geomesa.convert2.validators.SimpleFeatureValidator
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, TypeInference}
import org.locationtech.geomesa.utils.geotools.{ObjectType, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.TextTools
import pureconfig.error.ConfigReaderFailures
import pureconfig.{ConfigObjectCursor, ConfigReader}

import java.io.{InputStream, StringReader}
import java.nio.charset.{Charset, StandardCharsets}
import scala.util.{Failure, Try}

class DelimitedTextConverterFactory
    extends AbstractConverterFactory[DelimitedTextConverter, DelimitedTextConfig, BasicField, DelimitedTextOptions](
      DelimitedTextConverterFactory.TypeToProcess, DelimitedTextConfigConvert, BasicFieldConvert, DelimitedTextOptionsConvert) {

  import scala.collection.JavaConverters._

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      path: Option[String]): Option[(SimpleFeatureType, Config)] =
    infer(is, sft, path.map(EvaluationContext.inputFileParam).getOrElse(Map.empty)).toOption

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = {
    val tryLines = Try {
      val sampleSize = AbstractConverterFactory.inferSampleSize
      IOUtils.lineIterator(is, StandardCharsets.UTF_8.displayName).asScala.take(sampleSize).toSeq
    }
    tryLines.flatMap { lines =>
      // if only a single line, assume that it isn't actually delimited text
      if (lines.lengthCompare(2) < 0) { Failure(new RuntimeException("Not enough lines in input data")) } else {
        val attempts = Iterator.single(magic(lines, sft)) ++
            DelimitedTextConverter.inferences.iterator.map(infer(_, lines, sft))
        convert2.multiTry(attempts, new RuntimeException("Could not parse input as delimited text"))
      }
    }
  }

  private def infer(
      format: CSVFormat,
      lines: Seq[String],
      sft: Option[SimpleFeatureType]): Try[(SimpleFeatureType, Config)] = {
    // : Seq[List[String]]
    val rows = lines.flatMap { line =>
      if (TextTools.isWhitespace(line)) { None } else {
        Try(format.parse(new StringReader(line)).iterator().next.iterator.asScala.toList).toOption
      }
    }
    val counts = rows.map(_.length).distinct
    // try to verify that we actually have a delimited file
    // ensure that some lines parsed, that there were at most 3 different col counts, and that there were at least 2 cols
    if (counts.isEmpty || counts.lengthCompare(3) > 0 || counts.max < 2) {
      val f = if (format == Formats.Tabs) { "tsv" } else if (format == Formats.Quoted) { "quoted csv" }  else { "csv" }
      return Failure(new RuntimeException(s"Not a valid delimited text file using format: $f"))
    }
    Try {
      val names = sft match {
        case Some(s) =>
          s.getAttributeDescriptors.asScala.map(_.getLocalName)

        case None =>
          val values = rows.head.map(v => PathWithValues("", Seq(v)))
          if (TypeInference.infer(values, Left("")).types.forall(_.inferredType.typed == ObjectType.STRING)) {
            // assume the first row is headers
            rows.head
          } else {
            Seq.empty
          }
      }
      val nameIter = names.iterator
      val pathsAndValues = Seq.tabulate(counts.max) { col =>
        val values = rows.drop(1).map(vals => if (vals.lengthCompare(col) > 0) { vals(col) } else { null })
        PathWithValues(if (nameIter.hasNext) { nameIter.next } else { "" }, values)
      }
      val inferred =
        TypeInference.infer(pathsAndValues,
          sft.toRight(s"inferred-${if (format == Formats.Tabs) { "tsv" } else { "csv" }}"))
      val converterConfig = DelimitedTextConfig(typeToProcess, formats.find(_._2 == format).get._1,
        Some(Expression("md5(string2bytes($0))")), Map.empty, Map.empty)

      val fields = {
        var i = 0 // 0 is the whole record
        inferred.types.map { case TypeWithPath(_, inferredType) =>
          i += 1
          BasicField(inferredType.name, Some(Expression(inferredType.transform(i))))
        }
      }

      val options = DelimitedTextOptions(None, CharNotSpecified, CharNotSpecified, None,
        SimpleFeatureValidator.default, Seq.empty, ParseMode.Default, ErrorMode(), StandardCharsets.UTF_8)

      val config = configConvert.to(converterConfig)
          .withFallback(fieldConvert.to(fields.toSeq))
          .withFallback(optsConvert.to(options))
          .toConfig

      (inferred.sft, config)
    }
  }

  private def magic(lines: Seq[String], sft: Option[SimpleFeatureType]): Try[(SimpleFeatureType, Config)] = {
    if (!lines.head.startsWith("id,")) { Failure(new RuntimeException("Not a geomesa delimited export")) } else {
      Try {
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
          SimpleFeatureValidator.default, Seq.empty, ParseMode.Default, ErrorMode(), StandardCharsets.UTF_8)

        val config = configConvert.to(converterConfig)
            .withFallback(fieldConvert.to(fields.toSeq))
            .withFallback(optsConvert.to(options))
            .toConfig

        (sft.getOrElse(schema), config)
      }
    }
  }
}

object DelimitedTextConverterFactory {

  val TypeToProcess = "delimited-text"

  object DelimitedTextConfigConvert extends ConverterConfigConvert[DelimitedTextConfig] {

    override protected def decodeConfig(
        cur: ConfigObjectCursor,
        typ: String,
        idField: Option[Expression],
        caches: Map[String, Config],
        userData: Map[String, Expression]): Either[ConfigReaderFailures, DelimitedTextConfig] = {
      for { format <- cur.atKey("format").right.flatMap(_.asString).right } yield {
        DelimitedTextConfig(typ, format, idField, caches, userData)
      }
    }

    override protected def encodeConfig(
        config: DelimitedTextConfig,
        base: java.util.Map[String, AnyRef]): Unit = {
      base.put("format", config.format)
    }
  }

  object DelimitedTextOptionsConvert extends ConverterOptionsConvert[DelimitedTextOptions] {
    override protected def decodeOptions(
        cur: ConfigObjectCursor,
        validators: Seq[String],
        reporters: Seq[Config],
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
        DelimitedTextOptions(skipLines, quote, escape, delimiter, validators, reporters, parseMode, errorMode, encoding)
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
