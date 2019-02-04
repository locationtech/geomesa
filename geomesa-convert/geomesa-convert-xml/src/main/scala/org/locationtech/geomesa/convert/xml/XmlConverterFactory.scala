/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

import java.io.ByteArrayInputStream
import java.nio.charset.{Charset, StandardCharsets}

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.locationtech.geomesa.convert.Modes.{ErrorMode, LineMode, ParseMode}
import org.locationtech.geomesa.convert.SimpleFeatureConverters.SimpleFeatureConverterWrapper
import org.locationtech.geomesa.convert.xml.XmlConverter._
import org.locationtech.geomesa.convert.xml.XmlConverterFactory.{XmlConfigConvert, XmlFieldConvert, XmlOptionsConvert}
import org.locationtech.geomesa.convert.{EvaluationContext, SimpleFeatureConverter, SimpleFeatureValidator}
import org.locationtech.geomesa.convert2.AbstractConverterFactory
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, OptionConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import pureconfig.ConfigObjectCursor
import pureconfig.error.{CannotConvert, ConfigReaderFailures}

class XmlConverterFactory extends AbstractConverterFactory[XmlConverter, XmlConfig, XmlField, XmlOptions]
    with org.locationtech.geomesa.convert.SimpleFeatureConverterFactory[String] {

  override protected val typeToProcess: String = XmlConverterFactory.TypeToProcess

  override protected implicit def configConvert: ConverterConfigConvert[XmlConfig] = XmlConfigConvert
  override protected implicit def fieldConvert: FieldConvert[XmlField] = XmlFieldConvert
  override protected implicit def optsConvert: ConverterOptionsConvert[XmlOptions] = XmlOptionsConvert

  override protected def withDefaults(conf: Config): Config =
    super.withDefaults(conf).withFallback(ConfigFactory.load("xml-converter-defaults"))

  // deprecated version one processing, needed to handle processInput and processSingleInput

  override def canProcess(conf: Config): Boolean =
    conf.hasPath("type") && conf.getString("type").equalsIgnoreCase(typeToProcess)

  override def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[String] = {
    val converter = apply(sft, conf).orNull.asInstanceOf[XmlConverter]
    if (converter == null) {
      throw new IllegalStateException("Could not create converter - did you call canProcess()?")
    }
    // used to handle processInput and processSingleInput, which are always multiline
    lazy val multiline = if (converter.options.lineMode == LineMode.Multi) { converter } else {
      new XmlConverter(converter.targetSft, converter.config, converter.fields,
        converter.options.copy(lineMode = LineMode.Multi))
    }

    new SimpleFeatureConverterWrapper[String](converter) {
      override def processSingleInput(i: String, ec: EvaluationContext): Iterator[SimpleFeature] =
        register(multiline.process(new ByteArrayInputStream(i.getBytes(StandardCharsets.UTF_8)), ec))
    }
  }
}

object XmlConverterFactory {

  val TypeToProcess = "xml"

  object XmlConfigConvert extends ConverterConfigConvert[XmlConfig] with OptionConvert with StrictLogging {

    import scala.collection.JavaConverters._

    override protected def decodeConfig(cur: ConfigObjectCursor,
                                        `type`: String,
                                        idField: Option[Expression],
                                        caches: Map[String, Config],
                                        userData: Map[String, Expression]): Either[ConfigReaderFailures, XmlConfig] = {
      for {
        provider   <- cur.atKey("xpath-factory").right.flatMap(_.asString).right
        namespace  <- cur.atKey("xml-namespaces").right.flatMap(_.asObjectCursor).right
        path       <- optional(cur, "feature-path").right
        xsd        <- optional(cur, "xsd").right
      } yield {
        val namespaces = namespace.value.unwrapped().asInstanceOf[java.util.Map[String, String]].asScala.toMap
        XmlConfig(`type`, provider, namespaces, xsd, path, idField, caches, userData)
      }
    }

    override protected def encodeConfig(config: XmlConfig, base: java.util.Map[String, AnyRef]): Unit = {
      base.put("xpath-factory", config.xpathFactory)
      base.put("xml-namespaces", config.xmlNamespaces.asJava)
      config.featurePath.foreach(base.put("feature-path", _))
      config.xsd.foreach(base.put("xsd", _))
    }
  }

  object XmlFieldConvert extends FieldConvert[XmlField] with OptionConvert {
    override protected def decodeField(cur: ConfigObjectCursor,
                                       name: String,
                                       transform: Option[Expression]): Either[ConfigReaderFailures, XmlField] = {
      for { path <- optional(cur, "path").right } yield {
        path match {
          case None => DerivedField(name, transform)
          case Some(p) => XmlPathField(name, p, transform)
        }
      }
    }

    override protected def encodeField(field: XmlField, base: java.util.Map[String, AnyRef]): Unit = {
      field match {
        case f: XmlPathField => base.put("path", f.path.toString)
        case _ => // no-op
      }
    }
  }

  object XmlOptionsConvert extends ConverterOptionsConvert[XmlOptions] {
    override protected def decodeOptions(
        cur: ConfigObjectCursor,
        validators: SimpleFeatureValidator,
        parseMode: ParseMode,
        errorMode: ErrorMode,
        encoding: Charset): Either[ConfigReaderFailures, XmlOptions] = {
      def parse[T](key: String, values: Iterable[T]): Either[ConfigReaderFailures, T] = {
        cur.atKey(key).right.flatMap { value =>
          value.asString.right.flatMap { string =>
            values.find(_.toString.equalsIgnoreCase(string)) match {
              case Some(v) => Right(v.asInstanceOf[T])
              case None =>
                val msg = s"Must be one of: ${values.mkString(", ")}"
                value.failed(CannotConvert(value.value.toString, values.head.getClass.getSimpleName, msg))
            }
          }
        }
      }

      for {
        lineMode <- parse("line-mode", LineMode.values).right
      } yield {
        XmlOptions(validators, parseMode, errorMode, lineMode, encoding)
      }
    }

    override protected def encodeOptions(options: XmlOptions, base: java.util.Map[String, AnyRef]): Unit = {
      base.put("line-mode", options.lineMode.toString)
    }
  }
}