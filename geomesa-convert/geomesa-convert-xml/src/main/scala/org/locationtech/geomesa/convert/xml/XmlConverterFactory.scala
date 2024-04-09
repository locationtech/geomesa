/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.Modes.{ErrorMode, LineMode, ParseMode}
import org.locationtech.geomesa.convert.xml.XmlConverter._
import org.locationtech.geomesa.convert.xml.XmlConverterFactory.{XmlConfigConvert, XmlFieldConvert, XmlNamer, XmlOptionsConvert}
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{ConverterConfigConvert, ConverterOptionsConvert, FieldConvert, OptionConvert}
import org.locationtech.geomesa.convert2.TypeInference.{Namer, PathWithValues, TypeWithPath}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverterFactory, TypeInference}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.TextTools
import org.w3c.dom._
import pureconfig.error.{CannotConvert, ConfigReaderFailures}
import pureconfig.{ConfigObjectCursor, ConfigSource}

import java.io.InputStream
import java.nio.charset.{Charset, StandardCharsets}
import java.util.regex.Pattern
import javax.xml.xpath.XPathConstants
import scala.collection.mutable.ListBuffer
import scala.reflect.classTag
import scala.util.{Failure, Try}

class XmlConverterFactory extends AbstractConverterFactory[XmlConverter, XmlConfig, XmlField, XmlOptions](
  XmlConverterFactory.TypeToProcess, XmlConfigConvert, XmlFieldConvert, XmlOptionsConvert) {

  override protected def withDefaults(conf: Config): Config =
    super.withDefaults(conf).withFallback(ConfigFactory.load("xml-converter-defaults"))

  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      path: Option[String]): Option[(SimpleFeatureType, Config)] =
    infer(is, sft, path.map(EvaluationContext.inputFileParam).getOrElse(Map.empty)).toOption

  /**
   * Infer a configuration and simple feature type from an input stream, if possible
   *
   * Available hints:
   *  - `featurePath` - xpath expression pointing to the feature element
   *
   * @param is input
   * @param sft simple feature type, if known ahead of time
   * @param hints implementation specific hints about the input
   * @return
   */
  override def infer(
      is: InputStream,
      sft: Option[SimpleFeatureType],
      hints: Map[String, AnyRef]): Try[(SimpleFeatureType, Config)] = {
    val tryElements = Try {
      val parser = new DocParser(None)
      WithClose(XmlConverter.iterator(parser, is, StandardCharsets.UTF_8, LineMode.Multi, EvaluationContext.empty)) { iter =>
        iter.take(AbstractConverterFactory.inferSampleSize).toList
      }
    }

    tryElements.flatMap { elements =>
      val featurePath = hints.get(XmlConverterFactory.FeaturePathKey).map(_.toString)
      val namespaces = elements.headOption.map(XmlConverterFactory.getNamespaces).getOrElse(Map.empty)
      val xpathFactory =
        ConfigSource.fromConfig(withDefaults(XmlConverterFactory.TypeConfig))
            .loadOrThrow[XmlConfig](classTag[XmlConfig], XmlConfigConvert)
            .xpathFactory

      val tryFeatures = Try {
        featurePath match {
          case None => elements
          case Some(p) =>
            val xpath = XmlConverter.createXPath(xpathFactory, namespaces).compile(p)
            val features = elements.flatMap { element =>
              val nodeList = xpath.evaluate(element, XPathConstants.NODESET).asInstanceOf[NodeList]
              Seq.tabulate(nodeList.getLength)(i => nodeList.item(i).asInstanceOf[Element])
            }
            features.take(AbstractConverterFactory.inferSampleSize)
        }
      }

      tryFeatures.flatMap { features =>
        if (features.isEmpty) {
          Failure(new RuntimeException("Could not parse input as XML"))
        } else {
          Try {
            // track the properties in each feature
            // use linkedHashMap to retain insertion order
            val props = scala.collection.mutable.LinkedHashMap.empty[String, ListBuffer[Any]]

            val namer = new XmlNamer()
            features.foreach { feature =>
              namer.addRootElement(feature)
              XmlConverterFactory.parseNode(feature, "").foreach { case (k, v) =>
                props.getOrElseUpdate(k, ListBuffer.empty) += v
              }
            }

            val pathsAndValues = props.toSeq.map { case (path, values) => PathWithValues(path, values) }
            val inferredTypes = TypeInference.infer(pathsAndValues, sft.toRight("inferred-xml"), namer)

            val fieldConfig = inferredTypes.types.map { case TypeWithPath(path, inferredType) =>
              // account for optional nodes by wrapping transform with a try/null
              val transform = Some(Expression(s"try(${inferredType.transform.apply(0)},null)"))
              if (path.isEmpty) {
                DerivedField(inferredType.name, transform)
              } else {
                XmlPathField(inferredType.name, path, transform)
              }
            }

            val idField = Some(Expression("md5(stringToBytes(toString($0)))"))

            val xmlConfig =
              XmlConfig(typeToProcess, xpathFactory, namespaces, None, featurePath, idField, Map.empty, Map.empty)

            val config =
              configConvert.to(xmlConfig)
                  .withFallback(fieldConvert.to(fieldConfig))
                  .toConfig

            (inferredTypes.sft, config)
          }
        }
      }
    }
  }
}

object XmlConverterFactory {

  val TypeToProcess = "xml"

  val FeaturePathKey = "featurePath"

  private val TypeConfig = ConfigFactory.parseString(s"{type = $TypeToProcess}")

  object XmlConfigConvert extends ConverterConfigConvert[XmlConfig] with OptionConvert with StrictLogging {

    import scala.collection.JavaConverters._

    override protected def decodeConfig(
        cur: ConfigObjectCursor,
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
        val namespaces =
          namespace.valueOpt.map(_.unwrapped().asInstanceOf[java.util.Map[String, String]].asScala.toMap)
              .getOrElse(Map.empty)
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
        case f: XmlPathField => base.put("path", f.path)
        case _ => // no-op
      }
    }
  }

  object XmlOptionsConvert extends ConverterOptionsConvert[XmlOptions] {
    override protected def decodeOptions(
        cur: ConfigObjectCursor,
        validators: Seq[String],
        reporters: Seq[Config],
        parseMode: ParseMode,
        errorMode: ErrorMode,
        encoding: Charset): Either[ConfigReaderFailures, XmlOptions] = {
      def parse[T](key: String, values: Iterable[T]): Either[ConfigReaderFailures, T] = {
        cur.atKey(key).right.flatMap { value =>
          value.asString.right.flatMap { string =>
            values.find(_.toString.equalsIgnoreCase(string)) match {
              case Some(v) => Right(v)
              case None =>
                val msg = s"Must be one of: ${values.mkString(", ")}"
                value.failed(CannotConvert(value.valueOpt.map(_.toString).orNull, values.head.getClass.getSimpleName, msg))
            }
          }
        }
      }

      for {
        lineMode <- parse("line-mode", LineMode.values).right
      } yield {
        XmlOptions(validators, reporters, parseMode, errorMode, lineMode, encoding)
      }
    }

    override protected def encodeOptions(options: XmlOptions, base: java.util.Map[String, AnyRef]): Unit = {
      base.put("line-mode", options.lineMode.toString)
    }
  }

  /**
   * Attribute namer for xml elements
   */
  private class XmlNamer extends Namer {

    private val rootPrefixes = scala.collection.mutable.HashSet.empty[String]
    private val namespaceMatcher = Pattern.compile("(/@?)[^:/@]*:")

    /**
     * Add a root element, whose tag will be removed from any derived attribute names
     *
     * @param elem element
     */
    def addRootElement(elem: Element): Unit =
      rootPrefixes += s"/${elem.getTagName}/"

    override def apply(key: String): String = {
      val withoutRoot = rootPrefixes.find(key.startsWith) match {
        case None    => key
        case Some(p) => key.substring(p.length - 1)
      }
      // remove namespace prefixes from the attribute names
      val withoutPrefixes = namespaceMatcher.matcher(withoutRoot).replaceAll("$1")
      super.apply(withoutPrefixes)
    }
  }

  /**
   * Extract xmlns declarations from this element
   *
   * @param doc element
   * @return
   */
  private def getNamespaces(doc: Element): Map[String, String] = {
    val namespaces = Map.newBuilder[String, String]
    val attributes = doc.getAttributes
    var i = 0
    while (i < attributes.getLength) {
      val attr = attributes.item(i).asInstanceOf[Attr]
      if (attr.getName.startsWith("xmlns:")) {
        namespaces += attr.getName.substring(6) -> attr.getValue
      }
      i += 1
    }
    namespaces.result
  }

  /**
   * Parse an xml node
   *
   * @param node node
   * @param path xpath to the parent of the object
   * @return map of key is xpath to value, value is a string
   */
  private def parseNode(node: Node, path: String): Seq[(String, String)] = {
    // use linkedHashMap to preserve insertion order
    val builder = scala.collection.mutable.LinkedHashMap.empty[String, String]
    node match {
      case n: Element =>
        val p = s"$path/${n.getTagName}"
        Seq.tabulate(n.getAttributes.getLength)(n.getAttributes.item(_).asInstanceOf[Attr])
            .filterNot(_.getName.startsWith("xmlns:"))
            .sortBy(_.getLocalName)
            .foreach(attr => builder += s"$p/@${attr.getName}" -> attr.getValue)
        Seq.tabulate(n.getChildNodes.getLength)(n.getChildNodes.item)
            .foreach(child => builder ++= parseNode(child, p))

      case n: Text if !TextTools.isWhitespace(n.getData) =>
        builder += path -> n.getData

      case _ => // no-op
    }
    builder.toSeq
  }
}
