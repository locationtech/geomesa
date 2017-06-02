/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

import java.io.{InputStream, StringReader}
import java.nio.charset.StandardCharsets
import javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import javax.xml.xpath.{XPath, XPathConstants, XPathExpression, XPathFactory}

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BOMInputStream
import org.locationtech.geomesa.convert.LineMode.LineMode
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.w3c.dom.NodeList
import org.xml.sax.InputSource

import scala.collection.immutable.IndexedSeq
import scala.io.Source
import scala.util.control.NonFatal

class XMLConverter(val targetSFT: SimpleFeatureType,
                   val idBuilder: Expr,
                   val featurePath: Option[XPathExpression],
                   val xsd: Option[String],
                   val inputFields: IndexedSeq[Field],
                   val userDataBuilder: Map[String, Expr],
                   val parseOpts: ConvertParseOpts,
                   val lineMode: LineMode) extends ToSimpleFeatureConverter[String] with LazyLogging {

  private val docBuilder = {
    val factory = DocumentBuilderFactory.newInstance()
    factory.setNamespaceAware(false)
    factory.newDocumentBuilder()
  }
  private val xmlValidator = xsd.map { path =>
    val schemaFactory = SchemaFactory.newInstance(W3C_XML_SCHEMA_NS_URI)
    val xsdStream = getClass.getClassLoader.getResourceAsStream(path)
    val schema = schemaFactory.newSchema(new StreamSource(xsdStream))
    xsdStream.close()
    schema.newValidator()
  }

  override def fromInputType(i: String): Seq[Array[Any]] = {
    // if a schema is defined, validate it - this will throw an exception on failure
    xmlValidator.foreach(_.validate(new StreamSource(new StringReader(i))))
    // parse the document once, then extract each feature node and operate on it
    val root = docBuilder.parse(new InputSource(new StringReader(i))).getDocumentElement

    featurePath.map { path =>
      val nodeList = path.evaluate(root, XPathConstants.NODESET).asInstanceOf[NodeList]
      (0 until nodeList.getLength).map(i => Array[Any](nodeList.item(i)))
    }.getOrElse(Seq(Array[Any](root)))
  }

  // TODO GEOMESA-1039 more efficient InputStream processing for multi mode
  override def process(is: InputStream, ec: EvaluationContext = createEvaluationContext()): Iterator[SimpleFeature] = {
    val bomis = new BOMInputStream(is) // This detects and excludes the BOM if it exists
    lineMode match {
      case LineMode.Single =>
        processInput(Source.fromInputStream(bomis, StandardCharsets.UTF_8.displayName).getLines(), ec)
      case LineMode.Multi =>
        processInput(Iterator(IOUtils.toString(bomis, StandardCharsets.UTF_8.displayName)), ec)
    }
  }
}

class XMLConverterFactory extends AbstractSimpleFeatureConverterFactory[String] with LazyLogging {

  private val xpaths = new ThreadLocal[XPath]

  override protected val typeToProcess = "xml"

  override def buildConverter(sft: SimpleFeatureType, conf: Config): XMLConverter = {
    xpaths.set(getXPath(conf))
    try {
      super.buildConverter(sft, conf).asInstanceOf[XMLConverter]
    } finally {
      xpaths.remove()
    }
  }

  override protected def buildConverter(sft: SimpleFeatureType,
                                        conf: Config,
                                        idBuilder: Expr,
                                        fields: IndexedSeq[Field],
                                        userDataBuilder: Map[String, Expr],
                                        parseOpts: ConvertParseOpts): XMLConverter = {
    // feature path can be any xpath that resolves to a node set (or a single node)
    // it can be absolute, or relative to the root node
    val featurePath = if (!conf.hasPath("feature-path")) { None } else {
      Some(xpaths.get.compile(conf.getString("feature-path")))
    }
    val xsd         = if (conf.hasPath("xsd")) Some(conf.getString("xsd")) else None
    val lineMode    = LineMode.getLineMode(conf)

    new XMLConverter(sft, idBuilder, featurePath, xsd, fields, userDataBuilder, parseOpts, lineMode)
  }

  override protected def buildField(field: Config): Field = {
    val name = field.getString("name")
    val transform = if (field.hasPath("transform")) {
      Transformers.parseTransform(field.getString("transform"))
    } else {
      null
    }
    if (field.hasPath("path")) {
      // path can be absolute, or relative to the feature node
      // it can also include xpath functions to manipulate the result
      XMLField(name, xpaths.get.compile(field.getString("path")), transform)
    } else {
      SimpleField(name, transform)
    }
  }

  private def getXPath(conf: Config): XPath = {
    val provider = if (conf.hasPath("xpath-factory")) {
      conf.getString("xpath-factory")
    } else {
      // use saxon if available - though we can't distribute it due to CQs
      "net.sf.saxon.xpath.XPathFactoryImpl"
    }

    val factory = try {
      val res = XPathFactory.newInstance(XPathFactory.DEFAULT_OBJECT_MODEL_URI, provider, getClass.getClassLoader)
      logger.info(s"Loaded xpath factory ${res.getClass}")
      res
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Unable to load xpath provider '$provider': ${e.toString}. " +
            "Xpath queries may be slower - check your classpath")
        XPathFactory.newInstance(XPathFactory.DEFAULT_OBJECT_MODEL_URI)
    }
    factory.newXPath()
  }
}

case class XMLField(name: String, expression: XPathExpression, transform: Expr) extends Field {

  private val mutableArray = Array.ofDim[Any](1)

  override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
    mutableArray(0) = expression.evaluate(args(0))
    if (transform == null) {
      mutableArray(0)
    } else {
      super.eval(mutableArray)
    }
  }
}
