/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

import java.io.StringWriter
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.ConfigFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.{OutputKeys, Transformer, TransformerFactory}
import javax.xml.xpath.XPathExpression
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.utils.cache.SoftThreadLocal
import org.w3c.dom.Element

class XmlFunctionFactory extends TransformerFunctionFactory {

  import scala.collection.JavaConverters._

  lazy private val defaultXPathFactory = ConfigFactory.load("xml-converter-defaults").getString("xpath-factory")

  override def functions: Seq[TransformerFunction] = Seq(xpath, xmlToString)

  private val xmlToString: TransformerFunction =
    new NamedTransformerFunction(Seq("xmlToString", "xml2string"), pure = true) {
      private val transformers = new SoftThreadLocal[Transformer]

      override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
        val element = args.head.asInstanceOf[Element]
        val transformer = transformers.getOrElseUpdate {
          val t = TransformerFactory.newInstance().newTransformer()
          t.setOutputProperty(OutputKeys.ENCODING, "utf-8")
          t.setOutputProperty(OutputKeys.INDENT, "no")
          t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes")
          t
        }
        val result = new StreamResult(new StringWriter())
        val source = new DOMSource(element)
        transformer.transform(source, result)
        result.getWriter.toString
      }
    }


  private val xpath: TransformerFunction = new NamedTransformerFunction(Array("xpath"), pure = true) {
    private val cache = new ConcurrentHashMap[Any, XPathExpression]() // TODO is xpath thread safe?
    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      val factory = if (args.lengthCompare(3) < 0) { defaultXPathFactory } else {
        Option(args(2).asInstanceOf[String]).getOrElse(defaultXPathFactory)
      }
      val namespaces: Map[String, String] = if (args.lengthCompare(4) < 0) { Map.empty } else {
        Option(args(3).asInstanceOf[java.util.Map[String, String]]).map(_.asScala.toMap).getOrElse(Map.empty)
      }

      var path = cache.get((args(0), factory, namespaces))
      if (path == null) {
        path = XmlConverter.createXPath(factory, namespaces).compile(args(0).asInstanceOf[String])
        cache.put((args(0), factory, namespaces), path)
      }
      path.evaluate(args(1))
    }
  }
}
