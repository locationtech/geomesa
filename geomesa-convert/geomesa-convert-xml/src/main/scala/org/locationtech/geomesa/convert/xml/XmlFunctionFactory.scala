/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

import java.io.StringWriter

import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.transform.{OutputKeys, Transformer, TransformerFactory}
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.utils.cache.SoftThreadLocal
import org.w3c.dom.Element

class XmlFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(xmlToString)

  private val xmlToString = new NamedTransformerFunction(Seq("xmlToString", "xml2string")) {
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
}
