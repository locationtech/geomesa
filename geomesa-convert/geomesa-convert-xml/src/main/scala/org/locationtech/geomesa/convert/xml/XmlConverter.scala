/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BOMInputStream
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.Modes.{ErrorMode, LineMode, ParseMode}
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert.xml.XmlConverter.{XmlConfig, XmlField, XmlHelper, XmlOptions}
import org.locationtech.geomesa.convert2._
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.TextTools
import org.w3c.dom.{Element, NodeList}
import org.xml.sax.{ErrorHandler, InputSource, SAXParseException}

import java.io._
import java.nio.charset.Charset
import javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI
import javax.xml.namespace.NamespaceContext
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory
import javax.xml.xpath.{XPath, XPathConstants, XPathExpression, XPathFactory}
import scala.util.control.NonFatal

class XmlConverter(sft: SimpleFeatureType, config: XmlConfig, fields: Seq[XmlField], options: XmlOptions)
    extends AbstractConverter[Element, XmlConfig, XmlField, XmlOptions](sft, config, fields, options) {

  private val helper = new ThreadLocal[XmlHelper]() {
    override def initialValue: XmlHelper =
      new XmlHelper(config.xpathFactory, config.xmlNamespaces, config.xsd, config.featurePath)
  }

  fields.foreach(_.compile(helper))

  // TODO GEOMESA-1039 more efficient InputStream processing for multi mode

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[Element] =
    XmlConverter.iterator(helper.get.parser, is, options.encoding, options.lineMode, ec)

  override protected def values(
      parsed: CloseableIterator[Element],
      ec: EvaluationContext): CloseableIterator[Array[Any]] = {

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a68b9539c7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a290bbb67c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
    val array = Array.ofDim[Any](2)
=======
    val array = Array.ofDim[Any](1)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ebea6992b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f12c5b15a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 302df4295a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1db60d5575 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> edc23fb02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 89bb58cf10 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 302df4295a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6d51efa18f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ebea6992b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5f12c5b15a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d51efa18f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ebea6992b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5f12c5b15a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a68b9539c7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a68b9539c7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a290bbb67c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
    val array = Array.ofDim[Any](2)
=======
    val array = Array.ofDim[Any](1)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a68b9539c7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a290bbb67c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
    val array = Array.ofDim[Any](2)
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
    val array = Array.ofDim[Any](2)
=======
    val array = Array.ofDim[Any](1)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
    val array = Array.ofDim[Any](2)
=======
    val array = Array.ofDim[Any](1)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
    val array = Array.ofDim[Any](2)
=======
    val array = Array.ofDim[Any](1)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 50e677e046 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 302df4295a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ebea6992b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1db60d5575 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
=======
=======
>>>>>>> a68b9539c7 (GEOMESA-3071 Move all converter state into evaluation context)
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
    val array = Array.ofDim[Any](2)
=======
    val array = Array.ofDim[Any](1)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 993ffbfa6e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 915bd11f55 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> edc23fb02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 89bb58cf10 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a68b9539c7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a290bbb67c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 2537391cc6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 50e677e046 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2537391cc6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d51efa18f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ebea6992b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5f12c5b15a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
    val array = Array.ofDim[Any](2)
=======
    val array = Array.ofDim[Any](1)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
    val array = Array.ofDim[Any](2)
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')

    helper.get.rootPath match {
      case None =>
        parsed.map { element =>
          array(0) = element
          array
        }

      case Some(path) =>
        parsed.flatMap { element =>
          array(1) = element
          val nodeList = path.evaluate(element, XPathConstants.NODESET).asInstanceOf[NodeList]
          Iterator.tabulate(nodeList.getLength) { i =>
            array(0) = nodeList.item(i)
            array
          }
        }
    }
  }

}

object XmlConverter extends StrictLogging {

  import scala.collection.JavaConverters._

  def createXPath(factory: String, namespaces: Map[String, String] = Map.empty): XPath = {
    val fact = try {
      val res = XPathFactory.newInstance(XPathFactory.DEFAULT_OBJECT_MODEL_URI, factory, getClass.getClassLoader)
      logger.info(s"Loaded xpath factory ${res.getClass}")
      res
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Unable to load xpath provider '$factory': ${e.toString}. " +
            "Xpath queries may be slower - check your classpath")
        XPathFactory.newInstance(XPathFactory.DEFAULT_OBJECT_MODEL_URI)
    }
    val xpath = fact.newXPath()
    if (namespaces.nonEmpty) {
      xpath.setNamespaceContext(new NamespaceContext() {
        override def getPrefix(namespaceURI: String): String = null
        override def getPrefixes(namespaceURI: String): java.util.Iterator[String] = null
        override def getNamespaceURI(prefix: String): String = namespaces.getOrElse(prefix, null)
      })
    }
    xpath
  }

  def iterator(
      parser: DocParser,
      is: InputStream,
      encoding: Charset,
      mode: LineMode,
      ec: EvaluationContext): CloseableIterator[Element] = {

    // detect and exclude the BOM if it exists
    val bis = new BOMInputStream(is)
    if (mode == LineMode.Single) {
      val lines = IOUtils.lineIterator(bis, encoding)
      val elements = lines.asScala.flatMap { line =>
        ec.line += 1
        if (TextTools.isWhitespace(line)) { Iterator.empty } else {
          Iterator.single(parser.parse(new StringReader(line)))
        }
      }
      CloseableIterator(elements, lines.close())
    } else {
      val reader = new InputStreamReader(bis, encoding)
      CloseableIterator.fill(1, reader.close()) { ec.line += 1; parser.parse(reader) }
    }
  }

  // paths can be absolute, or relative to the feature node
  // they can also include xpath functions to manipulate the result
  // feature path can be any xpath that resolves to a node set (or a single node)

  case class XmlConfig(
      `type`: String,
      xpathFactory: String,
      xmlNamespaces: Map[String, String],
      xsd: Option[String],
      featurePath: Option[String],
      idField: Option[Expression],
      caches: Map[String, Config],
      userData: Map[String, Expression]
    ) extends ConverterConfig

  sealed trait XmlField extends Field {
    def compile(helper: ThreadLocal[XmlHelper]): Unit
  }

  case class DerivedField(name: String, transforms: Option[Expression]) extends XmlField {
    override def compile(helper: ThreadLocal[XmlHelper]): Unit = {}
    override val fieldArg: Option[Array[AnyRef] => AnyRef] = None
  }

  case class XmlPathField(name: String, path: String, transforms: Option[Expression]) extends XmlField {

    private var helper: ThreadLocal[XmlHelper] = _

<<<<<<< HEAD
    private val expression = new ThreadLocal[XPathExpression]() {
      override def initialValue(): XPathExpression = helper.get.xpath.compile(path)
    }

    override val fieldArg: Option[Array[AnyRef] => AnyRef] = Some(values)

    override def compile(helper: ThreadLocal[XmlHelper]): Unit = this.helper = helper

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
    private val mutableArray = Array.ofDim[Any](1)

=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
    private val mutableArray = Array.ofDim[Any](1)

>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
    private val mutableArray = Array.ofDim[Any](1)

>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
    private val mutableArray = Array.ofDim[Any](1)

>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
    private val mutableArray = Array.ofDim[Any](1)

>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
    private val mutableArray = Array.ofDim[Any](1)

<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
    private val mutableArray = Array.ofDim[Any](1)

>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
    private val expression = new ThreadLocal[XPathExpression]() {
      override def initialValue(): XPathExpression = helper.get.xpath.compile(path)
    }

    override val fieldArg: Option[Array[AnyRef] => AnyRef] = Some(values)

    override def compile(helper: ThreadLocal[XmlHelper]): Unit = this.helper = helper

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
    override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
      mutableArray(0) = expression.get.evaluate(args(0))
      super.eval(mutableArray)
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
    private def values(args: Array[AnyRef]): AnyRef = expression.get.evaluate(args(0))
  }

  case class XmlOptions(
      validators: Seq[String],
      reporters: Seq[Config],
      parseMode: ParseMode,
      errorMode: ErrorMode,
      lineMode: LineMode,
      encoding: Charset
    ) extends ConverterOptions

  /**
   * XML parser helper - holds non-thread-safe classes
   *
   * @param xpathFactory class name to use for creating xpaths
   * @param namespaces xml namespaces
   * @param xsd xsd path to an xsd used to validate parsed documents
   * @param featurePath path to features in the xml doc
   */
  class XmlHelper(
      xpathFactory: String,
      namespaces: Map[String, String],
      xsd: Option[String],
      featurePath: Option[String]) {
    val parser = new DocParser(xsd)
    val xpath: XPath = createXPath(xpathFactory, namespaces)
    val rootPath: Option[XPathExpression] = featurePath.map(xpath.compile)
  }

  /**
   * Document parser helper
   *
   * @param xsd xsd path to an xsd used to validate parsed documents
   */
  class DocParser(xsd: Option[String]) {

    private val builder = {
      val factory = DocumentBuilderFactory.newInstance()
      factory.setNamespaceAware(true)
      val builder = factory.newDocumentBuilder()
      // override default error handler which prints to stdout/stderr
      builder.setErrorHandler(new ErrorHandler() {
        override def warning(e: SAXParseException): Unit = logger.warn("XML parsing error:", e)
        override def error(e: SAXParseException): Unit = throw e
        override def fatalError(e: SAXParseException): Unit = throw e
      })
      builder
    }

    private val validator = xsd.map { path =>
      val schemaFactory = SchemaFactory.newInstance(W3C_XML_SCHEMA_NS_URI)
      WithClose(getClass.getClassLoader.getResourceAsStream(path)) { xsdStream =>
        schemaFactory.newSchema(new StreamSource(xsdStream)).newValidator()
      }
    }

    def parse(reader: Reader): Element = {
      // parse the document once, then extract each feature node and operate on it
      val document = builder.parse(new InputSource(reader))
      // if a schema is defined, validate it - this will throw an exception on failure
      validator.foreach(_.validate(new DOMSource(document)))
      document.getDocumentElement
    }
  }
}
