/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import org.locationtech.jts.geom.Point
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class XmlConverterTest extends Specification {

  sequential

  val sftConf = ConfigFactory.parseString(
    """{ type-name = "xmlFeatureType"
      |  attributes = [
      |    {name = "number", type = "Integer"}
      |    {name = "color",  type = "String"}
      |    {name = "weight", type = "Double"}
      |    {name = "source", type = "String"}
      |  ]
      |}
    """.stripMargin)

  val sft = SimpleFeatureTypes.createType(sftConf)

  "XML Converter" should {

    "parse multiple features out of a single document" >> {
      val xml =
        """<doc>
          |  <DataSource>
          |    <name>myxml</name>
          |  </DataSource>
          |  <Feature>
          |    <number>123</number>
          |    <color>red</color>
          |    <physical weight="127.5" height="5'11"/>
          |  </Feature>
          |  <Feature>
          |    <number>456</number>
          |    <color>blue</color>
              <physical weight="150" height="h2"/>
          |  </Feature>
          |</doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "Feature" // can be any xpath - relative to the root, or absolute
          |   options { line-mode  = "multi" }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "number",           transform = "$0::integer" }
          |     { name = "color",  path = "color",            transform = "trim($0)" }
          |     { name = "weight", path = "physical/@weight", transform = "$0::double" }
          |     { name = "source", path = "/doc/DataSource/name/text()" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(2)
        features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
        features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
        features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
        features(1).getAttribute("weight").asInstanceOf[Double] mustEqual 150
        features(1).getAttribute("source").asInstanceOf[String] mustEqual "myxml"
      }
    }

    "parse multiple features out of a single document with the geometry in the repeated XML tag" >> {

      val sftConf2 = ConfigFactory.parseString(
        """{ type-name = "xmlFeatureType"
          |  attributes = [
          |    {name = "number", type = "Integer"}
          |    {name = "color",  type = "String"}
          |    {name = "weight", type = "Double"}
          |    {name = "source", type = "String"}
          |    {name = "geom",   type = "Point"}
          |  ]
          |}
        """.stripMargin)
      val sft2 = SimpleFeatureTypes.createType(sftConf2)

      val xml =
        """<doc>
          |  <DataSource>
          |    <name>myxml</name>
          |  </DataSource>
          |  <Feature lon="1.23" lat="4.23">
          |    <number>123</number>
          |    <color>red</color>
          |    <physical weight="127.5" height="5'11"/>
          |  </Feature>
          |  <Feature lon="4.56" lat="7.56">
          |    <number>456</number>
          |    <color>blue</color>
              <physical weight="150" height="h2"/>
          |  </Feature>
          |</doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "Feature" // can be any xpath - relative to the root, or absolute
          |   options { line-mode  = "multi" }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "number",           transform = "$0::integer" }
          |     { name = "color",  path = "color",            transform = "trim($0)" }
          |     { name = "weight", path = "physical/@weight", transform = "$0::double" }
          |     { name = "source", path = "/doc/DataSource/name/text()" }
          |     { name = "lon",    path = "./@lon", transform = "$0::double" }
          |     { name = "lat",    path = "./@lat", transform = "$0::double" }
          |     { name = "geom",   transform = "point($lon, $lat)" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft2, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(2)
        features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
        features.head.getAttribute("geom").asInstanceOf[Point] mustEqual WKTUtils.read("POINT(1.23 4.23)").asInstanceOf[Point]

        features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
        features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
        features(1).getAttribute("weight").asInstanceOf[Double] mustEqual 150
        features(1).getAttribute("source").asInstanceOf[String] mustEqual "myxml"
        features(1).getAttribute("geom").asInstanceOf[Point] mustEqual WKTUtils.read("POINT(4.56 7.56)").asInstanceOf[Point]
      }
    }

    "parse nested feature nodes" >> {
      val xml =
        """<doc>
          |  <DataSource>
          |    <name>myxml</name>
          |  </DataSource>
          |  <IgnoreMe>
          |    <Feature>
          |      <number>123</number>
          |      <color>red</color>
          |      <physical weight="127.5" height="5'11"/>
          |    </Feature>
          |  </IgnoreMe>
          |  <IgnoreMe>
          |    <Feature>
          |      <number>456</number>
          |      <color>blue</color>
          |      <physical weight="150" height="h2"/>
          |    </Feature>
          |  </IgnoreMe>
          |</doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "/doc/IgnoreMe/Feature" // can be any xpath - relative to the root, or absolute
          |   options { line-mode  = "multi" }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "number",           transform = "$0::integer" }
          |     { name = "color",  path = "color",            transform = "trim($0)" }
          |     { name = "weight", path = "physical/@weight", transform = "$0::double" }
          |     { name = "source", path = "/doc/DataSource/name/text()" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(2)
        features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
        features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
        features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
        features(1).getAttribute("weight").asInstanceOf[Double] mustEqual 150
        features(1).getAttribute("source").asInstanceOf[String] mustEqual "myxml"
      }
    }

    "apply xpath functions" >> {
      val xml =
        """<doc>
          |  <DataSource>
          |    <name>myxml</name>
          |  </DataSource>
          |  <Feature>
          |    <number>123</number>
          |    <color>red</color>
          |    <physical weight="127.5" height="5'11"/>
          |  </Feature>
          |</doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "Feature" // can be any xpath - relative to the root, or absolute
          |   options { line-mode  = "multi" }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "number",                  transform = "$0::integer" }
          |     { name = "color",  path = "color",                   transform = "trim($0)" }
          |     { name = "weight", path = "floor(physical/@weight)", transform = "$0::double" }
          |     { name = "source", path = "/doc/DataSource/name/text()" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(1)
        features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127
        features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
      }
    }

    "use an ID hash for each node" >> {
      val xml =
        """<doc>
          |  <DataSource>
          |    <name>myxml</name>
          |  </DataSource>
          |  <Feature>
          |    <number>123</number>
          |    <color>red</color>
          |    <physical weight="127.5" height="5'11"/>
          |  </Feature>
          |  <Feature>
          |    <number>456</number>
          |    <color>blue</color>
                <physical weight="150" height="h2"/>
          |  </Feature>
          |</doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "md5(string2bytes(xml2string($0)))"
          |   feature-path = "Feature" // can be any xpath - relative to the root, or absolute
          |   options { line-mode  = "multi" }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "number",           transform = "$0::integer" }
          |     { name = "color",  path = "color",            transform = "trim($0)" }
          |     { name = "weight", path = "physical/@weight", transform = "$0::double" }
          |     { name = "source", path = "/doc/DataSource/name/text()" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(2)
        features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
        features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
        features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
        features(1).getAttribute("weight").asInstanceOf[Double] mustEqual 150
        features(1).getAttribute("source").asInstanceOf[String] mustEqual "myxml"
        features.head.getID mustEqual "441dd9114a1a345fe59f0dfe461f01ca"
        features(1).getID mustEqual "42aae6286c7204c3aa1aa99a4e8dae35"
      }
    }

    "validate with an xsd" >> {
      val xml =
        """<?xml version="1.0" encoding="UTF-8" ?>
          |<f:doc xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:f="http://geomesa.org/test-feature">
          |  <f:DataSource>
          |    <f:name>myxml</f:name>
          |  </f:DataSource>
          |  <f:Feature>
          |    <f:number>123</f:number>
          |    <f:color>red</f:color>
          |    <f:physical weight="127.5" height="5'11"/>
          |  </f:Feature>
          |</f:doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "ns:Feature" // can be any xpath - relative to the root, or absolute
          |   xsd          = "xml-feature.xsd" // looked up by class.getResource
          |   options { line-mode  = "multi" }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "ns:number",           transform = "$0::integer" }
          |     { name = "color",  path = "ns:color",            transform = "trim($0)" }
          |     { name = "weight", path = "ns:physical/@weight", transform = "$0::double" }
          |     { name = "source", path = "/ns:doc/ns:DataSource/ns:name/text()" }
          |   ]
          |   xml-namespaces = {
          |     ns = "http://geomesa.org/test-feature"
          |   }
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        "parse as itr" >> {
          val features = WithClose(converter.process(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))(_.toList)
          features must haveLength(1)
          features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
          features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
          features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
          features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
        }

        "parse as stream" >> {
          val features = WithClose(converter.process(new ByteArrayInputStream(xml.replaceAllLiterally("\n", " ").getBytes(StandardCharsets.UTF_8))))(_.toList)
          features must haveLength(1)
          features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
          features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
          features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
          features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
        }
      }
    }

    "parse xml im multi line mode" >> {
      val xml =
        """<?xml version="1.0" encoding="UTF-8" ?>
          |<f:doc xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:f="http://geomesa.org/test-feature">
          |  <f:DataSource>
          |    <f:name>myxml</f:name>
          |  </f:DataSource>
          |  <f:Feature>
          |    <f:number>123</f:number>
          |    <f:color>red</f:color>
          |    <f:physical weight="127.5" height="5'11"/>
          |  </f:Feature>
          |</f:doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "ns:Feature" // can be any xpath - relative to the root, or absolute
          |   xsd          = "xml-feature.xsd" // looked up by class.getResource
          |   options {
          |     line-mode  = "multi"
          |   }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "ns:number",           transform = "$0::integer" }
          |     { name = "color",  path = "ns:color",            transform = "trim($0)" }
          |     { name = "weight", path = "ns:physical/@weight", transform = "$0::double" }
          |     { name = "source", path = "/ns:doc/ns:DataSource/ns:name/text()" }
          |   ]
          |   xml-namespaces = {
          |     ns = "http://geomesa.org/test-feature"
          |   }
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(xml.getBytes)))(_.toList)
        features must haveLength(1)
        features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
      }
    }

    "parse xml in single line mode" >> {
      val origXml =
        """<?xml version="1.0" encoding="UTF-8" ?>
          |<f:doc xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:f="http://geomesa.org/test-feature">
          |  <f:DataSource>
          |    <f:name>myxml</f:name>
          |  </f:DataSource>
          |  <f:Feature>
          |    <f:number>123</f:number>
          |    <f:color>red</f:color>
          |    <f:physical weight="127.5" height="5'11"/>
          |  </f:Feature>
          |</f:doc>
        """.stripMargin

      val xml = origXml.replaceAllLiterally("\n", " ") + "\n" + origXml.replaceAllLiterally("\n", " ")

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "ns:Feature" // can be any xpath - relative to the root, or absolute
          |   xsd          = "xml-feature.xsd" // looked up by class.getResource
          |   options {
          |     line-mode  = "single"
          |   }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "ns:number",           transform = "$0::integer" }
          |     { name = "color",  path = "ns:color",            transform = "trim($0)" }
          |     { name = "weight", path = "ns:physical/@weight", transform = "$0::double" }
          |     { name = "source", path = "/ns:doc/ns:DataSource/ns:name/text()" }
          |   ]
          |   xml-namespaces = {
          |     ns = "http://geomesa.org/test-feature"
          |   }
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(xml.getBytes)))(_.toList)
        features must haveLength(2)

        features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"

        features.last.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.last.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.last.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features.last.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
      }
    }

    "invalidate with an xsd" >> {
      val xml =
        """<f:doc2 xmlns:f="http://geomesa.org/test-feature" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
          |  <f:DataSource>
          |    <f:name>myxml</f:name>
          |  </f:DataSource>
          |  <f:Feature>
          |    <f:number>123</f:number>
          |    <f:color>red</f:color>
          |    <f:physical weight="127.5" height="5'11"/>
          |  </f:Feature>
          |</f:doc2>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "ns:Feature" // can be any xpath - relative to the root, or absolute
          |   xsd          = "xml-feature.xsd" // looked up by class.getResource
          |   options { line-mode  = "multi" }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "ns:number",           transform = "$0::integer" }
          |     { name = "color",  path = "ns:color",            transform = "trim($0)" }
          |     { name = "weight", path = "ns:physical/@weight", transform = "$0::double" }
          |     { name = "source", path = "/ns:doc/ns:DataSource/ns:name/text()" }
          |   ]
          |   xml-namespaces = {
          |     ns = "http://geomesa.org/test-feature"
          |   }
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(0)
      }
    }

    "handle user data" >> {
      val xml =
        """<doc>
          |  <DataSource>
          |    <name>myxml</name>
          |  </DataSource>
          |  <Feature>
          |    <number>123</number>
          |    <color>red</color>
          |    <physical weight="127.5" height="5'11"/>
          |  </Feature>
          |</doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "Feature" // can be any xpath - relative to the root, or absolute
          |   options { line-mode  = "multi" }
          |   user-data    = {
          |     my.user.key  = "$weight"
          |   }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "number",                  transform = "$0::integer" }
          |     { name = "color",  path = "color",                   transform = "trim($0)" }
          |     { name = "weight", path = "floor(physical/@weight)", transform = "$0::double" }
          |     { name = "source", path = "/doc/DataSource/name/text()" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(1)
        features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127
        features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
        features.head.getUserData.get("my.user.key") mustEqual 127d
      }
    }

    "Parse XMLs with a BOM" >> {

      val xml = getClass.getClassLoader.getResource("bomTest.xml")
      xml must not(beNull)

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "Feature" // can be any xpath - relative to the root, or absolute
          |   options { line-mode  = "multi" }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "number",           transform = "$0::integer" }
          |     { name = "color",  path = "color",            transform = "trim($0)" }
          |     { name = "weight", path = "physical/@weight", transform = "$0::double" }
          |     { name = "source", path = "/doc/DataSource/name/text()" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(xml.openStream()))(_.toList)
        features must haveLength(2)
        features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
        features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
        features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
        features(1).getAttribute("weight").asInstanceOf[Double] mustEqual 150
        features(1).getAttribute("source").asInstanceOf[String] mustEqual "myxml"
      }
    }

    "support namespaces with saxon" >> {
      val xml =
        """<ns:doc xmlns:ns="http://geomesa.example.com/foo" xmlns:ns2="http://geomesa.example.com/foo2">
          |  <ns:DataSource>
          |    <ns:name>myxml</ns:name>
          |  </ns:DataSource>
          |  <ns:Feature>
          |    <ns:number>123</ns:number>
          |    <ns:color>red</ns:color>
          |    <ns2:physical weight="127.5" height="5'11"/>
          |  </ns:Feature>
          |</ns:doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "ns:Feature" // can be any xpath - relative to the root, or absolute
          |   options { line-mode  = "multi" }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "ns:number",                  transform = "$0::integer" }
          |     { name = "color",  path = "ns:color",                   transform = "trim($0)" }
          |     { name = "weight", path = "floor(ns2:physical/@weight)", transform = "$0::double" }
          |     { name = "source", path = "/ns:doc/ns:DataSource/ns:name/text()" }
          |   ]
          |   xml-namespaces = {
          |     ns  = "http://geomesa.example.com/foo"
          |     ns2 = "http://geomesa.example.com/foo2"
          |   }
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, parserConf)) { converter =>
        val features = WithClose(converter.process(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))(_.toList)
        features must haveLength(1)
        features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127
        features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
      }
    }

    "not use single line mode for v1 processInput/processSingleInput" >> {
      val xml =
        """<doc>
          |  <DataSource>
          |    <name>myxml</name>
          |  </DataSource>
          |  <Feature>
          |    <number>123</number>
          |    <color>red</color>
          |    <physical weight="127.5" height="5'11"/>
          |  </Feature>
          |</doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "Feature" // can be any xpath - relative to the root, or absolute
          |   options {
          |     line-mode  = "single"
          |   }
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "number",           transform = "$0::integer" }
          |     { name = "color",  path = "color",            transform = "trim($0)"    }
          |     { name = "weight", path = "physical/@weight", transform = "$0::double"  }
          |     { name = "source", path = "/doc/DataSource/name/text()"                 }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverters.build[String](sft, parserConf)) { converter =>
        val features = converter.processInput(Iterator(xml, xml)).toList
        features must haveLength(2)

        features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.head.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"

        features.last.getAttribute("number").asInstanceOf[Integer] mustEqual 123
        features.last.getAttribute("color").asInstanceOf[String] mustEqual "red"
        features.last.getAttribute("weight").asInstanceOf[Double] mustEqual 127.5
        features.last.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
      }
    }
  }
}


