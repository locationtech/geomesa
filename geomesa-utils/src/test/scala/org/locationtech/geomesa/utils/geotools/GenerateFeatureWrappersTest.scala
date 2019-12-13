/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GenerateFeatureWrappersTest extends Specification {

  "GenerateFeatureWrappers" should {

    "generate implicit wrappers for simple feature types" in {
      val sft = SimpleFeatureTypes.createType("test1", "attr:String,dtg:Date,*geom:Point")
      val template = GenerateFeatureWrappers.buildClass(sft, "")
      val expected =
        """implicit class test1(val sf: org.opengis.feature.simple.SimpleFeature) extends AnyVal {
          |
          |  def attr(): java.lang.String = sf.getAttribute(0).asInstanceOf[java.lang.String]
          |  def attrOpt(): Option[java.lang.String] = Option(attr())
          |  def setAttr(x: java.lang.String): Unit = sf.setAttribute(0, x)
          |
          |  def dtg(): java.util.Date = sf.getAttribute(1).asInstanceOf[java.util.Date]
          |  def dtgOpt(): Option[java.util.Date] = Option(dtg())
          |  def setDtg(x: java.util.Date): Unit = sf.setAttribute(1, x)
          |
          |  def geom(): org.locationtech.jts.geom.Point = sf.getAttribute(2).asInstanceOf[org.locationtech.jts.geom.Point]
          |  def geomOpt(): Option[org.locationtech.jts.geom.Point] = Option(geom())
          |  def setGeom(x: org.locationtech.jts.geom.Point): Unit = sf.setAttribute(2, x)
          |
          |  def debug(): String = {
          |    import scala.collection.JavaConversions._
          |    val sb = new StringBuilder(s"${sf.getType.getTypeName}:${sf.getID}")
          |    sf.getProperties.foreach(p => sb.append(s"|${p.getName.getLocalPart}=${p.getValue}"))
          |    sb.toString()
          |  }
          |}""".stripMargin
      template mustEqual(expected)
    }

    "support list and map types" in {
      val sft = SimpleFeatureTypes.createType("test1", "attr:List[String],dtg:Map[String,Date],*geom:Point")
      val template = GenerateFeatureWrappers.buildClass(sft, "")
      val expected =
        """implicit class test1(val sf: org.opengis.feature.simple.SimpleFeature) extends AnyVal {
          |
          |  def attr(): java.util.List[java.lang.String] = sf.getAttribute(0).asInstanceOf[java.util.List[java.lang.String]]
          |  def attrOpt(): Option[java.util.List[java.lang.String]] = Option(attr())
          |  def setAttr(x: java.util.List[java.lang.String]): Unit = sf.setAttribute(0, x)
          |
          |  def dtg(): java.util.Map[java.lang.String,java.util.Date] = sf.getAttribute(1).asInstanceOf[java.util.Map[java.lang.String,java.util.Date]]
          |  def dtgOpt(): Option[java.util.Map[java.lang.String,java.util.Date]] = Option(dtg())
          |  def setDtg(x: java.util.Map[java.lang.String,java.util.Date]): Unit = sf.setAttribute(1, x)
          |
          |  def geom(): org.locationtech.jts.geom.Point = sf.getAttribute(2).asInstanceOf[org.locationtech.jts.geom.Point]
          |  def geomOpt(): Option[org.locationtech.jts.geom.Point] = Option(geom())
          |  def setGeom(x: org.locationtech.jts.geom.Point): Unit = sf.setAttribute(2, x)
          |
          |  def debug(): String = {
          |    import scala.collection.JavaConversions._
          |    val sb = new StringBuilder(s"${sf.getType.getTypeName}:${sf.getID}")
          |    sf.getProperties.foreach(p => sb.append(s"|${p.getName.getLocalPart}=${p.getValue}"))
          |    sb.toString()
          |  }
          |}""".stripMargin
      template mustEqual(expected)
    }

    "work with invalid attribute names" in {
      val sft = SimpleFeatureTypes.createType("test1", "a#^2:String,@dt!g:Date,*geom:Point")
      val template = GenerateFeatureWrappers.buildClass(sft, "")
      val expected =
        """implicit class test1(val sf: org.opengis.feature.simple.SimpleFeature) extends AnyVal {
          |
          |  def a__2(): java.lang.String = sf.getAttribute(0).asInstanceOf[java.lang.String]
          |  def a__2Opt(): Option[java.lang.String] = Option(a__2())
          |  def setA__2(x: java.lang.String): Unit = sf.setAttribute(0, x)
          |
          |  def _dt_g(): java.util.Date = sf.getAttribute(1).asInstanceOf[java.util.Date]
          |  def _dt_gOpt(): Option[java.util.Date] = Option(_dt_g())
          |  def set_dt_g(x: java.util.Date): Unit = sf.setAttribute(1, x)
          |
          |  def geom(): org.locationtech.jts.geom.Point = sf.getAttribute(2).asInstanceOf[org.locationtech.jts.geom.Point]
          |  def geomOpt(): Option[org.locationtech.jts.geom.Point] = Option(geom())
          |  def setGeom(x: org.locationtech.jts.geom.Point): Unit = sf.setAttribute(2, x)
          |
          |  def debug(): String = {
          |    import scala.collection.JavaConversions._
          |    val sb = new StringBuilder(s"${sf.getType.getTypeName}:${sf.getID}")
          |    sf.getProperties.foreach(p => sb.append(s"|${p.getName.getLocalPart}=${p.getValue}"))
          |    sb.toString()
          |  }
          |}""".stripMargin
      template mustEqual(expected)
    }

    "generate a wrapper object" in {
      val sft = SimpleFeatureTypes.createType("test1", "attr:String,dtg:Date,*geom:Point")
      val template = GenerateFeatureWrappers.buildAllClasses(Seq(sft), "org.foo")
      val expected =
        """package org.foo
          |
          |object SimpleFeatureWrappers {
          |
          |  implicit class test1(val sf: org.opengis.feature.simple.SimpleFeature) extends AnyVal {
          |
          |    def attr(): java.lang.String = sf.getAttribute(0).asInstanceOf[java.lang.String]
          |    def attrOpt(): Option[java.lang.String] = Option(attr())
          |    def setAttr(x: java.lang.String): Unit = sf.setAttribute(0, x)
          |
          |    def dtg(): java.util.Date = sf.getAttribute(1).asInstanceOf[java.util.Date]
          |    def dtgOpt(): Option[java.util.Date] = Option(dtg())
          |    def setDtg(x: java.util.Date): Unit = sf.setAttribute(1, x)
          |
          |    def geom(): org.locationtech.jts.geom.Point = sf.getAttribute(2).asInstanceOf[org.locationtech.jts.geom.Point]
          |    def geomOpt(): Option[org.locationtech.jts.geom.Point] = Option(geom())
          |    def setGeom(x: org.locationtech.jts.geom.Point): Unit = sf.setAttribute(2, x)
          |
          |    def debug(): String = {
          |      import scala.collection.JavaConversions._
          |      val sb = new StringBuilder(s"${sf.getType.getTypeName}:${sf.getID}")
          |      sf.getProperties.foreach(p => sb.append(s"|${p.getName.getLocalPart}=${p.getValue}"))
          |      sb.toString()
          |    }
          |  }
          |}""".stripMargin
      template mustEqual(expected)
    }
  }
}