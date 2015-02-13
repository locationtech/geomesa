/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
          |  def geom(): com.vividsolutions.jts.geom.Point = sf.getAttribute(2).asInstanceOf[com.vividsolutions.jts.geom.Point]
          |  def geomOpt(): Option[com.vividsolutions.jts.geom.Point] = Option(geom())
          |  def setGeom(x: com.vividsolutions.jts.geom.Point): Unit = sf.setAttribute(2, x)
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
          |  def geom(): com.vividsolutions.jts.geom.Point = sf.getAttribute(2).asInstanceOf[com.vividsolutions.jts.geom.Point]
          |  def geomOpt(): Option[com.vividsolutions.jts.geom.Point] = Option(geom())
          |  def setGeom(x: com.vividsolutions.jts.geom.Point): Unit = sf.setAttribute(2, x)
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
          |    def geom(): com.vividsolutions.jts.geom.Point = sf.getAttribute(2).asInstanceOf[com.vividsolutions.jts.geom.Point]
          |    def geomOpt(): Option[com.vividsolutions.jts.geom.Point] = Option(geom())
          |    def setGeom(x: com.vividsolutions.jts.geom.Point): Unit = sf.setAttribute(2, x)
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