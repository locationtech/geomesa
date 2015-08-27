package org.locationtech.geomesa.convert.xml

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class XMLConverterTest extends Specification {

  val sftConf = ConfigFactory.parseString(
    """{ type-name = "xmlFeatureType"
      |  attributes = [
      |    {name = "number", type = "Integer"}
      |    {name = "color",  type = "String"}
      |    {name = "weight", type = "String"}
      |    {name = "source", type = "String"}
      |  ]
      |}
    """.stripMargin)

  val sft = SimpleFeatureTypes.createType(sftConf)
  implicit val ec = new EvaluationContext(null, null)

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
              <physical weight="w2" height="h2"/>
          |  </Feature>
          |</doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | converter = {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "Feature" // can be any xpath - relative to the root, or absolute
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "number",           transform = "$0::integer" }
          |     { name = "color",  path = "color",            transform = "trim($0)" }
          |     { name = "weight", path = "physical/@weight", transform = "trim($0)" }
          |     { name = "source", path = "/doc/DataSource/name/text()" }
          |   ]
          | }
        """.stripMargin)

      val converter = SimpleFeatureConverters.build[String](sft, parserConf)
      val features = converter.processInput(Iterator(xml)).toList
      features must haveLength(2)
      features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
      features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
      features.head.getAttribute("weight").asInstanceOf[String] mustEqual "127.5"
      features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
      features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
      features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
      features(1).getAttribute("weight").asInstanceOf[String] mustEqual "w2"
      features(1).getAttribute("source").asInstanceOf[String] mustEqual "myxml"
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
          |      <physical weight="w2" height="h2"/>
          |    </Feature>
          |  </IgnoreMe>
          |</doc>
        """.stripMargin

      val parserConf = ConfigFactory.parseString(
        """
          | converter = {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "/doc/IgnoreMe/Feature" // can be any xpath - relative to the root, or absolute
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "number",           transform = "$0::integer" }
          |     { name = "color",  path = "color",            transform = "trim($0)" }
          |     { name = "weight", path = "physical/@weight", transform = "trim($0)" }
          |     { name = "source", path = "/doc/DataSource/name/text()" }
          |   ]
          | }
        """.stripMargin)

      val converter = SimpleFeatureConverters.build[String](sft, parserConf)
      val features = converter.processInput(Iterator(xml)).toList
      features must haveLength(2)
      features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
      features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
      features.head.getAttribute("weight").asInstanceOf[String] mustEqual "127.5"
      features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
      features(1).getAttribute("number").asInstanceOf[Integer] mustEqual 456
      features(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
      features(1).getAttribute("weight").asInstanceOf[String] mustEqual "w2"
      features(1).getAttribute("source").asInstanceOf[String] mustEqual "myxml"
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
          | converter = {
          |   type         = "xml"
          |   id-field     = "uuid()"
          |   feature-path = "Feature" // can be any xpath - relative to the root, or absolute
          |   fields = [
          |     // paths can be any xpath - relative to the feature-path, or absolute
          |     { name = "number", path = "number",                   transform = "$0::integer" }
          |     { name = "color",  path = "color",                    transform = "trim($0)" }
          |     { name = "weight", path = "floor(physical/@weight)", transform = "trim($0)" }
          |     { name = "source", path = "/doc/DataSource/name/text()" }
          |   ]
          | }
        """.stripMargin)

      val converter = SimpleFeatureConverters.build[String](sft, parserConf)
      val features = converter.processInput(Iterator(xml)).toList
      features must haveLength(1)
      features.head.getAttribute("number").asInstanceOf[Integer] mustEqual 123
      features.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
      features.head.getAttribute("weight").asInstanceOf[String] mustEqual "127"
      features.head.getAttribute("source").asInstanceOf[String] mustEqual "myxml"
    }
  }
}


