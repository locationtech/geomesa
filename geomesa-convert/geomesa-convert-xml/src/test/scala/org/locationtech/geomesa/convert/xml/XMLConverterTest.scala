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

  val xml =
    """<doc>
      |  <DataSource>
      |    <name>WhoKnows</name>
      |  </DataSource>
      |  <Track>
      |    <trackNumber>123</trackNumber>
      |    <color>red</color>
      |    <fId squawk="abc"></fId>
      |  </Track>
      |  <Track>
      |    <trackNumber>456</trackNumber>
      |    <color>blue</color>
      |    <fId squawk="xyz"></fId>
      |  </Track>
      |</doc>
    """.stripMargin

  val sftConf = ConfigFactory.parseString(
    """{ type-name = "track_sft"
      |  attributes = [
      |    {name = "trackNumber", type = "Integer"}
      |    {name = "color", type = "String"}
      |    {name = "squawk", type = "String"}
      |    {name = "source", type = "String"}
      |  ]
      |}
    """.stripMargin)

  val parserConf = ConfigFactory.parseString(
    """
      | converter = {
      |   type         = "xml"
      |   id-field     = "uuid()"
      |   feature-path = "Track"
      |   fields = [
      |     { name = "trackNumber", path = "trackNumber", transform = "$0::integer" }
      |     { name = "color",       path = "color",       transform = "trim($0)" }
      |     { name = "squawk",      path = "fId/@squawk", transform = "trim($0)" }
      |     { name = "source",      path = "/doc/DataSource/name/text()" }
      |   ]
      | }
    """.stripMargin)

  "XML Converter should" >> {

    "parse XML" >> {
      val sft = SimpleFeatureTypes.createType(sftConf)
      val converter = SimpleFeatureConverters.build[String](sft, parserConf)
      implicit val ec = new EvaluationContext(null, null)
      val sfs = converter.processInput(Iterator(xml)).toList
      sfs must haveLength(2)
      sfs.head.getAttribute("trackNumber").asInstanceOf[Integer] mustEqual 123
      sfs.head.getAttribute("color").asInstanceOf[String] mustEqual "red"
      sfs.head.getAttribute("squawk").asInstanceOf[String] mustEqual "abc"
      sfs.head.getAttribute("source").asInstanceOf[String] mustEqual "WhoKnows"
      sfs(1).getAttribute("trackNumber").asInstanceOf[Integer] mustEqual 456
      sfs(1).getAttribute("color").asInstanceOf[String] mustEqual "blue"
      sfs(1).getAttribute("squawk").asInstanceOf[String] mustEqual "xyz"
      sfs(1).getAttribute("source").asInstanceOf[String] mustEqual "WhoKnows"
    }
  }
}


