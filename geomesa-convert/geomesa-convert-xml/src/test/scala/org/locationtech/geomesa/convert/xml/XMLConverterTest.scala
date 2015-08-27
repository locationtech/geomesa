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

  val myXML =
    """<doc>
      |  <Track>
      |    <trackNumber>123</trackNumber>
      |    <color>red</color>
      |    <fId squawk="abc123"></fId>
      |  </Track>
      |  <DataSource>
      |    <name>WhoKnows</name>
      |  </DataSource>
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
      |   type      = "xml"
      |   id-field  = "uuid()"
      |   fields = [
      |     { name = "trackNumber", path = ["Track", "trackNumber"],    transform = "$0::integer" }
      |     { name = "color",       path = ["Track", "color"],          transform = "trim($0)" }
      |     { name = "squawk",      path = ["Track", "fId", "@squawk"], transform = "trim($0)" }
      |     { name = "source",      path = ["DataSource", "name"] }
      |   ]
      | }
    """.stripMargin)

  "XML Converter should" >> {

    "parse XML" >> {

      val sft = SimpleFeatureTypes.createType(sftConf)
      val converter = SimpleFeatureConverters.build[String](sft, parserConf)
      implicit val ec = new EvaluationContext(null, null)
      val sf = converter.processSingleInput(myXML).get
      sf.getAttribute("trackNumber").asInstanceOf[Integer] mustEqual 123
      sf.getAttribute("color").asInstanceOf[String] mustEqual "red"
      sf.getAttribute("squawk").asInstanceOf[String] mustEqual "abc123"
      sf.getAttribute("source").asInstanceOf[String] mustEqual "WhoKnows"

    }
  }
}


