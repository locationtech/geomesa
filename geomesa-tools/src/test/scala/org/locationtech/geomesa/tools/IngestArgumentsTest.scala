package org.locationtech.geomesa.tools

import java.io.File

import com.beust.jcommander.ParameterException
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.Utils.Speculator
import org.locationtech.geomesa.utils.geotools.{Conversions, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats.Cardinality
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IngestArgumentsTest extends Specification {

  sequential

  val sftSpec = "name:String,age:Integer,*geom:Point:srid=4326"
  val featureName = "specTest"
  val sftConfig =
    """
      |{
      |  type-name = "specTest"
      |  attributes = [
      |    { name = "name", type = "String", index = false },
      |    { name = "age",  type = "Integer", index = false },
      |    { name = "geom", type = "Point",  index = true, srid = 4326, default = true }
      |  ]
      |}
    """.stripMargin

  val data =
    """
      |Andrew,30,Point(45 56)
      |Jim,33,Point(46 46)
      |Anthony,35,Point(47 47)
    """.stripMargin

  val convertConfig =
    """
      | converter = {
      |   type         = "delimited-text",
      |   format       = "DEFAULT",
      |   id-field     = "md5(string2bytes($0))",
      |   fields = [
      |     { name = "name",  transform = "$1" },
      |     { name = "age",   transform = "$2" },
      |     { name = "geom",  transform = "point($3)" }
      |   ]
      | }
    """.stripMargin

  val combined =
    """
      | sft = {
      |   type-name = "specTest"
      |   attributes = [
      |     { name = "name", type = "String", index = false },
      |     { name = "age",  type = "Integer", index = false },
      |     { name = "geom", type = "Point",  index = true, srid = 4326, default = true }
      |   ]
      | },
      | converter = {
      |   type         = "delimited-text",
      |   format       = "DEFAULT",
      |   id-field     = "md5(string2bytes($0))",
      |   fields = [
      |     { name = "name",  transform = "$1" },
      |     { name = "age",   transform = "$2" },
      |     { name = "geom",  transform = "point($3)" }
      |   ]
      | }
      """.stripMargin

  "Speculator" should {
    "work with sft and converter configs" >> {
      val sft = Speculator.getSft(sftSpec, featureName, convertConfig)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }

    "fail when given spec and no feature name" >> {
      Speculator.getSft(sftSpec, null, convertConfig) must throwA[ParameterException]
    }

    "create a spec from sft conf" >> {
      val sft = Speculator.getSft(sftConfig, null, convertConfig)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }

    "parse a combined config" >> {
      val sft = Speculator.getSft(combined, null, combined)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }

    "parse a combined config only" >> {
      val sft = Speculator.getSft(null, null, combined)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }
  }

  "GeoMesa Ingest Command" should {
    "work with sft and converter configs in files" >> {
      val confFile = new File(this.getClass.getClassLoader.getResource("examples/example1.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)


      val args = s"ingest --mock true -i test -u foo -p bar -c IngestAgumentsTest -fn $featureName".split("\\s+") ++
        Array( "-conf", confFile.getPath, "-fmt", "csv") ++
        Array(dataFile.getPath)

      Runner.main(args)
      success
    }
  }

}
