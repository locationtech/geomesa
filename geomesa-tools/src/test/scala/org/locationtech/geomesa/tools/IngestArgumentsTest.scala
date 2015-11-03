package org.locationtech.geomesa.tools

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.Utils.Speculator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IngestArgumentsTest extends Specification {

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


  "Speculator" should {
    "work with sft and converter configs" >> {
      val sftOpt = Speculator.getSft(sftSpec, Some(featureName))
      sftOpt.isDefined mustEqual true
      val sft = sftOpt.get
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }

    "fail when given spec and no feature name" >> {
      val sftOpt = Speculator.getSft(sftSpec, Some(null))
      sftOpt.isDefined mustEqual false
    }

    "create a spec from sft conf" >> {
      val sftOpt = Speculator.getSft(sftConfig, Some(null))
      sftOpt.isDefined mustEqual true
      val sft = sftOpt.get
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }
  }

//  "GeoMesa Ingest Command" should {
//    "work with sft and converter configs" >> {
//
//
//      val args = s"ingest --mock true -i test -u root -p secret -c IngestAgumentsTest -fn $featureName".split("\\s+") ++
//        Array("-s", sftConfig, "--convert", convertConfig) ++
//        Array("/tmp/foobar")
//
//
//      Runner.main(args)
//
//      success
//
//
//    }
//  }

}
