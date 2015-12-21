package org.locationtech.geomesa.tools

import java.io.File

import com.beust.jcommander.ParameterException
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.params
import org.locationtech.geomesa.utils.geotools.Conversions
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
      val sft = SftArgParser.getSft(sftSpec, featureName, convertConfig)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }

    "fail when given spec and no feature name" >> {
      SftArgParser.getSft(sftSpec, null, convertConfig) must throwA[ParameterException]
    }

    "create a spec from sft conf" >> {
      val sft = SftArgParser.getSft(sftConfig, null, convertConfig)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }

    "parse a combined config" >> {
      val sft = SftArgParser.getSft(combined, null, combined)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }

    "parse a combined config only" >> {
      val sft = SftArgParser.getSft(null, null, combined)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }
  }

  "GeoMesa Ingest Command" should {
    // needed for travis build...
    System.setProperty("geomesa.tools.accumulo.site.xml",
      this.getClass.getClassLoader.getResource("accumulo-site.xml").getFile)

    var n = 0
    def nextId = {
      n += 1
      this.getClass.getSimpleName + n.toString
    }

    def getDS(id: String) = {
      import scala.collection.JavaConversions._
      def paramMap = Map[String, String](
      params.instanceIdParam.getName -> id,
      params.zookeepersParam.getName -> "zoo1:2181,zoo2:2181,zoo3:2181",
      params.userParam.getName       -> "foo",
      params.passwordParam.getName   -> "bar",
      params.tableNameParam.getName  -> id,
      params.mockParam.getName       -> "true")
      DataStoreFinder.getDataStore(paramMap).asInstanceOf[AccumuloDataStore]
    }

    "work with sft and converter configs in files" >> {
      val id = nextId
      val confFile = new File(this.getClass.getClassLoader.getResource("examples/example1.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)
      val args = (s"ingest --mock true -i $id -u foo -p bar -c $id " +
        s"-conf  ${confFile.getPath} ${dataFile.getPath}").split("\\s+")
      args.length mustEqual 14

      Runner.main(args)

      val ds = getDS(id)
      import Conversions._
      val features = ds.getFeatureSource("renegades").getFeatures.features().toList
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }

    // TODO GEOMESA-529 more testing of explicit commands

  }

}
