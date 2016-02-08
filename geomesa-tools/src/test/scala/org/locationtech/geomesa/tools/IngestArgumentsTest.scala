/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import java.io.File

import com.beust.jcommander.ParameterException
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.apache.commons.io.FileUtils
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
    "work with " >> {
      val sft = CLArgResolver.getSft(sftSpec, featureName)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }

    "fail when given spec and no feature name" >> {
      CLArgResolver.getSft(sftSpec, null) must throwA[ParameterException]
    }

    "create a spec from sft conf" >> {
      val sft = CLArgResolver.getSft(sftConfig, null)
      sft.getAttributeCount mustEqual 3
      sft.getDescriptor(0).getLocalName mustEqual "name"
      sft.getDescriptor(1).getLocalName mustEqual "age"
      sft.getDescriptor(2).getLocalName mustEqual "geom"
    }

    "parse a combined config" >> {
      val sft = CLArgResolver.getSft(combined, null)
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

    "work with sft and converter configs as strings" >> {
      val id = nextId
      val conf = ConfigFactory.load("examples/example1.conf")
      val sft = conf.getConfigList("geomesa.sfts").get(0).root().render(ConfigRenderOptions.concise())
      val converter = conf.getConfigList("geomesa.converters").get(0).root().render(ConfigRenderOptions.concise())
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)
      val args = Array("ingest", "--mock", "-i", id, "-u", "foo", "-p", "bar", "-c", id,
        "--converter", converter, "-s", sft, dataFile.getPath)
      args.length mustEqual 15

      Runner.createCommand(args).execute()

      val ds = getDS(id)
      import Conversions._
      val features = ds.getFeatureSource("renegades").getFeatures.features().toList
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }

    "work with sft and converter configs as files" >> {
      val id = nextId
      val conf = ConfigFactory.load("examples/example1.conf")
      val sft = conf.getConfigList("geomesa.sfts").get(0).root().render(ConfigRenderOptions.concise())
      val sftFile = File.createTempFile("geomesa", "sft")
      sftFile.createNewFile()
      sftFile.deleteOnExit()
      FileUtils.write(sftFile, sft)

      val converter = conf.getConfigList("geomesa.converters").get(0).root().render(ConfigRenderOptions.concise())
      val convertFile = File.createTempFile("geomesa", "convert")
      convertFile.createNewFile()
      convertFile.deleteOnExit()
      FileUtils.write(convertFile, converter)

      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)
      val args = Array("ingest", "--mock", "-i", id, "-u", "foo", "-p", "bar", "-c", id,
        "--converter", convertFile.getPath, "-s", sftFile.getPath, dataFile.getPath)
      args.length mustEqual 15

      Runner.createCommand(args).execute()

      val ds = getDS(id)
      import Conversions._
      val features = ds.getFeatureSource("renegades").getFeatures.features().toList
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }

    // TODO GEOMESA-529 more testing of explicit commands

  }

}
