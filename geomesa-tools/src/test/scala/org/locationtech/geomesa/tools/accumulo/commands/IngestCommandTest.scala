/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import java.io.File

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.accumulo.AccumuloRunner
import org.locationtech.geomesa.utils.geotools.Conversions
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IngestCommandTest extends Specification {

  sequential

  "GeoMesa Accumulo Ingest Command" should {
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
        AccumuloDataStoreParams.instanceIdParam.getName -> id,
        AccumuloDataStoreParams.zookeepersParam.getName -> "zoo1:2181,zoo2:2181,zoo3:2181",
        AccumuloDataStoreParams.userParam.getName       -> "foo",
        AccumuloDataStoreParams.passwordParam.getName   -> "bar",
        AccumuloDataStoreParams.tableNameParam.getName  -> id,
        AccumuloDataStoreParams.mockParam.getName       -> "true")
      DataStoreFinder.getDataStore(paramMap).asInstanceOf[AccumuloDataStore]
    }

    "work with sft and converter configs as strings using geomesa.sfts.<name> and geomesa.converters.<name>" >> {
      val id = nextId

      val conf = ConfigFactory.load("examples/example1.conf")
      val sft = conf.root().render(ConfigRenderOptions.concise())
      val converter = conf.root().render(ConfigRenderOptions.concise())
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)

      val args = Array("ingest", "--mock", "-i", id, "-u", "foo", "-p", "bar", "-c", id,
        "--converter", converter, "-s", sft, dataFile.getPath)
      args.length mustEqual 15

      AccumuloRunner.createCommand(args).execute()

      val ds = getDS(id)
      import Conversions._
      val features = ds.getFeatureSource("renegades").getFeatures.features().toList
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }

    "work with nested sft and converter configs as files" >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/example1.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)

      val args = Array("ingest", "--mock", "-i", id, "-u", "foo", "-p", "bar", "-c", id,
        "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 15

      AccumuloRunner.createCommand(args).execute()

      val ds = getDS(id)
      import Conversions._
      val features = ds.getFeatureSource("renegades").getFeatures.features().toList
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }

    // TODO GEOMESA-529 more testing of explicit commands

  }

}
