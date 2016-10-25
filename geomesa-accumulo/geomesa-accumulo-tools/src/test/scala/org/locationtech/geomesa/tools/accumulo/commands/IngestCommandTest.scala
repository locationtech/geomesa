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
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.accumulo.{AccumuloRunner, GeoMesaConnectionParams}
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

    "work with sft and converter configs as strings using geomesa.sfts.<name> and geomesa.converters.<name>" >> {
      val id = nextId

      val conf = ConfigFactory.load("examples/example1-csv.conf")
      val sft = conf.root().render(ConfigRenderOptions.concise())
      val converter = conf.root().render(ConfigRenderOptions.concise())
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)

      val args = Array("ingest", "--mock", "-i", id, "-u", "foo", "-p", "bar", "-c", id,
        "--converter", converter, "-s", sft, dataFile.getPath)
      args.length mustEqual 15

      val command = AccumuloRunner.createCommand(args)
      command.execute()

      val ds = DataStoreParamsHelper.createDataStore(command.params.asInstanceOf[GeoMesaConnectionParams])
      import Conversions._
      val features = ds.getFeatureSource("renegades").getFeatures.features().toList
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }

    "work with nested sft and converter configs as files" >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/example1-csv.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)

      val args = Array("ingest", "--mock", "-i", id, "-u", "foo", "-p", "bar", "-c", id,
        "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 15

      val command = AccumuloRunner.createCommand(args)
      command.execute()

      val ds = DataStoreParamsHelper.createDataStore(command.params.asInstanceOf[GeoMesaConnectionParams])
      import Conversions._
      val features = ds.getFeatureSource("renegades").getFeatures.features().toList
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }

    "not ingest csv to tsv " >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/example1-tsv.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)

      val args = Array("ingest", "--mock", "-i", id, "-u", "foo", "-p", "bar", "-c", id,
        "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 15

      val command = AccumuloRunner.createCommand(args)
      command.execute()

      val ds = DataStoreParamsHelper.createDataStore(command.params.asInstanceOf[GeoMesaConnectionParams])
      import Conversions._
      val features = ds.getFeatureSource("renegades2").getFeatures.features().toList
      features.size mustEqual 0
    }

    "ingest mysql to tsv" >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/city-tsv.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/city.mysql").getFile)

      val args = Array("ingest", "--mock", "-i", id, "-u", "foo", "-p", "bar", "-c", id,
        "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 15

      val command = AccumuloRunner.createCommand(args)
      command.execute()

      val ds = DataStoreParamsHelper.createDataStore(command.params.asInstanceOf[GeoMesaConnectionParams])
      import Conversions._
      val features = ds.getFeatureSource("geonames").getFeatures.features().toList
      features.size mustEqual 3
    }

     "not ingest tsv to mysql" >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/city-mysql.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/city.tsv").getFile)

      val args = Array("ingest", "--mock", "-i", id, "-u", "foo", "-p", "bar", "-c", id,
        "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 15

      val command = AccumuloRunner.createCommand(args)
      command.execute()

      val ds = DataStoreParamsHelper.createDataStore(command.params.asInstanceOf[GeoMesaConnectionParams])
      import Conversions._
      val features = ds.getFeatureSource("geonames").getFeatures.features().toList
      features.size mustEqual 0
    }
    // TODO GEOMESA-529 more testing of explicit commands

  }

}
