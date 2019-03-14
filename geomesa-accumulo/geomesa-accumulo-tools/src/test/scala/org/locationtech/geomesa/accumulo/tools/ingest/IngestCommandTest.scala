/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import java.io.File

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloRunner}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IngestCommandTest extends Specification {

  sequential

  "GeoMesa Accumulo Ingest Command" should {

    // Mock shouldn't require instance or zookeepers
    val baseArgs = Array("ingest", "--instance", "instance", "--zookeepers", "zoo", "--mock", "--user", "myuser", "--password", "mypassword")

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

      val args = baseArgs ++ Array("--catalog", id, "--converter", converter, "-s", sft, dataFile.getPath)
      args.length mustEqual 17

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList)
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }

    "work with nested sft and converter configs as files" >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/example1-csv.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)

      val args = baseArgs ++ Array("--catalog", id, "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 17

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList)
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }

    "not ingest csv to tsv " >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/example1-tsv.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)

      val args = baseArgs ++ Array("--catalog", id, "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 17

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("renegades2").getFeatures.features).toList)
      features.size mustEqual 0
    }

    "ingest mysql to tsv" >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/city-tsv.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/city.mysql").getFile)

      val args = baseArgs ++ Array("--catalog", id, "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 17

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("geonames").getFeatures.features).toList)
      features.size mustEqual 3
    }

     "not ingest tsv to mysql" >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/city-mysql.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/city.tsv").getFile)

      val args = baseArgs ++ Array("--catalog", id, "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 17

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

       val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("geonames").getFeatures.features).toList)
      features.size mustEqual 0
    }

    "ingest from tar files" >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/example1-csv.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.tar").getFile)

      val args = baseArgs ++ Array("--catalog", id, "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 17

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore { ds =>
        SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList
      }
      features.size mustEqual 5
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus", "Ron", "Ginny"))
    }

    "ingest from zip files" >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/example1-csv.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.zip").getFile)

      val args = baseArgs ++ Array("--catalog", id, "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 17

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore { ds =>
        SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList
      }
      features.size mustEqual 5
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus", "Ron", "Ginny"))
    }

    "ingest from tgz files" >> {
      val id = nextId

      val confFile = new File(this.getClass.getClassLoader.getResource("examples/example1-csv.conf").getFile)
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.tgz").getFile)

      val args = baseArgs ++ Array("--catalog", id, "--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)
      args.length mustEqual 17

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore { ds =>
        SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList
      }
      features.size mustEqual 5
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus", "Ron", "Ginny"))
    }

    "work with mock with provided ZK and instance and default accumulo passwords!" >> {
      val id = nextId

      val conf = ConfigFactory.load("examples/example1-csv.conf")
      val sft = conf.root().render(ConfigRenderOptions.concise())
      val converter = conf.root().render(ConfigRenderOptions.concise())
      val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)

      val combinations =
        Seq (
          (Array("-z", "foobar", "-i", "hello"), 17)
        )

      val userCombinations = Seq (
        ("myuser", "mypassword"),
        ("root", "secret"), // default
        ("root", "other")  // default user
      )

      combinations.zip(userCombinations).forall { case ((additionalArgs, count), (user, pass)) =>
        val testArgs = Array("ingest", "--mock", "--user", user, "--password", pass)

        val args = testArgs ++ Array("--catalog", id, "--converter", converter, "-s", sft, dataFile.getPath) ++ additionalArgs

        args.length mustEqual count

        val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
        command.execute()

        val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList)
        features.size mustEqual 3
        features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
      }

    }

    // TODO GEOMESA-529 more testing of explicit commands

  }

}
