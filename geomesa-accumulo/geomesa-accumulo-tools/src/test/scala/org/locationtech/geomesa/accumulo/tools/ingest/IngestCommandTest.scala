/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.MiniCluster
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloRunner}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IngestCommandTest extends Specification {

  private val sftCounter = new AtomicInteger(0)

  def baseArgs: Array[String] = Array(
    "ingest",
    "--instance",      MiniCluster.cluster.getInstanceName,
    "--zookeepers",    MiniCluster.cluster.getZooKeepers,
    "--user",          MiniCluster.Users.root.name,
    "--password",      MiniCluster.Users.root.password,
    "--catalog",       s"${MiniCluster.namespace}.${getClass.getSimpleName}${sftCounter.getAndIncrement()}",
    "--compact-stats", "false"
  )

  "GeoMesa Accumulo Ingest Command" should {

    "work with sft and converter configs as strings using geomesa.sfts.<name> and geomesa.converters.<name>" in {
      val conf = ConfigFactory.load("examples/example1-csv.conf")
      val sft = conf.root().render(ConfigRenderOptions.concise())
      val converter = conf.root().render(ConfigRenderOptions.concise())
      val dataFile = new File(getClass.getClassLoader.getResource("examples/example1.csv").getFile)

      val args = baseArgs ++ Array("--converter", converter, "-s", sft, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      command.withDataStore { ds =>
        try {
          val features = SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList
          features must haveSize(3)
          features.map(_.getAttribute("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
        } finally {
          ds.delete()
        }
      }
    }

    "work with nested sft and converter configs as files" in {
      val confFile = new File(getClass.getClassLoader.getResource("examples/example1-csv.conf").getFile)
      val dataFile = new File(getClass.getClassLoader.getResource("examples/example1.csv").getFile)

      val args = baseArgs ++ Array("--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      command.withDataStore { ds =>
        try {
          val features = SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList
          features.size mustEqual 3
          features.map(_.getAttribute("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
        } finally {
          ds.delete()
        }
      }
    }

    "not ingest csv to tsv " in {
      val confFile = new File(getClass.getClassLoader.getResource("examples/example1-tsv.conf").getFile)
      val dataFile = new File(getClass.getClassLoader.getResource("examples/example1.csv").getFile)

      val args = baseArgs ++ Array("--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      command.withDataStore { ds =>
        try {
          val features = SelfClosingIterator(ds.getFeatureSource("renegades2").getFeatures.features).toList
          features must beEmpty
        } finally {
          ds.delete()
        }
      }
    }

    "ingest mysql to tsv" in {
      val confFile = new File(getClass.getClassLoader.getResource("examples/city-tsv.conf").getFile)
      val dataFile = new File(getClass.getClassLoader.getResource("examples/city.mysql").getFile)

      val args = baseArgs ++ Array("--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      command.withDataStore { ds =>
        try {
          val features = SelfClosingIterator(ds.getFeatureSource("geonames").getFeatures.features).toList
          features must haveSize(3)
        } finally {
          ds.delete()
        }
      }
    }

     "not ingest tsv to mysql" in {
      val confFile = new File(getClass.getClassLoader.getResource("examples/city-mysql.conf").getFile)
      val dataFile = new File(getClass.getClassLoader.getResource("examples/city.tsv").getFile)

      val args = baseArgs ++ Array("--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

       command.withDataStore { ds =>
         try {
           val features = SelfClosingIterator(ds.getFeatureSource("geonames").getFeatures.features).toList
           features must beEmpty
         } finally {
           ds.delete()
         }
       }
    }

    "ingest from tar files" in {
      val confFile = new File(getClass.getClassLoader.getResource("examples/example1-csv.conf").getFile)
      val dataFile = new File(getClass.getClassLoader.getResource("examples/example1.tar").getFile)

      val args = baseArgs ++ Array("--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      command.withDataStore { ds =>
        try {
          val features = SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList
          features must haveSize(5)
          features.map(_.getAttribute("name")) must
              containTheSameElementsAs(Seq("Hermione", "Harry", "Severus", "Ron", "Ginny"))
        } finally {
          ds.delete()
        }
      }
    }

    "ingest from zip files" in {
      val confFile = new File(getClass.getClassLoader.getResource("examples/example1-csv.conf").getFile)
      val dataFile = new File(getClass.getClassLoader.getResource("examples/example1.zip").getFile)

      val args = baseArgs ++ Array("--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      command.withDataStore { ds =>
        try {
          val features = SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList
          features must haveSize(5)
          features.map(_.getAttribute("name")) must
              containTheSameElementsAs(Seq("Hermione", "Harry", "Severus", "Ron", "Ginny"))
        } finally {
          ds.delete()
        }
      }
    }

    "ingest from tgz files" in {
      val confFile = new File(getClass.getClassLoader.getResource("examples/example1-csv.conf").getFile)
      val dataFile = new File(getClass.getClassLoader.getResource("examples/example1.tgz").getFile)

      val args = baseArgs ++ Array("--converter", confFile.getPath, "-s", confFile.getPath, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      command.withDataStore { ds =>
        try {
          val features = SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList
          features must haveSize(5)
          features.map(_.getAttribute("name")) must
              containTheSameElementsAs(Seq("Hermione", "Harry", "Severus", "Ron", "Ginny"))
        } finally {
          ds.delete()
        }
      }
    }

    "fail without user" >> {
      val dataFile = new File(getClass.getClassLoader.getResource("examples/example1.csv").getFile)
      val base = baseArgs
      val i = base.indexOf("--user")
      val args = base.slice(0, i) ++ base.slice(i + 2, base.length) ++ Array(dataFile.getPath)
      AccumuloRunner.parseCommand(args) must throwA[com.beust.jcommander.ParameterException]
    }

    "fail with user and password and keytab" >> {
      val dataFile = new File(getClass.getClassLoader.getResource("examples/example1.csv").getFile)
      val args = baseArgs ++ Array("--keytab", "/path/to/some/file", dataFile.getPath)
      AccumuloRunner.parseCommand(args) must throwA[com.beust.jcommander.ParameterException]
    }

    // TODO GEOMESA-2797 get kerberos tests working
    //    "work with user and keytab" >> {
    //
    //      val authArgs = Array("--user", "root", "--keytab", "/path/to/some/file")
    //      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "userandkeytab", "--converter", converter, "-s", sft, dataFile.getPath)
    //
    //      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
    //      command.execute()
    //
    //      val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList)
    //      features.size mustEqual 3
    //      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    //    }

    // TODO GEOMESA-529 more testing of explicit commands

  }
}
