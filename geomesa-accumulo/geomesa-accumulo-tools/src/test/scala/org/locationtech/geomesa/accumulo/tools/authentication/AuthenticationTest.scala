/***********************************************************************
 * Crown Copyright (c) 2017-2019 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.authentication

import java.io.File

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloRunner}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AuthenticationTest extends Specification {

  sequential

  "GeoMesa Accumulo Commands" should {

    // These tests all invoke an ingest command using a mock instance
    // The authentication parameters aren't actually used by the mock instance, but these tests serve to test parameter
    // validation in AccumuloRunner and associated classes.

    val conf = ConfigFactory.load("examples/example1-csv.conf")
    val sft = conf.root().render(ConfigRenderOptions.concise())
    val converter = conf.root().render(ConfigRenderOptions.concise())
    val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)

    // Only test ingest, other commands use the same Accumulo Parameters anyway
    val cmd = Array("ingest")


    "fail without user" >> {
      val authArgs = Array("")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

    // With just user parameter, the user is prompted to enter a password at the command line  which we can't test here

    "work with user and password" >> {

      val authArgs = Array("--user", "root", "--password", "secret")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "userandpassword", "--converter", converter, "-s", sft, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList)
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }

    "work with user and keytab" >> {

      val authArgs = Array("--user", "root", "--keytab", "/path/to/some/file")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "userandkeytab", "--converter", converter, "-s", sft, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList)
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }


    "fail with user and password and keytab" >> {
      val authArgs = Array("--instance", "instance", "--zookeepers", "zoo", "--user", "user", "--password", "secret", "--keytab", "/path/to/some/file")
      val args = cmd ++ authArgs ++ Array("--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }
  }

}
