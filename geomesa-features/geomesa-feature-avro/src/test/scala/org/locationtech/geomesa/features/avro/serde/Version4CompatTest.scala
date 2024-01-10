/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro.serde

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.io.AvroDataFileReader
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.{File, FileInputStream}

@RunWith(classOf[JUnitRunner])
class Version4CompatTest extends Specification with LazyLogging {

  def read(file: File): List[SimpleFeature] = new AvroDataFileReader(new FileInputStream(file)).toList

  val files: Seq[String] = Seq("example-v4.avro", "example-v4-with-vis.avro", "example-v5.avro", "example-v5-with-vis.avro")
  val gt20files: Seq[String] = Seq("example-v4-gt18.avro", "example-v4-with-vis-gt18.avro")

  "Current Reader" should {
    "read version 4 avro with new Hints package name and version 5 avro" >> {
       forall(files) { filename =>
         logger.debug(s"Processing $filename")
         val file: File = new File(getClass.getClassLoader.getResource(filename).toURI)
         val list = read(file)
         list.size mustEqual 3
       }
    }
    "read version 4 avro with old Hints package name" >> {
      forall(gt20files) { filename =>
        logger.debug(s"Processing $filename")
        val file: File = new File(getClass.getClassLoader.getResource(filename).toURI)
        val list = read(file)
        list.size mustEqual 3
      }
    }
  }
}
