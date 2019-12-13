/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.orc

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class OrcFileSystemWriterTest extends Specification {

  val sft = SimpleFeatureTypes.createType("orc-test", "name:String,age:Int,dtg:Date,*geom:LineString:srid=4326")

  val features = Seq(
    ScalaSimpleFeature.create(sft, "0", "name0", "0", "2017-01-01T00:00:00.000Z", "LINESTRING (10 0, 5 0)"),
    ScalaSimpleFeature.create(sft, "1", "name1", "1", "2017-01-01T00:00:01.000Z", "LINESTRING (10 1, 5 1)")
  )

  val config = new Configuration()

  "OrcFileSystemWriter" should {
    "write and read simple features" in {

      withPath { path =>
        withTestFile { file =>
          WithClose(new OrcFileSystemWriter(sft, config, file)) { writer => features.foreach(writer.write) }
          val reader = new OrcFileSystemReader(sft, config, None, None)
          val read = WithClose(reader.read(file)) { i => SelfClosingIterator(i).map(ScalaSimpleFeature.copy).toList }
          read mustEqual features
          // test out not calling 'hasNext'
          var i = 0
          WithClose(reader.read(file)) { iter =>
            while (i < features.size) {
              iter.next() mustEqual features(i)
              i += 1
            }
            iter.next must throwA[NoSuchElementException]
          }
        }
      }
    }
  }

  def withTestFile[R](code: Path => R): R = {
    val file = Files.createTempFile("gm-orc-test", "")
    file.toFile.delete()
    try { code(new Path(file.toUri)) } finally {
      file.toFile.delete() || { file.toFile.deleteOnExit(); true }
    }
  }

  def withPath[R](code: Path => R): R = {
    val file = Files.createTempDirectory("geomesa").toFile.getPath
    try { code(new Path(file)) } finally {
      FileUtils.deleteDirectory(new File(file))
    }
  }
}
