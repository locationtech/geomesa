/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b7629f50a0 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

<<<<<<< HEAD
=======
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.metadata.MetadataJson.MetadataPath
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

<<<<<<< HEAD
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

=======
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
@RunWith(classOf[JUnitRunner])
class MetadataJsonTest extends Specification {

  lazy val conf = new Configuration()
  lazy val fc = FileContext.getFileContext(conf)
<<<<<<< HEAD

  "MetadataJson" should {
    "persist and replace system properties (and environment variables)" in {
      skipped("fails in github actions")
=======
  val schemeOptions =

  "MetadataJson" should {
    "persist and replace system properties (and environment variables)" in {
>>>>>>> b9bdd406e3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
      withPath { context =>
        val prop = "MetadataJsonTest.foo"
        val interpolated = "${" + prop + "}"
        System.setProperty(prop, "bar")
        try {
          val opts = NamedOptions("jdbc", Map("user" -> "root", "password" -> interpolated))
          MetadataJson.writeMetadata(context, opts)
          val file = new Path(context.root, MetadataPath)
          val serialized = WithClose(context.fc.open(file))(is => IOUtils.toString(is, StandardCharsets.UTF_8))
          serialized must contain(interpolated)
          serialized must not(contain("bar"))
          val returned = MetadataJson.readMetadata(context)
          returned must beSome(NamedOptions("jdbc", Map("user" -> "root", "password" -> "bar")))
        } finally {
          System.clearProperty(prop)
        }
      }
    }
  }

  def withPath[R](code: FileSystemContext => R): R = {
    val file = Files.createTempDirectory("geomesa").toFile.getPath
    try { code(FileSystemContext(fc, conf, new Path(file))) } finally {
      FileUtils.deleteDirectory(new File(file))
    }
  }
}
