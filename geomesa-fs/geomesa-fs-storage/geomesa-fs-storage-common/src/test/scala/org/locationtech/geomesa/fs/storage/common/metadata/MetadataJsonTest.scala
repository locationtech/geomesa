/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.metadata.MetadataJson.MetadataPath
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MetadataJsonTest extends Specification {

  lazy val conf = new Configuration()
  lazy val fc = FileContext.getFileContext(conf)

  "MetadataJson" should {
    "persist and replace system properties (and environment variables)" in {
      skipped("fails in github actions")
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
