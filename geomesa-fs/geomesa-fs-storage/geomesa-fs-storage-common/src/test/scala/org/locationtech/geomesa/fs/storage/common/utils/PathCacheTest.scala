/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.nio.file.Files
import scala.concurrent.duration.DurationInt

@RunWith(classOf[JUnitRunner])
class PathCacheTest extends Specification {

  "PathCache" should {
    "update list cache when registering a new file" >> {
      val root = new Path(Files.createTempDirectory("geomesa").toFile.getPath)
      val fs = FileSystem.get(root.toUri, new Configuration())
      try {
        val file = new Path(root, "test")
        PathCache.exists(fs, file) must beFalse
        PathCache.list(fs, root) must beEmpty
        // create the file
        fs.create(file).close()
        fs.exists(file) must beTrue
        // verify cache has not been updated
        PathCache.exists(fs, file) must beFalse
        PathCache.list(fs, root) must beEmpty
        // register the file
        PathCache.register(fs, file)
        // verify cached values have been updated
        PathCache.exists(fs, file) must beTrue
        eventually(10, 100.millis)(PathCache.list(fs, root).toList must haveLength(1))
        // note: it's hard to verify this is a cached value, since it doesn't cache if a file doesn't exist...
        PathCache.status(fs, file) must not(beNull)
      } finally {
        fs.delete(root, true)
      }
    }
  }
}
