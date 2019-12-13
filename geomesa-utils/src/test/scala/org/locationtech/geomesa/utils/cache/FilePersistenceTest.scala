/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.cache

import java.io.File
import java.nio.file.{Files, Path}

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.io.PathUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilePersistenceTest extends Specification {

  def withTestDir[T](fn: (Path) => T): T = {
    val path = Files.createTempDirectory("gmFilePersistenceTest")
    try {
      fn(path)
    } finally {
      PathUtils.deleteRecursively(path)
    }

  }

  "FilePersistence" should {
    "fail for non-directories" in {
      withTestDir { dir =>
        val file = new File(dir.toFile, "foo")
        file.createNewFile() must beTrue
        new FilePersistence(file, "foo") must throwAn[IllegalArgumentException]
      }
    }
    "set and get values" in {
      withTestDir { dir =>
        val one = new FilePersistence(dir.toFile, "foo")
        one.persist("foo", "bar")
        one.persist("bar", "baz")
        one.persistAll(Map("fizz" -> "buzz", "baz" -> "blue"))

        one.keys() mustEqual Set("foo", "bar", "fizz", "baz")
        one.entries() mustEqual Set("bar" -> "baz", "baz" -> "blue", "foo" -> "bar", "fizz" -> "buzz")
        one.read("foo") must beSome("bar")
        one.read("bar") must beSome("baz")
        one.read("blue") must beNone

        one.remove("bar") must beTrue
        one.remove("blue") must beFalse
        one.removeAll(Seq("fizz", "baz"))

        one.keys() mustEqual Set("foo")
        one.entries() mustEqual Set("foo" -> "bar")
        one.read("foo") must beSome("bar")
        one.read("bar") must beNone
      }
    }
    "return properties by prefix" in {
      withTestDir { dir =>
        val one = new FilePersistence(dir.toFile, "foo")
        one.persist("foo", "bar")
        one.persist("bar", "baz")
        one.persistAll(Map("fizz" -> "buzz", "baz" -> "blue"))
        one.entries("b") mustEqual Set("bar" -> "baz", "baz" -> "blue")
      }
    }
    "persist properties across instances" in {
      withTestDir { dir =>
        val one = new FilePersistence(dir.toFile, "foo")
        one.persist("foo", "bar")
        one.persist("bar", "baz")
        one.persistAll(Map("fizz" -> "buzz", "baz" -> "blue"))
        val two = new FilePersistence(dir.toFile, "foo")
        two.entries() mustEqual one.entries()
      }
    }
  }
}
