/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IncrementingFileNameTest extends Specification with LazyLogging {

  "IncrementingFileName" should {
    "increment regular file names" in {
      new IncrementingFileName("foo.csv").take(3).toList mustEqual Seq("foo_000.csv", "foo_001.csv", "foo_002.csv")
    }

    "increment regular file paths" in {
      new IncrementingFileName("/path/to/foo.json").take(3).toList mustEqual
          Seq("/path/to/foo_000.json", "/path/to/foo_001.json", "/path/to/foo_002.json")
    }

    "increment file names with no extension" in {
      new IncrementingFileName("foo_bar").take(3).toList mustEqual Seq("foo_bar_000", "foo_bar_001", "foo_bar_002")
    }

    "increment compressed file names" in {
      new IncrementingFileName("foo.tgz").take(2).toList mustEqual Seq("foo_000.tgz", "foo_001.tgz")
      new IncrementingFileName("foo.tar.gz").take(2).toList mustEqual Seq("foo_000.tar.gz", "foo_001.tar.gz")
      new IncrementingFileName("foo.tbz").take(2).toList mustEqual Seq("foo_000.tbz", "foo_001.tbz")
      new IncrementingFileName("foo.tar.bz2").take(2).toList mustEqual Seq("foo_000.tar.bz2", "foo_001.tar.bz2")
      new IncrementingFileName("foo.txz").take(2).toList mustEqual Seq("foo_000.txz", "foo_001.txz")
      new IncrementingFileName("foo.tar.xz").take(2).toList mustEqual Seq("foo_000.tar.xz", "foo_001.tar.xz")
    }

    "increment uncompressed file names" in {
      new IncrementingFileName("foo.bar.txt").take(2).toList mustEqual Seq("foo.bar_000.txt", "foo.bar_001.txt")
    }

    "maintain uniqueness if formatting is exceeded" in {
      new IncrementingFileName("foo.txt", 1).take(12).toList mustEqual
          Seq("foo_0.txt", "foo_1.txt", "foo_2.txt", "foo_3.txt", "foo_4.txt", "foo_5.txt", "foo_6.txt",
            "foo_7.txt", "foo_8.txt", "foo_9.txt", "foo_10.txt", "foo_11.txt")
    }
  }
}


