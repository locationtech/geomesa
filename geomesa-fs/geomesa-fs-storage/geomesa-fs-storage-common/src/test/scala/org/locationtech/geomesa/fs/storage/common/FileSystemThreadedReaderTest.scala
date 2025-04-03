/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common

import org.apache.hadoop.fs.Path
import org.geotools.api.feature.simple.SimpleFeature
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{StorageFile, StorageFilePath}
import org.locationtech.geomesa.fs.storage.common.AbstractFileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean


@RunWith(classOf[JUnitRunner])
class FileSystemThreadedReaderTest extends Specification {

  "FileSystemThreadedReader" should {
    "not hang when interrupted" in {
      val sft = SimpleFeatureTypes.createType("test", "name:String")
      val feature = ScalaSimpleFeature.create(sft, "1", "name")
      val featureGate = new LinkedBlockingQueue[Boolean](1)
      val reader = new FileSystemPathReader() {
        override def read(path: Path): CloseableIterator[SimpleFeature] = {
          featureGate.take()
          CloseableIterator.single(feature)
        }
      }
      // ensure we have more files than threads so that we register phasers that don't complete right away
      val files = Seq.tabulate(10)(i => StorageFilePath(StorageFile(s"$i", i), new Path(s"$i")))
      val readers = Iterator.single(reader -> files)
      WithClose(FileSystemThreadedReader(readers, 2)) { reader =>
        featureGate.put(false)
        reader.hasNext must beTrue
        reader.next() mustEqual feature
        reader.close()
        val gotNext = new AtomicBoolean(false)
        val hasNext = new AtomicBoolean(false)
        val future = CachedThreadPool.submit(() => {
          hasNext.set(reader.hasNext)
          gotNext.set(true)
        })
        try {
          eventually(gotNext.get() must beTrue)
          hasNext.get() must beFalse
        } finally {
          future.cancel(true)
        }
      }
    }
  }
}
