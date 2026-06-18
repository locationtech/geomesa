/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.utils

import org.apache.iceberg.{DataFiles, FileFormat, PartitionSpec, Schema}
import org.apache.iceberg.types.Types
import org.geotools.api.feature.simple.SimpleFeature
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileSystemPathReader
import org.locationtech.geomesa.fs.storage.core.Partition
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.Closeable
import java.net.URI
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

@RunWith(classOf[JUnitRunner])
class FileSystemThreadedReaderTest extends Specification {

  private def createTestDataFile(path: String, count: Long): org.apache.iceberg.DataFile = {
    // Create a minimal schema and spec for testing
    val schema = new Schema(Types.NestedField.required(1, "name", Types.StringType.get()))
    val spec = PartitionSpec.unpartitioned()

    DataFiles.builder(spec)
      .withPath(s"file:///$path")
      .withFormat(FileFormat.PARQUET)
      .withFileSizeInBytes(1024)
      .withRecordCount(count)
      .build()
  }

  "FileSystemThreadedReader" should {
    "not hang when interrupted" in {
      val sft = SimpleFeatureTypes.createType("test", "name:String")
      val feature = ScalaSimpleFeature.create(sft, "1", "name")
      val featureGate = new LinkedBlockingQueue[Boolean](1)
      val reader = new FileSystemPathReader() {
        override def root: URI = new URI("/")
        override def read(file: URI): Iterator[SimpleFeature] with Closeable = {
          featureGate.take()
          CloseableIterator.single(feature)
        }
      }
      // ensure we have more files than threads so that we register phasers that don't complete right away
      val files = Seq.tabulate(10)(i => createTestDataFile(s"w_$i", i))
      WithClose(FileSystemThreadedReader(reader , files, 2)) { reader =>
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
