/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import java.util.concurrent.TimeUnit

import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloProperties
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.text.Suffixes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaBatchWriterConfigTest extends Specification {
  val bwc = GeoMesaBatchWriterConfig()    // Builds new BWC which has not been mutated by some other test.

  import AccumuloProperties.BatchWriterProperties

  sequential

  "GeoMesaBatchWriterConfig" should {
    "have defaults set" in {
      bwc.getMaxMemory                         must be equalTo BatchWriterProperties.WRITER_MEMORY_BYTES.toBytes.get
      bwc.getMaxLatency(TimeUnit.MILLISECONDS) must be equalTo BatchWriterProperties.WRITER_LATENCY.toDuration.get.toMillis
      bwc.getMaxWriteThreads                   must be equalTo BatchWriterProperties.WRITER_THREADS.default.toInt
    }
  }

  "GeoMesaBatchWriterConfig" should {
    "respect system properties" in {
      val latencyProp = "1234"
      val memoryProp  = "2345"
      val threadsProp = "25"
      val timeoutProp = "33"

      try {
        BatchWriterProperties.WRITER_LATENCY.threadLocalValue.set(latencyProp + " millis")
        BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.set(memoryProp)
        BatchWriterProperties.WRITER_THREADS.threadLocalValue.set(threadsProp)
        BatchWriterProperties.WRITE_TIMEOUT.threadLocalValue.set(timeoutProp + " millis")

        val nbwc = GeoMesaBatchWriterConfig()

        nbwc.getMaxLatency(TimeUnit.MILLISECONDS) must be equalTo java.lang.Long.parseLong(latencyProp)
        nbwc.getMaxMemory                         must be equalTo java.lang.Long.parseLong(memoryProp)
        nbwc.getMaxWriteThreads                   must be equalTo java.lang.Integer.parseInt(threadsProp)
        nbwc.getTimeout(TimeUnit.MILLISECONDS)    must be equalTo java.lang.Long.parseLong(timeoutProp)
      } finally {
        BatchWriterProperties.WRITER_LATENCY.threadLocalValue.remove()
        BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.remove()
        BatchWriterProperties.WRITER_THREADS.threadLocalValue.remove()
        BatchWriterProperties.WRITE_TIMEOUT.threadLocalValue.remove()
      }
    }
  }

  "GeoMesaBatchWriterConfig" should {
    "respect system properties for memory with a K suffix" in {
      val memoryProp = "1234K"
      BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.set(memoryProp)
      try {
        GeoMesaBatchWriterConfig().getMaxMemory mustEqual 1234l * 1024l
      } finally {
        BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.remove()
      }
    }

    "respect system properties for memory with a k suffix" in {
      val memoryProp = "1234k"
      BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.set(memoryProp)
      try {
        GeoMesaBatchWriterConfig().getMaxMemory mustEqual 1234l * 1024l
      } finally {
        BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.remove()
      }
    }

    "respect system properties for memory with a M suffix" in {
      val memoryProp = "1234M"
      BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.set(memoryProp)
      try {
        GeoMesaBatchWriterConfig().getMaxMemory mustEqual 1234l * 1024l * 1024l
      } finally {
        BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.remove()
      }
    }

    "respect system properties for memory with a m suffix" in {
      val memoryProp = "1234m"
      BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.set(memoryProp)
      try {
        GeoMesaBatchWriterConfig().getMaxMemory mustEqual 1234l * 1024l * 1024l
      } finally {
        BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.remove()
      }
    }

    "respect system properties for memory with a G suffix" in {
      val memoryProp = "1234G"
      BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.set(memoryProp)
      try {
        GeoMesaBatchWriterConfig().getMaxMemory mustEqual 1234l * 1024l * 1024l * 1024l
      } finally {
        BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.remove()
      }
    }

    "respect system properties for memory with a g suffix" in {
      val memoryProp = "1234g"
      BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.set(memoryProp)
      try {
        GeoMesaBatchWriterConfig().getMaxMemory mustEqual 1234l * 1024l * 1024l * 1024l
      } finally {
        BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.remove()
      }
    }

    "respect system properties for invalid non-suffixed memory specifications exceeding Long limits" in {
      // Long.MaxValue =  9223372036854775807
      val memoryProp   = "9999999999999999999b"
      BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.set(memoryProp)
      try {
        GeoMesaBatchWriterConfig().getMaxMemory mustEqual
            Suffixes.Memory.bytes(BatchWriterProperties.WRITER_MEMORY_BYTES.default).get
      } finally {
        BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.remove()
      }
    }

    "respect system properties for invalid suffixed memory specifications exceeding Long limits" in {
      val memoryProp = java.lang.Long.MAX_VALUE.toString + "k"
      BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.set(memoryProp)
      try {
        GeoMesaBatchWriterConfig().getMaxMemory mustEqual
            Suffixes.Memory.bytes(BatchWriterProperties.WRITER_MEMORY_BYTES.default).get
      } finally {
        BatchWriterProperties.WRITER_MEMORY_BYTES.threadLocalValue.remove()
      }
    }
  }

  "fetchMemoryProperty" should {
    val fooMemory = SystemProperty("foo", null)
    val bazMemory = SystemProperty("baz", null)
    "retrieve a long correctly" in {
      fooMemory.threadLocalValue.set("123456789")
      try {
        fooMemory.toBytes must beSome(123456789L)
      } finally {
        fooMemory.threadLocalValue.remove()
      }
    }

    "retrieve a long correctly with a K suffix" in {
      fooMemory.threadLocalValue.set("123456789K")
      try {
        fooMemory.toBytes must beSome(123456789l * 1024l)
      } finally {
        fooMemory.threadLocalValue.remove()
      }
    }

    "retrieve a long correctly with a k suffix" in {
      fooMemory.threadLocalValue.set("123456789k")
      try {
        fooMemory.toBytes must beSome(123456789l * 1024l)
      } finally {
        fooMemory.threadLocalValue.remove()
      }
    }

    "retrieve a long correctly with a M suffix" in {
      fooMemory.threadLocalValue.set("123456789m")
      try {
        fooMemory.toBytes must beSome(123456789l * 1024l * 1024l)
      } finally {
        fooMemory.threadLocalValue.remove()
      }
    }

    "retrieve a long correctly with a m suffix" in {
      fooMemory.threadLocalValue.set("123456789m")
      try {
        fooMemory.toBytes must beSome(123456789l * 1024l * 1024l)
      } finally {
        fooMemory.threadLocalValue.remove()
      }
    }

    "retrieve a long correctly with a G suffix" in {
      fooMemory.threadLocalValue.set("123456789G")
      try {
        fooMemory.toBytes must beSome(123456789l * 1024l * 1024l * 1024l)
      } finally {
        fooMemory.threadLocalValue.remove()
      }
    }

    "retrieve a long correctly with a g suffix" in {
      fooMemory.threadLocalValue.set("123456789g")
      try {
        fooMemory.toBytes must beSome(123456789l * 1024l * 1024l * 1024l)
      } finally {
        fooMemory.threadLocalValue.remove()
      }
    }

    "return None correctly" in {
      bazMemory.toBytes must beNone
    }

    "return None correctly when the System property is not parseable as a Long" in {
      bazMemory.threadLocalValue.set("fizzbuzz")
      try {
        bazMemory.toBytes must beNone
      } finally {
        bazMemory.threadLocalValue.remove()
      }
    }

    "return None correctly when the System property is not parseable as a Long with a suffix" in {
      bazMemory.threadLocalValue.set("64fizzbuzz")
      try {
        bazMemory.toBytes must beNone
      } finally {
        bazMemory.threadLocalValue.remove()
      }
    }

    "return None correctly when the System property is not parseable as a Long with a prefix" in {
      bazMemory.threadLocalValue.set("fizzbuzz64")
      try {
        bazMemory.toBytes must beNone
      } finally {
        bazMemory.threadLocalValue.remove()
      }
    }

    "return None correctly when the System property is not parseable with trailing garbage" in {
      bazMemory.threadLocalValue.set("64k bazbaz")
      try {
        bazMemory.toBytes must beNone
      } finally {
        bazMemory.threadLocalValue.remove()
      }
    }

    "return None correctly when the System property is not parseable with leading garbage" in {
      bazMemory.threadLocalValue.set("foofoo 64G")
      try {
        bazMemory.toBytes must beNone
      } finally {
        bazMemory.threadLocalValue.remove()
      }
    }
  }
}
