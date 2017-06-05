/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMesaBatchWriterConfigTest extends Specification {
  val bwc = GeoMesaBatchWriterConfig.buildBWC    // Builds new BWC which has not been mutated by some other test.

  import AccumuloProperties.BatchWriterProperties

  sequential

  "GeoMesaBatchWriterConfig" should {
    "have defaults set" in {
      bwc.getMaxMemory                         must be equalTo BatchWriterProperties.WRITER_MEMORY_BYTES.default.toLong
      bwc.getMaxLatency(TimeUnit.MILLISECONDS) must be equalTo BatchWriterProperties.WRITER_LATENCY_MILLIS.default.toLong
      bwc.getMaxWriteThreads                   must be equalTo BatchWriterProperties.WRITER_THREADS.default.toInt
    }
  }

  "GeoMesaBatchWriterConfig" should {
    "respect system properties" in {
      val latencyProp = "1234"
      val memoryProp  = "2345"
      val threadsProp = "25"
      val timeoutProp = "33"

      System.setProperty(BatchWriterProperties.WRITER_LATENCY_MILLIS.property, latencyProp)
      System.setProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property, memoryProp)
      System.setProperty(BatchWriterProperties.WRITER_THREADS.property, threadsProp)
      System.setProperty(BatchWriterProperties.WRITE_TIMEOUT_MILLIS.property, timeoutProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC

      System.clearProperty(BatchWriterProperties.WRITER_LATENCY_MILLIS.property)
      System.clearProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property)
      System.clearProperty(BatchWriterProperties.WRITER_THREADS.property)
      System.clearProperty(BatchWriterProperties.WRITE_TIMEOUT_MILLIS.property)

      nbwc.getMaxLatency(TimeUnit.MILLISECONDS) must be equalTo java.lang.Long.parseLong(latencyProp)
      nbwc.getMaxMemory                         must be equalTo java.lang.Long.parseLong(memoryProp)
      nbwc.getMaxWriteThreads                   must be equalTo java.lang.Integer.parseInt(threadsProp)
      nbwc.getTimeout(TimeUnit.MILLISECONDS)    must be equalTo java.lang.Long.parseLong(timeoutProp)
    }
  }

  "GeoMesaBatchWriterConfig" should {
    "respect system properties for memory with a K suffix" in {
      val memoryProp  = "1234K"
      System.setProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property, memoryProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC
      System.clearProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property)

      nbwc.getMaxMemory must be equalTo java.lang.Long.valueOf(1234l * 1024l)
    }

    "respect system properties for memory with a k suffix" in {
      val memoryProp  = "1234k"
      System.setProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property, memoryProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC
      System.clearProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property)

      nbwc.getMaxMemory must be equalTo java.lang.Long.valueOf(1234l * 1024l)
    }

    "respect system properties for memory with a M suffix" in {
      val memoryProp  = "1234M"
      System.setProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property, memoryProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC
      System.clearProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property)

      nbwc.getMaxMemory must be equalTo java.lang.Long.valueOf(1234l * 1024l * 1024l)
    }

    "respect system properties for memory with a m suffix" in {
      val memoryProp  = "1234m"
      System.setProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property, memoryProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC
      System.clearProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property)

      nbwc.getMaxMemory must be equalTo java.lang.Long.valueOf(1234l * 1024l * 1024l)
    }

    "respect system properties for memory with a G suffix" in {
      val memoryProp  = "1234G"
      System.setProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property, memoryProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC
      System.clearProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property)

      nbwc.getMaxMemory must be equalTo java.lang.Long.valueOf(1234l * 1024l * 1024l * 1024l)
    }

    "respect system properties for memory with a g suffix" in {
      val memoryProp  = "1234g"
      System.setProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property, memoryProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC
      System.clearProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property)

      nbwc.getMaxMemory must be equalTo java.lang.Long.valueOf(1234l * 1024l * 1024l * 1024l)
    }

    "respect system properties for invalid non-suffixed memory specifications exceeding Long limits" in {
      //java.Long.MAX_VALUE =  9223372036854775807
      val memoryProp        = "9999999999999999999"
      System.setProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property, memoryProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC
      System.clearProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property)

      nbwc.getMaxMemory must be equalTo BatchWriterProperties.WRITER_MEMORY_BYTES.default.toLong
    }

    "respect system properties for invalid suffixed memory specifications exceeding Long limits" in {
      val memoryProp = java.lang.Long.MAX_VALUE.toString + "k"
      System.setProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property, memoryProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC
      System.clearProperty(BatchWriterProperties.WRITER_MEMORY_BYTES.property)

      nbwc.getMaxMemory must be equalTo BatchWriterProperties.WRITER_MEMORY_BYTES.default.toLong
    }
  }

  "fetchProperty" should {
    "retrieve a long correctly" in {
      System.setProperty("foo", "123456789")
      val ret = GeoMesaBatchWriterConfig.fetchProperty(SystemProperty("foo", null))
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l))
    }

    "return None correctly" in {
      GeoMesaBatchWriterConfig.fetchProperty(SystemProperty("bar", null)) should equalTo(None)
    }

    "return None correctly when the System property is not parseable as a Long" in {
      System.setProperty("baz", "fizzbuzz")
      val ret = GeoMesaBatchWriterConfig.fetchProperty(SystemProperty("foo", null))
      System.clearProperty("baz")
      ret should equalTo(None)
    }
  }

  "fetchMemoryProperty" should {
    val fooMemory = SystemProperty("foo", null)
    val bazMemory = SystemProperty("baz", null)
    "retrieve a long correctly" in {
      System.setProperty("foo", "123456789")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(fooMemory)
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l))
    }

    "retrieve a long correctly with a K suffix" in {
      System.setProperty("foo", "123456789K")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(fooMemory)
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l * 1024l))
    }

    "retrieve a long correctly with a k suffix" in {
      System.setProperty("foo", "123456789k")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(fooMemory)
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l * 1024l))
    }

    "retrieve a long correctly with a M suffix" in {
      System.setProperty("foo", "123456789m")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(fooMemory)
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l * 1024l * 1024l))
    }

    "retrieve a long correctly with a m suffix" in {
      System.setProperty("foo", "123456789m")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(fooMemory)
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l * 1024l * 1024l))
    }

    "retrieve a long correctly with a G suffix" in {
      System.setProperty("foo", "123456789G")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(fooMemory)
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l * 1024l * 1024l * 1024l))
    }

    "retrieve a long correctly with a g suffix" in {
      System.setProperty("foo", "123456789g")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(fooMemory)
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l * 1024l * 1024l * 1024l))
    }

    "return None correctly" in {
      GeoMesaBatchWriterConfig.fetchMemoryProperty(bazMemory) should equalTo(None)
    }

    "return None correctly when the System property is not parseable as a Long" in {
      System.setProperty("baz", "fizzbuzz")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(bazMemory)
      System.clearProperty("baz")
      ret should equalTo(None)
    }

    "return None correctly when the System property is not parseable as a Long with a suffix" in {
      System.setProperty("baz", "64fizzbuzz")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(fooMemory)
      System.clearProperty("baz")
      ret should equalTo(None)
    }

    "return None correctly when the System property is not parseable as a Long with a prefix" in {
      System.setProperty("baz", "fizzbuzz64")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(bazMemory)
      System.clearProperty("baz")
      ret should equalTo(None)
    }

    "return None correctly when the System property is not parseable with trailing garbage" in {
      System.setProperty("baz", "64k bazbaz")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(bazMemory)
      System.clearProperty("baz")
      ret should equalTo(None)
    }

    "return None correctly when the System property is not parseable with leading garbage" in {
      System.setProperty("baz", "foofoo 64G")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty(bazMemory)
      System.clearProperty("baz")
      ret should equalTo(None)
    }
  }
}

