package org.locationtech.geomesa.accumulo.util

import java.util.concurrent.TimeUnit

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class GeoMesaBatchWriterConfigTest extends Specification {
  val bwc = GeoMesaBatchWriterConfig.buildBWC    // Builds new BWC which has not been mutated by some other test.

  sequential

  "GeoMesaBatchWriterConfig" should {
    "have defaults set" in {
      bwc.getMaxMemory                         must be equalTo GeoMesaBatchWriterConfig.DEFAULT_MAX_MEMORY
      bwc.getMaxLatency(TimeUnit.MILLISECONDS) must be equalTo GeoMesaBatchWriterConfig.DEFAULT_LATENCY
      bwc.getMaxWriteThreads                   must be equalTo GeoMesaBatchWriterConfig.DEFAULT_THREADS
    }
  }

  "GeoMesaBatchWriterConfig" should {
    "respect system properties" in {
      val latencyProp = "1234"
      val memoryProp  = "2345"
      val threadsProp = "25"
      val timeoutProp = "33"

      System.setProperty(GeoMesaBatchWriterConfig.WRITER_LATENCY_MILLIS, latencyProp)
      System.setProperty(GeoMesaBatchWriterConfig.WRITER_MEMORY,         memoryProp)
      System.setProperty(GeoMesaBatchWriterConfig.WRITER_THREADS,        threadsProp)
      System.setProperty(GeoMesaBatchWriterConfig.WRITE_TIMEOUT,         timeoutProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC

      System.clearProperty(GeoMesaBatchWriterConfig.WRITER_LATENCY_MILLIS)
      System.clearProperty(GeoMesaBatchWriterConfig.WRITER_MEMORY)
      System.clearProperty(GeoMesaBatchWriterConfig.WRITER_THREADS)
      System.clearProperty(GeoMesaBatchWriterConfig.WRITE_TIMEOUT)

      nbwc.getMaxLatency(TimeUnit.MILLISECONDS) must be equalTo java.lang.Long.parseLong(latencyProp)
      nbwc.getMaxMemory                         must be equalTo java.lang.Long.parseLong(memoryProp)
      nbwc.getMaxWriteThreads                   must be equalTo java.lang.Integer.parseInt(threadsProp)
      nbwc.getTimeout(TimeUnit.SECONDS)         must be equalTo java.lang.Long.parseLong(timeoutProp)
    }
  }

  "fetchProperty" should {
    "retrieve a long correctly" in {
      System.setProperty("foo", "123456789")
      val ret = GeoMesaBatchWriterConfig.fetchProperty("foo")
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l))
    }

    "return None correctly" in {
      GeoMesaBatchWriterConfig.fetchProperty("bar") should equalTo(None)
    }

    "return None correctly when the System property is not parseable as a Long" in {
      System.setProperty("baz", "fizzbuzz")
      val ret = GeoMesaBatchWriterConfig.fetchProperty("foo")
      System.clearProperty("baz")
      ret should equalTo(None)
    }
  }

  "fetchMemoryProperty" should {
    "retrieve a long correctly" in {
      System.setProperty("foo", "123456789")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty("foo")
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l))
    }

    "return None correctly" in {
      GeoMesaBatchWriterConfig.fetchMemoryProperty("bar") should equalTo(None)
    }

    "return None correctly when the System property is not parseable as a Long" in {
      System.setProperty("baz", "fizzbuzz")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty("foo")
      System.clearProperty("baz")
      ret should equalTo(None)
    }

    "retrieve a long correctly with a suffix" in {
      System.setProperty("foo", "123456789K")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty("foo")
      System.clearProperty("foo")
      ret should equalTo(Some(123456789l * 1024l))
    }

    "return None correctly when the System property is not parseable as a Long with a suffix" in {
      System.setProperty("baz", "64fizzbuzz")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty("foo")
      System.clearProperty("baz")
      ret should equalTo(None)
    }

    "return None correctly when the System property is not parseable as a Long with a prefix" in {
      System.setProperty("baz", "fizzbuzz64")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty("foo")
      System.clearProperty("baz")
      ret should equalTo(None)
    }

    "return None correctly when the System property is not parseable with trailing garbage" in {
      System.setProperty("baz", "64k bazbaz")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty("foo")
      System.clearProperty("baz")
      ret should equalTo(None)
    }

    "return None correctly when the System property is not parseable with leading garbage" in {
      System.setProperty("baz", "foofoo 64G")
      val ret = GeoMesaBatchWriterConfig.fetchMemoryProperty("foo")
      System.clearProperty("baz")
      ret should equalTo(None)
    }
  }
}

