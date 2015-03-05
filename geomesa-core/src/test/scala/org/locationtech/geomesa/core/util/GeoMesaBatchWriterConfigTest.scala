package org.locationtech.geomesa.core.util

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
      bwc.getMaxMemory                         must be equalTo GeoMesaBatchWriterConfig.DEFAULTMAXMEMORY
      bwc.getMaxLatency(TimeUnit.MILLISECONDS) must be equalTo GeoMesaBatchWriterConfig.DEFAULTLATENCY
      bwc.getMaxWriteThreads                   must be equalTo GeoMesaBatchWriterConfig.DEFAULTTHREADS
    }
  }

  "GeoMesaBatchWriterConfig" should {
    "respect system properties" in {
      val latencyProp = "1234"
      val memoryProp  = "2345"
      val threadsProp = "25"
      val timeoutProp = "33"

      System.setProperty(GeoMesaBatchWriterConfig.WRITERLATENCYMILLIS, latencyProp)
      System.setProperty(GeoMesaBatchWriterConfig.WRITERMEMORY,        memoryProp)
      System.setProperty(GeoMesaBatchWriterConfig.WRITERTHREADS,       threadsProp)
      System.setProperty(GeoMesaBatchWriterConfig.WRITETIMEOUT,        timeoutProp)

      val nbwc = GeoMesaBatchWriterConfig.buildBWC

      System.clearProperty(GeoMesaBatchWriterConfig.WRITERLATENCYMILLIS)
      System.clearProperty(GeoMesaBatchWriterConfig.WRITERMEMORY)
      System.clearProperty(GeoMesaBatchWriterConfig.WRITERTHREADS)
      System.clearProperty(GeoMesaBatchWriterConfig.WRITETIMEOUT)

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
}
