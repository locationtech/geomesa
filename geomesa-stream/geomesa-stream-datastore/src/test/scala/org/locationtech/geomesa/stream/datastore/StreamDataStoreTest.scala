/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.stream.datastore

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong

import com.google.common.io.Resources
import org.apache.commons.io.IOUtils
import org.apache.commons.net.DefaultSocketFactory
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class StreamDataStoreTest extends Specification {

  import org.locationtech.geomesa.filter.ff

  import scala.concurrent.ExecutionContext.Implicits.global

  sequential

  val count  = new AtomicLong(0)
  val count2 = new AtomicLong(0)
  val count3 = new AtomicLong(0)

  val sourceConf =
    """
      |{
      |  type         = "generic"
      |  source-route = "netty4:tcp://localhost:5899?textline=true"
      |  sft          = {
      |                   type-name = "testdata"
      |                   fields = [
      |                     { name = "label",     type = "String" }
      |                     { name = "geom",      type = "Point",  index = true, srid = 4326, default = true }
      |                     { name = "dtg",       type = "Date",   index = true }
      |                   ]
      |                 }
      |  converter    = {
      |                   id-field = "md5(string2bytes($0))"
      |                   type = "delimited-text"
      |                   format = "DEFAULT"
      |                   fields = [
      |                     { name = "label",     transform = "trim($1)" }
      |                     { name = "geom",      transform = "point($2::double, $3::double)" }
      |                     { name = "dtg",       transform = "datetime($4)" }
      |                   ]
      |                 }
      |}
    """.stripMargin

  val sds = DataStoreFinder.getDataStore(Map(
      StreamDataStoreParams.StreamDatastoreConfig.key -> sourceConf,
      StreamDataStoreParams.CacheTimeout.key -> Integer.valueOf(2)
    )).asInstanceOf[StreamDataStore]

  "StreamDataStore" should {
    "be built from a conf string" >> {
      sds must not beNull
    }

    "read and write" >> {
      val listener = new StreamListener {
        override def onNext(sf: SimpleFeature): Unit = {
          count.incrementAndGet()
        }
      }
      sds.registerListener(listener)

      val listener2 = StreamListener { sf => count2.incrementAndGet() }
      sds.registerListener(listener2)

      val bboxFilter = ff.bbox("geom", 49.0, 79.0, 51.0, 80.0, "EPSG:4326")
      val listener3 = StreamListener(bboxFilter, _ =>  count3.incrementAndGet())
      sds.registerListener(listener3)

      val fs = sds.getFeatureSource("testdata")

      "handle new data" >> {
        val url = Resources.getResource("testdata.tsv")
        val lines = Resources.readLines(url, StandardCharsets.UTF_8)
        val socketFactory = new DefaultSocketFactory
        Future {
          val socket = socketFactory.createSocket("localhost", 5899)
          val os = socket.getOutputStream
          IOUtils.writeLines(lines, IOUtils.LINE_SEPARATOR_UNIX, os)
          os.flush()
          // wait for data to arrive at the server
          Thread.sleep(4000)
          os.close()
        }

        Thread.sleep(1000)
        fs.getFeatures(Filter.INCLUDE).features().hasNext must beTrue
      }

      "support listeners" >> {
        count.get() must equalTo(7)
        count2.get() must equalTo(7)
        count3.get() must equalTo(3)
      }

      "handle bbox filters" >> {
        fs.getFeatures(bboxFilter).size() must be equalTo 3
      }

      "expire data after the appropriate amount of time" >> {
        Thread.sleep(3000)
        fs.getFeatures(Filter.INCLUDE).features().hasNext must beFalse
      }
      ok
    }
  }
}