/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.data.memory.MemoryDataStore
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.locationtech.geomesa.utils.collection.CloseableIterator

import java.util.Date
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class SimplePlaybackIteratorTest extends Specification with LazyLogging {

  val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
  val builder = new SimpleFeatureBuilder(sft)
  val features = Seq.tabulate(10) { i =>
    builder.addAll(s"name$i", s"2018-01-01T00:00:0${9 - i}.000Z", s"POINT (4$i 55)")
    builder.buildFeature(s"$i")
  }

  val dtg = Some("dtg")
  val interval: (Date, Date) = {
    val start = features.last.getAttribute("dtg").asInstanceOf[Date].getTime - 1
    val end = features.head.getAttribute("dtg").asInstanceOf[Date].getTime + 1
    (new Date(start), new Date(end))
  }

  "PlaybackIterator" should {
    "replicate original rate" in {
      val filteredFeatures = features.toStream
        .filter(sf => sf.getAttribute("name").equals("name7") || sf.getAttribute("name").equals("name8"))
        .sortWith(_.getAttribute("dtg").asInstanceOf[Date].getTime < _.getAttribute("dtg").asInstanceOf[Date].getTime)
      WithClose(new SimplePlaybackIterator(CloseableIterator(filteredFeatures.iterator), sft, dtg, rate = 10f)) { iter =>
        // don't time the first result, as it will be inconsistent due to setup and querying
        iter.hasNext must beTrue
        iter.next()
        val start = System.currentTimeMillis()
        iter.hasNext must beTrue
        // should block until second feature time has elapsed, 100 millis from first feature (due to 10x rate)
        iter.next()
        val elapsed = System.currentTimeMillis() - start
        // due to system load, this test tends to fail in travis, so we don't cause an explicit test failure
        if (elapsed > 130L) {
          logger.warn(s"PlaybackIteratorTest - playback result was delayed longer than expected 100ms: ${elapsed}ms")
        }

        iter.hasNext must beFalse
      }
    }
    "project to current time" in {
      // dates are 1 and 2 seconds into interval
      val filteredFeatures = features.toStream
        .filter(sf => sf.getAttribute("name").equals("name7") || sf.getAttribute("name").equals("name8"))
        .sortWith(_.getAttribute("dtg").asInstanceOf[Date].getTime < _.getAttribute("dtg").asInstanceOf[Date].getTime)

      WithClose(new SimplePlaybackIterator(CloseableIterator(filteredFeatures.iterator), sft, dtg, rate = 10f, live = true)) { iter =>
        // don't time the first result, as it will be inconsistent due to setup and querying
        iter.hasNext must beTrue
        val dtg1 = iter.next().getAttribute("dtg").asInstanceOf[Date]
        val start = System.currentTimeMillis()
        iter.hasNext must beTrue
        // should block until second feature time has elapsed, 100 millis from first feature (due to 10x rate)
        val dtg2 = iter.next().getAttribute("dtg").asInstanceOf[Date]
        val elapsed = System.currentTimeMillis() - start
        // due to system load, this test tends to fail in travis, so we don't cause an explicit test failure
        if (elapsed > 130L) {
          logger.warn(s"PlaybackIteratorTest - playback result was delayed longer than expected 100ms: ${elapsed}ms")
        }
        dtg1.getTime must beCloseTo(start, 30)
        dtg2.getTime must beCloseTo(start + 100, 30) // + 100 due to 10x rate

        iter.hasNext must beFalse
      }
    }
  }
}
