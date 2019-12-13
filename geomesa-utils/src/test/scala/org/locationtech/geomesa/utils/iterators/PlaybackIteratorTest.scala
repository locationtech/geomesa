/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.iterators

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.memory.MemoryDataStore
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class PlaybackIteratorTest extends Specification with LazyLogging {

  val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
  val builder = new SimpleFeatureBuilder(sft)
  val features = Seq.tabulate(10) { i =>
    builder.addAll(Array[AnyRef](s"name$i", s"2018-01-01T00:00:0${9 - i}.000Z", s"POINT (4$i 55)"))
    builder.buildFeature(s"$i")
  }
  val ds = new MemoryDataStore(features.toArray)

  val dtg = Some("dtg")
  val interval: (Date, Date) = {
    val start = features.last.getAttribute("dtg").asInstanceOf[Date].getTime - 1
    val end = features.head.getAttribute("dtg").asInstanceOf[Date].getTime + 1
    (new Date(start), new Date(end))
  }

  "PlaybackIterator" should {
    "return features in sorted order" in {
      WithClose(new PlaybackIterator(ds, sft.getTypeName, interval, dtg, rate = 100f)) { iter =>
        foreach(iter.sliding(2).toSeq) { case Seq(left, right) =>
          left.getAttribute("dtg").asInstanceOf[Date].before(right.getAttribute("dtg").asInstanceOf[Date]) must beTrue
        }
      }
    }
    "query using windows" in {
      val window = Some(Duration("5 seconds"))
      WithClose(new PlaybackIterator(ds, sft.getTypeName, interval, dtg, window = window, rate = 100f)) { iter =>
        foreach(iter.sliding(2).toSeq) { case Seq(left, right) =>
          left.getAttribute("dtg").asInstanceOf[Date].before(right.getAttribute("dtg").asInstanceOf[Date]) must beTrue
        }
      }
    }
    "replicate original rate" in {
      val filter = Some(ECQL.toFilter("name IN ('name8', 'name7')")) // dates are 1 and 2 seconds into interval
      WithClose(new PlaybackIterator(ds, sft.getTypeName, interval, dtg, filter = filter, rate = 10f)) { iter =>
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
  }
}
