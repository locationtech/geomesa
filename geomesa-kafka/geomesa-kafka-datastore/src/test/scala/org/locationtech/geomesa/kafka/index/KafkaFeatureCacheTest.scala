/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.index.BucketIndexSupport
import org.locationtech.geomesa.memory.cqengine.GeoCQIndexSupport
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class KafkaFeatureCacheTest extends Specification {

  sequential

  val wholeWorldFilter = ECQL.toFilter("INTERSECTS(geom, POLYGON((-180 -90, -180 90, 0 90, 180 90, 180 -90, 0 -90, -180 -90)))")

  val sft = SimpleFeatureTypes.createType("track", "trackId:String,*geom:Point:srid=4326")

  val track0v0 = track("track0", "POINT (30 30)")
  val track0v1 = track("track0", "POINT (35 30)")

  val track1v0 = track("track1", "POINT (50 20)")
  val track1v1 = track("track1", "POINT (50 21)")

  val track2v0 = track("track2", "POINT (30 30)")

  val track3v0 = track("track3", "POINT (0 60)")

  def track(id: String, track: String): SimpleFeature = ScalaSimpleFeature.create(sft, id, id, track)

  def caches(expiry: Duration = Duration.Inf) =
    Iterator(
      KafkaFeatureCache(sft, BucketIndexSupport(sft, 360, 180), expiry, None),
      KafkaFeatureCache(sft, GeoCQIndexSupport(sft, 360, 180), expiry, None)
    )

  "KafkaFeatureCache" should {

    "put" >> {
      foreach(caches()) { cache =>
        try {
          cache.put(track0v0)
          cache.put(track1v0)
          cache.query(track0v0.getID) must beSome(track0v0)
          cache.query(track1v0.getID) must beSome(track1v0)
          val res = cache.query(wholeWorldFilter).toSeq
          res must containTheSameElementsAs(Seq(track0v0, track1v0))
          cache.size() mustEqual 2
        } finally {
          cache.close()
        }
      }
    }

    "update" >> {
      foreach(caches()) { cache =>
        try {
          cache.put(track0v0)
          cache.put(track1v0)
          cache.put(track0v1)
          cache.query(track0v1.getID) must beSome(track0v1)
          cache.query(track1v0.getID) must beSome(track1v0)
          val res = cache.query(wholeWorldFilter).toSeq
          res must containTheSameElementsAs(Seq(track0v1, track1v0))
          cache.size() mustEqual 2
        } finally {
          cache.close()
        }
      }
    }

    "remove" >> {
      foreach(caches()) { cache =>
        try {
          cache.put(track0v0)
          cache.put(track1v0)
          cache.put(track0v1)
          cache.remove(track0v1.getID)
          cache.query(track0v1.getID) must beNone
          cache.query(track1v0.getID) must beSome(track1v0)
          cache.query(wholeWorldFilter).toSeq mustEqual Seq(track1v0)
          cache.size() mustEqual 1
        } finally {
          cache.close()
        }
      }
    }

    "clear" >> {
      foreach(caches()) { cache =>
        try {
          cache.put(track0v0)
          cache.put(track1v0)
          cache.put(track0v1)
          cache.remove(track0v1.getID)
          cache.put(track2v0)
          cache.put(track1v1)
          cache.put(track3v0)
          cache.clear()
          cache.query(wholeWorldFilter).toSeq must beEmpty
          cache.size() mustEqual 0
        } finally {
          cache.close()
        }
      }
    }

    "expire" >> {
      foreach(caches(Duration("100ms"))) { cache =>
        try {
          cache.put(track0v0)

          cache.size() mustEqual 1
          cache.query(track0v0.getID) must beSome(track0v0)
          cache.query(wholeWorldFilter).toSeq mustEqual Seq(track0v0)

          cache.query(track0v0.getID) must eventually(40, 100.millis)(beNone)
          cache.query(wholeWorldFilter).toSeq must eventually(40, 100.millis)(beEmpty)
          cache.size() mustEqual 0
        } finally {
          cache.close()
        }
      }
    }

    "query on strings" >> {
      foreach(caches()) { cache =>
        try {
          cache.put(track0v0)
          cache.put(track1v0)
          cache.size() mustEqual 2
          cache.query(ECQL.toFilter("trackId > 'track0'")).toSeq mustEqual Seq(track1v0)
        } finally {
          cache.close()
        }
      }
    }

    "query on likes" >> {
      foreach(caches()) { cache =>
        try {
          cache.put(track0v0)
          cache.put(track1v0)
          cache.size() mustEqual 2
          cache.query(ECQL.toFilter("trackId ILIKE 'T%'")).toSeq must containTheSameElementsAs(Seq(track0v0, track1v0))
        } finally {
          cache.close()
        }
      }
    }
  }
}

