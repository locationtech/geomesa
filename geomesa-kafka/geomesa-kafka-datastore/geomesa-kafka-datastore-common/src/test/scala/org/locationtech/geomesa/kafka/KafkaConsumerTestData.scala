/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.joda.time.Instant
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object KafkaConsumerTestData {

  val typeName = "track"
  val sft = KafkaDataStoreHelper.createStreamingSFT(
    SimpleFeatureTypes.createType(typeName, "trackId:String,*geom:LineString:srid=4326"), "/test")

  val track0v0 = track("track0", "LineString (30 30, 30 30)")
  val track0v1 = track("track0", "LineString (30 30, 35 30)")
  val track0v2 = track("track0", "LineString (30 30, 35 30, 40 34)")
  val track0v3 = track("track0", "LineString (30 30, 35 32, 40 34, 45 36)")

  // Changed due to instability in between WKT Utils and Avro Geometry deserialization
  val track1v0 = track("track1", "LineString (50 20, 50 21)")
  val track1v1 = track("track1", "LineString (50 20, 40 30)")
  val track1v2 = track("track1", "LineString (50 20, 40 30, 30 30)")

  val track2v0 = track("track2", "LineString (30 30, 30 30)")
  val track2v1 = track("track2", "LineString (30 30, 30 25)")
  val track2v2 = track("track2", "LineString (30 30, 30 25, 28 20)")
  val track2v3 = track("track2", "LineString (30 30, 30 25, 25 20, 20 15)")

  val track3v0 = track("track3", "LineString (0 60,  0 60)")
  val track3v1 = track("track3", "LineString (0 60, 10 60)")
  val track3v2 = track("track3", "LineString (0 60, 10 60, 20 55)")
  val track3v3 = track("track3", "LineString (0 60, 10 60, 20 55, 30 40)")
  val track3v4 = track("track3", "LineString (0 60, 10 60, 20 55, 30 40, 30 30)")

  val messages: Seq[GeoMessage] = Seq(

    // offset
    CreateOrUpdate(new Instant(10993), track0v0),   //  0
    CreateOrUpdate(new Instant(11001), track3v0),   //  1

    CreateOrUpdate(new Instant(11549), track3v1),   //  2

    CreateOrUpdate(new Instant(11994), track0v1),   //  3
    CreateOrUpdate(new Instant(11995), track1v0),   //  4
    CreateOrUpdate(new Instant(11995), track3v2),   //  5

    CreateOrUpdate(new Instant(12998), track1v1),   //  6
    CreateOrUpdate(new Instant(13000), track2v0),   //  7
    CreateOrUpdate(new Instant(13002), track3v3),   //  8
    CreateOrUpdate(new Instant(13002), track0v2),   //  9

    CreateOrUpdate(new Instant(13444), track1v2),   // 10

    CreateOrUpdate(new Instant(13996), track2v1),   // 11
    CreateOrUpdate(new Instant(13999), track3v4),   // 12
    CreateOrUpdate(new Instant(14002), track0v3),   // 13
    Delete(        new Instant(14005), "track1"),   // 14

    Delete(        new Instant(14990), "track3"),   // 15
    CreateOrUpdate(new Instant(14999), track2v2),   // 16
    Delete(        new Instant(15000), "track0"),   // 17

    Clear(         new Instant(16003)),             // 18

    CreateOrUpdate(new Instant(16997), track2v3),   // 19

    Delete(        new Instant(17000), "track3")    // 20

  )


  def track(id: String, track: String): SimpleFeature = {
    val geom = WKTUtils.read(track)
    new SimpleFeatureImpl(List[Object](id, geom), sft, new FeatureIdImpl(id))
  }

}
