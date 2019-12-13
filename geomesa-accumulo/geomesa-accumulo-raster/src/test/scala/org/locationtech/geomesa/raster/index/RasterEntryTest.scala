/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.index

import java.util.Date

import org.apache.accumulo.core.data.Key
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RasterEntryTest extends Specification {

  sequential

  val now = new Date()
  val emptyByte = Array.empty[Byte]

  def makeKey(cq: Array[Byte]): Key = new Key(emptyByte, emptyByte, cq, emptyByte, Long.MaxValue)

  "RasterEntry" should {

    "encode and decode Raster meta-data properly" in {
      val wkt = "POLYGON ((10 0, 10 10, 0 10, 0 0, 10 0))"
      val id = "Feature0123456789"
      val geom = WKTUtils.read(wkt)
      val date = now

      // output encoded meta data
      val cqMetaData = RasterEntry.encodeIndexCQMetadata(id, geom, Some(date))

      // convert CQ Array[Byte] to Key (a key with everything as a null except CQ)
      val keyWithCq = makeKey(cqMetaData)

      // decode metadata from key
      val decoded = RasterEntry.decodeIndexCQMetadata(keyWithCq)

      // requirements
      decoded must not equalTo null
      decoded.id must be equalTo id
      WKTUtils.write(decoded.geom) must be equalTo wkt
      decoded.date.get must be equalTo now
    }

    "encode and decode Raster meta-data properly when there is no datetime" in {
      val wkt = "POLYGON ((10 0, 10 10, 0 10, 0 0, 10 0))"
      val id = "Feature0123456789"
      val geom = WKTUtils.read(wkt)
      val dt: Option[Date] = None

      // output encoded meta data
      val cqMetaData = RasterEntry.encodeIndexCQMetadata(id, geom, dt)

      // convert CQ Array[Byte] to Key (a key with everything as a null except CQ)
      val keyWithCq = makeKey(cqMetaData)

      // decode metadata from key
      val decoded = RasterEntry.decodeIndexCQMetadata(keyWithCq)

      // requirements
      decoded must not equalTo null
      decoded.id must be equalTo id
      WKTUtils.write(decoded.geom) must be equalTo wkt
      dt.isDefined must beFalse
    }

  }

}
