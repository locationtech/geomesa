/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.tube

import org.locationtech.jts.geom.GeometryCollection
import org.apache.log4j.Logger
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._


@RunWith(classOf[JUnitRunner])
class TubeBinTest extends Specification {

  import TubeBuilder.DefaultDtgField

  sequential

  private val log = Logger.getLogger(classOf[TubeBinTest])

  val geotimeAttributes = s"geom:Point:srid=4326,$DefaultDtgField:Date,dtg_end_time:Date"

  "NoGapFill" should {

    "correctly time bin features" in {
      val sftName = "tubetest2"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")

      val features = for(day <- 1 until 20) yield {
        val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), day.toString)
        val lat = 40+day
        sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
        sf.setAttribute(DefaultDtgField, f"2011-01-$day%02dT00:00:00Z")
        sf.setAttribute("type","test")
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        sf
      }

      log.debug("features: "+features.size)
      val ngf = new NoGapFill(new DefaultFeatureCollection(sftName, sft), 1.0, 6)
      val binnedFeatures = ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features), DefaultDtgField).toSeq, 6)

      binnedFeatures.foreach { sf =>
        sf.getDefaultGeometry match {
          case collection: GeometryCollection => log.debug("size: " + collection.getNumGeometries + " " + sf.getDefaultGeometry)
          case _ => log.debug("size: 1")
        }
      }

      ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features), DefaultDtgField).toSeq, 1).size mustEqual 1

      ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features), DefaultDtgField).toSeq, 0).size mustEqual 19
    }

  }
}
