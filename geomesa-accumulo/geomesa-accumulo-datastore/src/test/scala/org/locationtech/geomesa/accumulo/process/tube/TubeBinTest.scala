/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.process.tube

import com.vividsolutions.jts.geom.GeometryCollection
import org.apache.log4j.Logger
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._


@RunWith(classOf[JUnitRunner])
class TubeBinTest extends Specification {

  sequential

  private val log = Logger.getLogger(classOf[TubeBinTest])

  val geotimeAttributes = "geom:Point:srid=4326,dtg:Date,dtg_end_time:Date"

  "NoGapFilll" should {

    "correctly time bin features" in {
      val sftName = "tubetest2"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")

      val features = for(day <- 1 until 20) yield {
        val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), day.toString)
        val lat = 40+day
        sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
        sf.setAttribute("dtg", new DateTime(f"2011-01-$day%02dT00:00:00Z", DateTimeZone.UTC).toDate)
        sf.setAttribute("type","test")
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        sf
      }

      log.debug("features: "+features.size)
      val ngf = new NoGapFill(new DefaultFeatureCollection(sftName, sft), 1.0, 6)
      val binnedFeatures = ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features), "dtg").toSeq, 6)

      binnedFeatures.foreach { sf =>
        if (sf.getDefaultGeometry.isInstanceOf[GeometryCollection])
          log.debug("size: " + sf.getDefaultGeometry.asInstanceOf[GeometryCollection].getNumGeometries +" "+ sf.getDefaultGeometry)
        else log.debug("size: 1")
      }

      ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features), "dtg").toSeq, 1).size mustEqual 1

      ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features), "dtg").toSeq, 0).size mustEqual 1
    }

  }
}
