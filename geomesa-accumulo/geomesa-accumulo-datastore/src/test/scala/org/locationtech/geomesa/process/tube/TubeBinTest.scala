/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.tube

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.GeometryCollection
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class TubeBinTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  import TubeBuilder.DefaultDtgField

  sequential

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
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf: SimpleFeature
      }

      logger.debug("features: "+features.size)
      val ngf = new NoGapFill(new DefaultFeatureCollection(sftName, sft), 1.0, 6)
      val binnedFeatures = ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features.asJava), DefaultDtgField).toSeq, 6)

      binnedFeatures.foreach { sf =>
        sf.getDefaultGeometry match {
          case collection: GeometryCollection => logger.debug("size: " + collection.getNumGeometries + " " + sf.getDefaultGeometry)
          case _ => logger.debug("size: 1")
        }
      }

      ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features.asJava), DefaultDtgField).toSeq, 1).size mustEqual 1

      ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features.asJava), DefaultDtgField).toSeq, 0).size mustEqual 19
    }

  }
}
