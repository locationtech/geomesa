/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.process.tube

import com.vividsolutions.jts.geom.GeometryCollection
import org.apache.log4j.Logger
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo._
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

  val geotimeAttributes = org.locationtech.geomesa.accumulo.index.spec

  "NoGapFilll" should {

    "correctly time bin features" in {
      val sftName = "tubetest2"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")

      val features = for(day <- 1 until 20) yield {
        val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), day.toString)
        val lat = 40+day
        sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
        sf.setAttribute(DEFAULT_DTG_PROPERTY_NAME, new DateTime(f"2011-01-$day%02dT00:00:00Z", DateTimeZone.UTC).toDate)
        sf.setAttribute("type","test")
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        sf
      }

      log.debug("features: "+features.size)
      val ngf = new NoGapFill(new DefaultFeatureCollection(sftName, sft), 1.0, 6)
      val binnedFeatures = ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features),DEFAULT_DTG_PROPERTY_NAME).toSeq, 6)

      binnedFeatures.foreach { sf =>
        if (sf.getDefaultGeometry.isInstanceOf[GeometryCollection])
          log.debug("size: " + sf.getDefaultGeometry.asInstanceOf[GeometryCollection].getNumGeometries +" "+ sf.getDefaultGeometry)
        else log.debug("size: 1")
      }

      ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features), DEFAULT_DTG_PROPERTY_NAME).toSeq, 1).size mustEqual 1

      ngf.timeBinAndUnion(ngf.transform(new ListFeatureCollection(sft, features), DEFAULT_DTG_PROPERTY_NAME).toSeq, 0).size mustEqual 1
    }

  }
}
