/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.tools

import java.io.{StringWriter, ByteArrayOutputStream}
import java.util.Date

import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.tools.Utils.Formats
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._
import scala.xml.XML

@RunWith(classOf[JUnitRunner])
class FeatureExporterTest extends Specification {

  "DelimitedExport" >> {
    val sft = SimpleFeatureTypes.createType("DelimitedExportTest", "name:String,geom:Geometry:srid=4326,dtg:Date")
    val hints = new Hints(Hints.FEATURE_FACTORY, classOf[AvroSimpleFeatureFactory])
    val featureFactory = CommonFactoryFinder.getFeatureFactory(hints)

    // create a feature
    val builder = new SimpleFeatureBuilder(sft, featureFactory)
    builder.addAll(Array[AnyRef]("myname", null, new Date(0)))
    val liveFeature = builder.buildFeature("fid-1")
    val geom = WKTUtils.read("POINT(45.0 49.0)")
    liveFeature.setDefaultGeometry(geom)

    // make sure we ask the system to re-use the provided feature-ID
    liveFeature.getUserData().asScala(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

    val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)

    featureCollection.add(liveFeature)

    "should properly export to CSV" >> {
      val writer = new StringWriter()
      val export = new DelimitedExport(writer, Formats.CSV, Some("name,geom,dtg"), None, None, None, None)
      export.write(featureCollection)
      export.close()

      val result = writer.toString.split("\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "name,geom,dtg"
      data mustEqual "myname,POINT (45 49),1970-01-01 00:00:00"
    }
  }
}
