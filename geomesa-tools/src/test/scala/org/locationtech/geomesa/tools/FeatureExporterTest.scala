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

import java.io.StringWriter
import java.util.Date

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.{PasswordToken, AuthenticationToken}
import org.geotools.data.{Query, DataStoreFinder}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureStore
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.tools.Utils.Formats
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FeatureExporterTest extends Specification {

  sequential

  "DelimitedExport" >> {
    val sftName = "DelimitedExportTest"
    val sft = SimpleFeatureTypes.createType(sftName, "name:String,geom:Geometry:srid=4326,dtg:Date")

    val attributes = Array("myname", "POINT(45.0 49.0)", new Date(0))
    val feature = ScalaSimpleFeatureFactory.buildFeature(sft, attributes, "fid-1")

    val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)
    featureCollection.add(feature)

    val connector = new MockInstance().getConnector("", new PasswordToken(""))

    val ds = DataStoreFinder
        .getDataStore(Map("connector" -> connector, "tableName" -> sftName, "caching"   -> false))
    ds.createSchema(sft)
    ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore].addFeatures(featureCollection)

    "should properly export to CSV" >> {
      val writer = new StringWriter()
      val export = new DelimitedExport(writer, Formats.CSV, Some("name,geom,dtg"))
      export.write(featureCollection)
      export.close()

      val result = writer.toString.split("\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "name,geom,dtg"
      data mustEqual "myname,POINT (45 49),1970-01-01 00:00:00"
    }

    "should handle transforms" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("derived=strConcat(name, '-test')", "geom", "dtg"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = new DelimitedExport(writer, Formats.CSV, Some("derived=strConcat(name\\, '-test'),geom,dtg"))
      export.write(features)
      export.close()

      val result = writer.toString.split("\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "derived,geom,dtg"
      data mustEqual "myname-test,POINT (45 49),1970-01-01 00:00:00"
    }

    "should handle escapes" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("derived=strConcat(name, ',test')", "geom", "dtg"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = new DelimitedExport(writer, Formats.CSV, Some("derived=strConcat(name\\, '\\,test'),geom,dtg"))
      export.write(features)
      export.close()

      val result = writer.toString.split("\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "derived,geom,dtg"
      data mustEqual "\"myname,test\",POINT (45 49),1970-01-01 00:00:00"
    }
  }
}
