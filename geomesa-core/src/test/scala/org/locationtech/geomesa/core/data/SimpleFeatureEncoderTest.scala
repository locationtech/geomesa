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

package org.locationtech.geomesa.core.data

import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Value, Mutation}
import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.codec.binary.Hex
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.{DataStoreFinder, DataUtilities, Query}
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.index.SF_PROPERTY_START_TIME
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._


/**
* The purpose of this test is to ensure that the table version is backwards compatible with
* older versions (e.g. 0.10.x). The table format should not be changed without some sort of
* transition map/reduce job to convert table formats.
*/
@RunWith(classOf[JUnitRunner])
class SimpleFeatureEncoderTest extends Specification {

  val sftName = "SimpleFeatureEncoderTest"
  val sft = SimpleFeatureTypes.createType(sftName, "name:String,*geom:Point,dtg:Date")
  sft.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

  val builder = AvroSimpleFeatureFactory.featureBuilder(sft)

  def getFeatures = (0 until 6).map { i =>
    builder.reset
    builder.set("geom", WKTUtils.read("POINT(-110 30)"))
    builder.set("dtg", "2012-01-02T05:06:07.000Z")
    builder.set("name",i.toString)
    val sf = builder.buildFeature(i.toString)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    sf
  }

  val encoder = new AvroFeatureEncoder

  "SimpleFeatureEncoder" should {
    "encode and decode points" in {
      val features = getFeatures
      val encoded = features.map(encoder.encode(_))
      val decoded = encoded.map(bytes => encoder.decode(sft, new Value(bytes)))
      decoded.map(_.getDefaultGeometry) mustEqual(features.map(_.getDefaultGeometry))
    }
  }

}
