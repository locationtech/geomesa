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

package org.locationtech.geomesa.feature

import com.vividsolutions.jts.geom.Point
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SimpleFeatureEncoderTest extends Specification {

  sequential

  val sftName = "SimpleFeatureEncoderTest"
  val sft = SimpleFeatureTypes.createType(sftName, "name:String,*geom:Point,dtg:Date")

  val builder = AvroSimpleFeatureFactory.featureBuilder(sft)

  def getFeatures = (0 until 6).map { i =>
    builder.reset()
    builder.set("geom", WKTUtils.read("POINT(-110 30)"))
    builder.set("dtg", "2012-01-02T05:06:07.000Z")
    builder.set("name",i.toString)
    val sf = builder.buildFeature(i.toString)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    sf
  }

  "SimpleFeatureEncoder" should {
    "encode and decode points" in {
      val encoder = new AvroFeatureEncoder(sft)
      val decoder = new AvroFeatureDecoder(sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)
      val decoded = encoded.map { bytes => decoder.decode(bytes) }
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)
    }

    "encode and decode points in kryo" in {
      val encoder = new KryoFeatureEncoder(sft, sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)
      val decoded = encoded.map { bytes => encoder.decode(bytes) }
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)
    }

    "have properly working apply() methods" in {
      SimpleFeatureEncoder(sft, "avro") must beAnInstanceOf[AvroFeatureEncoder]
      SimpleFeatureEncoder(sft, FeatureEncoding.AVRO) must beAnInstanceOf[AvroFeatureEncoder]

      SimpleFeatureEncoder(sft, "text") must beAnInstanceOf[TextFeatureEncoder]
      SimpleFeatureEncoder(sft, FeatureEncoding.TEXT) must beAnInstanceOf[TextFeatureEncoder]

      SimpleFeatureEncoder(sft, "kryo") must beAnInstanceOf[KryoFeatureEncoder]
      SimpleFeatureEncoder(sft, FeatureEncoding.KRYO) must beAnInstanceOf[KryoFeatureEncoder]
    }
  }

  "SimpleFeatureDecoder" should {
    "have working apply() methods" in {
      SimpleFeatureDecoder(sft, "avro") must beAnInstanceOf[AvroFeatureDecoder]
      SimpleFeatureDecoder(sft, FeatureEncoding.AVRO) must beAnInstanceOf[AvroFeatureDecoder]

      SimpleFeatureDecoder(sft, "text") must beAnInstanceOf[TextFeatureDecoder]
      SimpleFeatureDecoder(sft, FeatureEncoding.TEXT) must beAnInstanceOf[TextFeatureDecoder]

      SimpleFeatureDecoder(sft, FeatureEncoding.KRYO) must beAnInstanceOf[KryoFeatureEncoder]
      SimpleFeatureDecoder(sft, "kryo") must beAnInstanceOf[KryoFeatureEncoder]

      val projectedSft = SimpleFeatureTypes.createType(sftName, "*geom:Point")
      SimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.AVRO) must beAnInstanceOf[ProjectingAvroFeatureDecoder]
      SimpleFeatureDecoder(sft, projectedSft, "avro") must beAnInstanceOf[ProjectingAvroFeatureDecoder]

      SimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.TEXT) must beAnInstanceOf[ProjectingTextDecoder]
      SimpleFeatureDecoder(sft, projectedSft, "text") must beAnInstanceOf[ProjectingTextDecoder]

      SimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.KRYO) must beAnInstanceOf[KryoFeatureEncoder]
      SimpleFeatureDecoder(sft, projectedSft, "kryo") must beAnInstanceOf[KryoFeatureEncoder]
    }

    "properly project features" in {
      val encoder = SimpleFeatureEncoder(sft, "avro")

      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")
      val projectingDecoder = SimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.AVRO)

      val features = getFeatures
      val encoded = features.map(encoder.encode)
      val decoded = encoded.map(projectingDecoder.decode)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)

      forall(decoded) { sf =>
        sf.getAttributeCount mustEqual 1
        sf.getAttribute(0) must beAnInstanceOf[Point]
        sf.getFeatureType mustEqual projectedSft
      }
    }

    "properly project features in kryo" in {
      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")

      val projectingEncoder = SimpleFeatureDecoder(sft, projectedSft, "kryo").asInstanceOf[KryoFeatureEncoder]
      val decoder = SimpleFeatureDecoder(projectedSft, "kryo").asInstanceOf[KryoFeatureEncoder]

      val features = getFeatures
      val encoded = features.map(projectingEncoder.encode)
      val decoded = encoded.map(decoder.decode)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)

      forall(decoded) { sf =>
        sf.getAttributeCount mustEqual 1
        sf.getAttribute(0) must beAnInstanceOf[Point]
        sf.getFeatureType mustEqual projectedSft
      }
    }
  }

}
