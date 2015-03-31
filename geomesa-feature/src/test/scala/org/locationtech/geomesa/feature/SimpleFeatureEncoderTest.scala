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
import org.locationtech.geomesa.utils.security.SecurityUtils
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

  def getFeaturesWithVisibility = {
    val features = getFeatures
    val visibilities = Seq("test&usa", "admin&user", "", null, "test", "user")

    features.zip(visibilities).map({
      case (sf, vis) =>
        SecurityUtils.setFeatureVisibility(sf, vis)
        sf
    })

    features
  }

  "SimpleFeatureEncoder" should {

    "have properly working apply() methods" >> {
      SimpleFeatureEncoder(sft, "avro") must beAnInstanceOf[AvroFeatureEncoder]
      SimpleFeatureEncoder(sft, FeatureEncoding.AVRO) must beAnInstanceOf[AvroFeatureEncoder]

      SimpleFeatureEncoder(sft, "text") must beAnInstanceOf[TextFeatureEncoder]
      SimpleFeatureEncoder(sft, FeatureEncoding.TEXT) must beAnInstanceOf[TextFeatureEncoder]

      SimpleFeatureEncoder(sft, "kryo") must beAnInstanceOf[KryoFeatureEncoder]
      SimpleFeatureEncoder(sft, FeatureEncoding.KRYO) must beAnInstanceOf[KryoFeatureEncoder]
    }
  }

  "SimpleFeatureDecoder" should {

    "have properly working apply() methods" >> {
      SimpleFeatureDecoder(sft, "avro") must beAnInstanceOf[AvroFeatureDecoder]
      SimpleFeatureDecoder(sft, FeatureEncoding.AVRO) must beAnInstanceOf[AvroFeatureDecoder]

      SimpleFeatureDecoder(sft, "text") must beAnInstanceOf[TextFeatureDecoder]
      SimpleFeatureDecoder(sft, FeatureEncoding.TEXT) must beAnInstanceOf[TextFeatureDecoder]

      SimpleFeatureDecoder(sft, FeatureEncoding.KRYO) must beAnInstanceOf[KryoFeatureEncoder]
      SimpleFeatureDecoder(sft, "kryo") must beAnInstanceOf[KryoFeatureEncoder]

      val projectedSft = SimpleFeatureTypes.createType(sftName, "*geom:Point")
      SimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.AVRO) must beAnInstanceOf[ProjectingAvroFeatureDecoder]
      SimpleFeatureDecoder(sft, projectedSft, "avro") must beAnInstanceOf[ProjectingAvroFeatureDecoder]

      SimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.TEXT) must beAnInstanceOf[ProjectingTextFeatureDecoder]
      SimpleFeatureDecoder(sft, projectedSft, "text") must beAnInstanceOf[ProjectingTextFeatureDecoder]

      SimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.KRYO) must beAnInstanceOf[KryoFeatureEncoder]
      SimpleFeatureDecoder(sft, projectedSft, "kryo") must beAnInstanceOf[KryoFeatureEncoder]
    }
  }

  "AvroFeatureEncoder" should {

    "return the correct encoding" >> {
      val encoder = new AvroFeatureEncoder(sft)
      encoder.encoding mustEqual FeatureEncoding.AVRO
    }

    "be able to encode points" >> {
      val encoder = new AvroFeatureEncoder(sft)
      val features = getFeatures

      val encoded = features.map(encoder.encode)
      encoded must not(beNull)
      encoded must have size features.size
    }

    "not include visibilities when not requested" >> {
      val encoder = new AvroFeatureEncoder(sft)
      val expected = getFeatures.map(encoder.encode)

      val featuresWithVis = getFeaturesWithVisibility
      val actual = featuresWithVis.map(encoder.encode)

      actual must haveSize(expected.size)

      forall(actual.zip(expected)) {
        case (a, e) => a mustEqual e
      }
    }

    "include visibilities when requested" >> {
      val noVis = {
        val encoder = new AvroFeatureEncoder(sft, Set.empty)
        getFeatures.map(encoder.encode)
      }
      val withVis = {
        val encoder = new AvroFeatureEncoder(sft, Set(EncodingOption.WITH_VISIBILITIES))
        getFeaturesWithVisibility.map(encoder.encode)
      }

      withVis must haveSize(noVis.size)

      forall(withVis.zip(noVis)) {
        case (y, n) => y.length must beGreaterThan(n.length)
      }
    }
  }

  "AvroFeatureDecoder" should {

    "return the correct encoding" >> {
      val decoder = new AvroFeatureDecoder(sft)
      decoder.encoding mustEqual FeatureEncoding.AVRO
    }

    "be able to decode points" >> {
      val encoder = new AvroFeatureEncoder(sft)
      val decoder = new AvroFeatureDecoder(sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(decoder.decode)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)
    }

    "be able to decode points with visbility" >> {
      val encoder = new AvroFeatureEncoder(sft, Set(EncodingOption.WITH_VISIBILITIES))
      val decoder = new AvroFeatureDecoder(sft, Set(EncodingOption.WITH_VISIBILITIES))

      val features = getFeaturesWithVisibility

      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(decoder.decode)

      // when decoding any empty visibilities will be ignored
      val emptyToNull = (s: String) => if (s == null || s.isEmpty) null else s

      decoded.map(SecurityUtils.getVisibility) mustEqual features.map(SecurityUtils.getVisibility _ andThen emptyToNull)
    }

    "be able to extract feature IDs" >> {
      val encoder = new AvroFeatureEncoder(sft)
      val decoder = new AvroFeatureDecoder(sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      encoded.map(decoder.extractFeatureId) mustEqual features.map(_.getID)
    }

    "fail when visbilities were encoded but are not expected by decoder" >> {
      val encoder = new AvroFeatureEncoder(sft, Set(EncodingOption.WITH_VISIBILITIES))
      val encoded = encoder.encode(getFeaturesWithVisibility.head)

      val decoder = new AvroFeatureDecoder(sft, EncodingOptions.none)

      decoder.decode(encoded) must throwA[Exception]
    }

    "fail when visibilities were not encoded but are expected by the decoder" >> {
      val encoder = new AvroFeatureEncoder(sft, EncodingOptions.none)
      val encoded = encoder.encode(getFeaturesWithVisibility.head)

      val decoder = new AvroFeatureDecoder(sft, Set(EncodingOption.WITH_VISIBILITIES))

      decoder.decode(encoded) must throwA[Exception]
    }
  }

  "ProjectingAvroFeatureDecoder" should {

    "properly project features" >> {
      val encoder = new AvroFeatureEncoder(sft)

      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")
      val projectingDecoder = new ProjectingAvroFeatureDecoder(sft, projectedSft)

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
  }

  "KryoFeatureEncoder" should {

    "return the correct encoding" >> {
      val encoder = new KryoFeatureEncoder(sft, sft)
      encoder.encoding mustEqual FeatureEncoding.KRYO
    }

    "be able to encode points" >> {
      val encoder = new KryoFeatureEncoder(sft, sft)
      val features = getFeatures

      val encoded = features.map(encoder.encode)
      encoded must not(beNull)
      encoded must have size features.size
    }

    "be able to decode points" >> {
      val encoder = new KryoFeatureEncoder(sft, sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(encoder.decode)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)
    }

    "be able to extract feature IDs" >> {
      val encoder = new KryoFeatureEncoder(sft, sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      encoded.map(encoder.extractFeatureId) mustEqual features.map(_.getID)
    }

    "properly project features when encoding" >> {
      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")

      val encoder = new KryoFeatureEncoder(sft, projectedSft)
      val decoder = new KryoFeatureEncoder(projectedSft, projectedSft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)
      val decoded = encoded.map(decoder.decode)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)

      forall(decoded) { sf =>
        sf.getAttributeCount mustEqual 1
        sf.getAttribute(0) must beAnInstanceOf[Point]
        sf.getFeatureType mustEqual projectedSft
      }
    }

    "properly project features when decoding" >> {
      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")

      val encoder = new KryoFeatureEncoder(sft, sft)
      val decoder = new KryoFeatureEncoder(sft, projectedSft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)
      val decoded = encoded.map(decoder.decode)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)

      forall(decoded) { sf =>
        sf.getAttributeCount mustEqual 1
        sf.getAttribute(0) must beAnInstanceOf[Point]
        sf.getFeatureType mustEqual projectedSft
      }
    }
  }


  "TextFeatureEncoder" should {

    "return the correct encoding" >> {
      val encoder = new TextFeatureEncoder(sft)
      encoder.encoding mustEqual FeatureEncoding.TEXT
    }

    "be able to encode points" >> {
      val encoder = new TextFeatureEncoder(sft)
      val features = getFeatures

      val encoded = features.map(encoder.encode)
      encoded must not(beNull)
      encoded must have size features.size
    }
  }

  "TextFeatureDecoder" should {

    "return the correct encoding" >> {
      val decoder = new TextFeatureDecoder(sft)
      decoder.encoding mustEqual FeatureEncoding.TEXT
    }

    "be able to decode points" >> {
      val encoder = new TextFeatureEncoder(sft)
      val decoder = new TextFeatureDecoder(sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(decoder.decode)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)
    }

    "be able to extract feature IDs" >> {
      val encoder = new TextFeatureEncoder(sft)
      val decoder = new TextFeatureDecoder(sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      encoded.map(decoder.extractFeatureId) mustEqual features.map(_.getID)
    }
  }

  "ProjectingTextDecoder" should {

    "properly project features" >> {
      val encoder = new TextFeatureEncoder(sft)

      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")
      val projectingDecoder = new ProjectingTextFeatureDecoder(sft, projectedSft)

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
  }
}
