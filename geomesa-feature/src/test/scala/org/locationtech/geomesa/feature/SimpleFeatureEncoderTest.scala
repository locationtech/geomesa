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
import org.locationtech.geomesa.feature.EncodingOption.EncodingOptions
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.Matcher
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
        sf.visibility = vis
        sf
    })

    features
  }

  "SimpleFeatureEncoder" should {

    "have a properly working apply() method" >> {
      val opts = EncodingOptions.withUserData

      // AVRO without options
      val avro1 = SimpleFeatureEncoder(sft, FeatureEncoding.AVRO)
      avro1 must beAnInstanceOf[AvroFeatureEncoder]
      avro1.options mustEqual EncodingOptions.none

      // AVRO with options
      val avro2 = SimpleFeatureEncoder(sft, FeatureEncoding.AVRO, opts)
      avro2 must beAnInstanceOf[AvroFeatureEncoder]
      avro2.options mustEqual opts

      // KRYO without options
      val kryo1 = SimpleFeatureEncoder(sft, FeatureEncoding.KRYO)
      kryo1 must beAnInstanceOf[KryoFeatureEncoder]
      kryo1.options mustEqual EncodingOptions.none

      // KRYO with options
      val kryo2 = SimpleFeatureEncoder(sft, FeatureEncoding.KRYO, opts)
      kryo2 must beAnInstanceOf[KryoFeatureEncoder]
      kryo2.options mustEqual opts
    }
  }

  "SimpleFeatureDecoder" should {

    "have a properly working apply() method" >> {
      val opts = EncodingOptions.withUserData

      // AVRO without options
      val avro1 = SimpleFeatureDecoder(sft, FeatureEncoding.AVRO)
      avro1 must beAnInstanceOf[AvroFeatureDecoder]
      avro1.options mustEqual EncodingOptions.none

      // AVRO with options
      val avro2 = SimpleFeatureDecoder(sft, FeatureEncoding.AVRO, opts)
      avro2 must beAnInstanceOf[AvroFeatureDecoder]
      avro2.options mustEqual opts

      // KRYO without options
      val kryo1 = SimpleFeatureDecoder(sft, FeatureEncoding.KRYO)
      kryo1 must beAnInstanceOf[KryoFeatureEncoder]
      kryo1.options mustEqual EncodingOptions.none

      // KRYO with options
      val kryo2 = SimpleFeatureDecoder(sft, FeatureEncoding.KRYO, opts)
      kryo2 must beAnInstanceOf[KryoFeatureEncoder]
      kryo2.options mustEqual opts
    }
  }

  "ProjectingSimpleFeatureDecoder" should {

    "have a properly working apply() method" >> {
      val projectedSft = SimpleFeatureTypes.createType(sftName, "*geom:Point")
      val opts = EncodingOptions.withUserData

      // AVRO without options
      val avro1 = ProjectingSimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.AVRO)
      avro1 must beAnInstanceOf[ProjectingAvroFeatureDecoder]
      avro1.options mustEqual EncodingOptions.none

      // AVRO with options
      val avro2 = ProjectingSimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.AVRO, opts)
      avro2 must beAnInstanceOf[ProjectingAvroFeatureDecoder]
      avro2.options mustEqual opts

      // KRYO without options
      val kryo1 = ProjectingSimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.KRYO)
      kryo1 must beAnInstanceOf[KryoFeatureEncoder]
      kryo1.options mustEqual EncodingOptions.none

      // KRYO with options
      val kryo2 = ProjectingSimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.KRYO, opts)
      kryo2 must beAnInstanceOf[KryoFeatureEncoder]
      kryo2.options mustEqual opts
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

    "not include user data when not requested" >> {
      val encoder = new AvroFeatureEncoder(sft)
      val expected = getFeatures.map(encoder.encode)

      val featuresWithVis = getFeaturesWithVisibility
      val actual = featuresWithVis.map(encoder.encode)

      actual must haveSize(expected.size)

      forall(actual.zip(expected)) {
        case (a, e) => a mustEqual e
      }
    }

    "include user data when requested" >> {
      val noUserData = {
        val encoder = new AvroFeatureEncoder(sft, EncodingOptions.none)
        getFeatures.map(encoder.encode)
      }
      val withUserData = {
        val encoder = new AvroFeatureEncoder(sft, EncodingOptions.withUserData)
        getFeaturesWithVisibility.map(encoder.encode)
      }

      withUserData must haveSize(noUserData.size)

      forall(withUserData.zip(noUserData)) {
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
      decoded must equalFeatures(features, withoutUserData)
    }

    "be able to decode points with user data" >> {
      val encoder = new AvroFeatureEncoder(sft, EncodingOptions.withUserData)
      val decoder = new AvroFeatureDecoder(sft, EncodingOptions.withUserData)

      val features = getFeaturesWithVisibility
      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(decoder.decode)

      decoded must equalFeatures(features)
    }

    "be able to extract feature IDs" >> {
      val encoder = new AvroFeatureEncoder(sft)
      val decoder = new AvroFeatureDecoder(sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      encoded.map(decoder.extractFeatureId) mustEqual features.map(_.getID)
    }

    "work when user data were encoded but are not expected by decoder" >> {
      // in this case the encoded user data will be ignored
      val sf = getFeaturesWithVisibility.head
      val encoder = new AvroFeatureEncoder(sft, EncodingOptions.withUserData)
      val encoded = encoder.encode(sf)

      val decoder = new AvroFeatureDecoder(sft, EncodingOptions.none)

      decoder.decode(encoded) must equalSF(sf, withoutUserData)
    }

    "fail when user data were not encoded but are expected by the decoder" >> {
      val encoder = new AvroFeatureEncoder(sft, EncodingOptions.none)
      val encoded = encoder.encode(getFeaturesWithVisibility.head)

      val decoder = new AvroFeatureDecoder(sft, EncodingOptions.withUserData)

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

      decoded.map(_.getID) mustEqual features.map(_.getID)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)

      forall(decoded) { sf =>
        sf.getAttributeCount mustEqual 1
        sf.getAttribute(0) must beAnInstanceOf[Point]
        sf.getFeatureType mustEqual projectedSft
      }
    }

    "be able to decode points with user data" >> {
      val encoder = new AvroFeatureEncoder(sft, EncodingOptions.withUserData)

      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")
      val decoder = new ProjectingAvroFeatureDecoder(sft, projectedSft, EncodingOptions.withUserData)

      val features = getFeaturesWithVisibility
      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(decoder.decode)

      forall(features.zip(decoded)) { case (in, out) =>
        out.getUserData mustEqual in.getUserData
      }
    }
  }

  "KryoFeatureEncoder" should {

    "return the correct encoding" >> {
      val encoder = new KryoFeatureEncoder(sft)
      encoder.encoding mustEqual FeatureEncoding.KRYO
    }

    "be able to encode points" >> {
      val encoder = new KryoFeatureEncoder(sft)
      val features = getFeatures

      val encoded = features.map(encoder.encode)
      encoded must not(beNull)
      encoded must have size features.size
    }

    "not include visibilities when not requested" >> {
      val encoder = new KryoFeatureEncoder(sft)
      val expected = getFeatures.map(encoder.encode)

      val featuresWithVis = getFeaturesWithVisibility
      val actual = featuresWithVis.map(encoder.encode)

      actual must haveSize(expected.size)

      forall(actual.zip(expected)) {
        case (a, e) => a mustEqual e
      }
    }

    "include user data when requested" >> {
      val noVis = {
        val encoder = new KryoFeatureEncoder(sft, EncodingOptions.none)
        getFeatures.map(encoder.encode)
      }
      val withVis = {
        val encoder = new KryoFeatureEncoder(sft, EncodingOptions.withUserData)
        getFeaturesWithVisibility.map(encoder.encode)
      }

      withVis must haveSize(noVis.size)

      forall(withVis.zip(noVis)) {
        case (y, n) => y.length must beGreaterThan(n.length)
      }
    }
  }

  "KryoFeatureDecoder" should {

    "return the correct encoding" >> {
      val decoder = new KryoFeatureDecoder(sft)
      decoder.encoding mustEqual FeatureEncoding.KRYO
    }

    "be able to decode points" >> {
      val encoder = new KryoFeatureEncoder(sft)
      val decoder = new KryoFeatureDecoder(sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(decoder.decode)
      decoded must equalFeatures(features, withoutUserData)
    }

    "be able to decode points with user data" >> {
      val encoder = new KryoFeatureEncoder(sft, EncodingOptions.withUserData)
      val decoder = new KryoFeatureDecoder(sft, EncodingOptions.withUserData)

      val features = getFeaturesWithVisibility
      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(decoder.decode)

      decoded must equalFeatures(features)
    }

    "be able to extract feature IDs" >> {
      val encoder = new KryoFeatureEncoder(sft)
      val decoder = new KryoFeatureDecoder(sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      encoded.map(decoder.extractFeatureId) mustEqual features.map(_.getID)
    }

    "work user data were encoded but are not expected by decoder" >> {
      // in this case the encoded user data will be ignored
      val sf = getFeaturesWithVisibility.head
      val encoder = new KryoFeatureEncoder(sft, EncodingOptions.withUserData)
      val encoded = encoder.encode(sf)

      val decoder = new KryoFeatureDecoder(sft, EncodingOptions.none)

      decoder.decode(encoded) must equalSF(sf, withoutUserData)
    }

    "fail when user data were not encoded but are expected by the decoder" >> {
      val encoder = new KryoFeatureEncoder(sft, EncodingOptions.none)
      val encoded = encoder.encode(getFeaturesWithVisibility.head)

      val decoder = new KryoFeatureDecoder(sft, EncodingOptions.withUserData)

      decoder.decode(encoded) must throwA[Exception]
    }
  }

  "ProjectingKryoFeatureDecoder" should {

    "properly project features" >> {
      val encoder = new KryoFeatureEncoder(sft)

      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")
      val projectingDecoder = new ProjectingKryoFeatureDecoder(sft, projectedSft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)
      val decoded = encoded.map(projectingDecoder.decode)

      decoded.map(_.getID) mustEqual features.map(_.getID)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)

      forall(decoded) { sf =>
        sf.getAttributeCount mustEqual 1
        sf.getAttribute(0) must beAnInstanceOf[Point]
        sf.getFeatureType mustEqual projectedSft
      }
    }

    "be able to decode points with user data" >> {
      val encoder = new KryoFeatureEncoder(sft, EncodingOptions.withUserData)

      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")
      val decoder = new ProjectingKryoFeatureDecoder(sft, projectedSft, EncodingOptions.withUserData)

      val features = getFeaturesWithVisibility
      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(decoder.decode)

      // when decoding any empty visibilities will be transformed to null
      forall(features.zip(decoded)) { case (in, out) =>
        out.getUserData mustEqual in.getUserData
      }
    }
  }

  type MatcherFactory[T] = (T) => Matcher[T]
  type UserDataMap = java.util.Map[AnyRef, AnyRef]

  val withoutUserData: MatcherFactory[UserDataMap] = {
    expected: UserDataMap => actual: UserDataMap => actual.isEmpty must beTrue
  }

  val withUserData: MatcherFactory[UserDataMap] = {
    expected: UserDataMap => actual: UserDataMap => actual mustEqual expected
  }

  def equalFeatures(expected: Seq[SimpleFeature], withUserDataMatcher: MatcherFactory[UserDataMap] = withUserData): Matcher[Seq[SimpleFeature]] = {
    actual: Seq[SimpleFeature] => {
      actual must not(beNull)
      actual must haveSize(expected.size)

      forall(actual zip expected) {
        case (act, exp) => act must equalSF(exp, withUserDataMatcher)
      }
    }
  }

  def equalSF(expected: SimpleFeature, matchUserData: MatcherFactory[UserDataMap] = withUserData): Matcher[SimpleFeature] = {
    sf: SimpleFeature => {
      sf.getID mustEqual expected.getID
      sf.getDefaultGeometry mustEqual expected.getDefaultGeometry
      sf.getAttributes mustEqual expected.getAttributes
      sf.getUserData must matchUserData(expected.getUserData)
    }
  }
}
