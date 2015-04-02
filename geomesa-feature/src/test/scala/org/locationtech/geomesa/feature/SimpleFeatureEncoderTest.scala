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

import java.nio.charset.StandardCharsets

import com.vividsolutions.jts.geom.Point
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.security.SecurityUtils
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
        SecurityUtils.setFeatureVisibility(sf, vis)
        sf
    })

    features
  }

  "SimpleFeatureEncoder" should {

    "have a properly working apply() method" >> {
      val opts = Set(EncodingOption.WITH_VISIBILITIES)

      // AVRO without options
      val avro1 = SimpleFeatureEncoder(sft, FeatureEncoding.AVRO)
      avro1 must beAnInstanceOf[AvroFeatureEncoder]
      avro1.options mustEqual Set.empty

      // AVRO with options
      val avro2 = SimpleFeatureEncoder(sft, FeatureEncoding.AVRO, opts)
      avro2 must beAnInstanceOf[AvroFeatureEncoder]
      avro2.options mustEqual opts

      // TEXT without options
      val text1 = SimpleFeatureEncoder(sft, FeatureEncoding.TEXT)
      text1 must beAnInstanceOf[TextFeatureEncoder]
      text1.options mustEqual Set.empty

      // TEXT with options
      val text2 = SimpleFeatureEncoder(sft, FeatureEncoding.TEXT, opts)
      text2 must beAnInstanceOf[TextFeatureEncoder]
      text2.options mustEqual opts

      // KRYO without options
      val kryo1 = SimpleFeatureEncoder(sft, FeatureEncoding.KRYO)
      kryo1 must beAnInstanceOf[KryoFeatureEncoder]
      kryo1.options mustEqual Set.empty

      // KRYO with options
      val kryo2 = SimpleFeatureEncoder(sft, FeatureEncoding.KRYO, opts)
      kryo2 must beAnInstanceOf[KryoFeatureEncoder]
      kryo2.options mustEqual opts
    }
  }

  "SimpleFeatureDecoder" should {

    "have a properly working apply() method" >> {
      val opts = Set(EncodingOption.WITH_VISIBILITIES)

      // AVRO without options
      val avro1 = SimpleFeatureDecoder(sft, FeatureEncoding.AVRO)
      avro1 must beAnInstanceOf[AvroFeatureDecoder]
      avro1.options mustEqual Set.empty

      // AVRO with options
      val avro2 = SimpleFeatureDecoder(sft, FeatureEncoding.AVRO, opts)
      avro2 must beAnInstanceOf[AvroFeatureDecoder]
      avro2.options mustEqual opts

      // TEXT without options
      val text1 = SimpleFeatureDecoder(sft, FeatureEncoding.TEXT)
      text1 must beAnInstanceOf[TextFeatureDecoder]
      text1.options mustEqual Set.empty

      // TEXT with options
      val text2 = SimpleFeatureDecoder(sft, FeatureEncoding.TEXT, opts)
      text2 must beAnInstanceOf[TextFeatureDecoder]
      text2.options mustEqual opts

      // KRYO without options
      val kryo1 = SimpleFeatureDecoder(sft, FeatureEncoding.KRYO)
      kryo1 must beAnInstanceOf[KryoFeatureEncoder]
      kryo1.options mustEqual Set.empty

      // KRYO with options
      val kryo2 = SimpleFeatureDecoder(sft, FeatureEncoding.KRYO, opts)
      kryo2 must beAnInstanceOf[KryoFeatureEncoder]
      kryo2.options mustEqual opts
    }
  }

  "ProjectingSimpleFeatureDecoder" should {

    "have a properly working apply() method" >> {
      val projectedSft = SimpleFeatureTypes.createType(sftName, "*geom:Point")
      val opts = Set(EncodingOption.WITH_VISIBILITIES)

      // AVRO without options
      val avro1 = ProjectingSimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.AVRO)
      avro1 must beAnInstanceOf[ProjectingAvroFeatureDecoder]
      avro1.options mustEqual Set.empty

      // AVRO with options
      val avro2 = ProjectingSimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.AVRO, opts)
      avro2 must beAnInstanceOf[ProjectingAvroFeatureDecoder]
      avro2.options mustEqual opts

      // TEXT without options
      val text1 = ProjectingSimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.TEXT)
      text1 must beAnInstanceOf[ProjectingTextFeatureDecoder]
      text1.options mustEqual Set.empty

      // TEXT with options
      val text2 = ProjectingSimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.TEXT, opts)
      text2 must beAnInstanceOf[ProjectingTextFeatureDecoder]
      text2.options mustEqual opts

      // KRYO without options
      val kryo1 = ProjectingSimpleFeatureDecoder(sft, projectedSft, FeatureEncoding.KRYO)
      kryo1 must beAnInstanceOf[KryoFeatureEncoder]
      kryo1.options mustEqual Set.empty

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
      decoded must equalFeatures(features)
    }

    "be able to decode points with visibility" >> {
      val encoder = new AvroFeatureEncoder(sft, Set(EncodingOption.WITH_VISIBILITIES))
      val decoder = new AvroFeatureDecoder(sft, Set(EncodingOption.WITH_VISIBILITIES))

      val features = getFeaturesWithVisibility
      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(decoder.decode)

      // when decoding any empty visibilities will be transformed to null
      decoded must equalFeatures(features, withLooseVisibilityMatch)
    }

    "be able to extract feature IDs" >> {
      val encoder = new AvroFeatureEncoder(sft)
      val decoder = new AvroFeatureDecoder(sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      encoded.map(decoder.extractFeatureId) mustEqual features.map(_.getID)
    }

    "fail when visibilities were encoded but are not expected by decoder" >> {
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

      decoded.map(_.getID) mustEqual features.map(_.getID)
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

    "include visibilities when requested" >> {
      val noVis = {
        val encoder = new KryoFeatureEncoder(sft, EncodingOptions.none)
        getFeatures.map(encoder.encode)
      }
      val withVis = {
        val encoder = new KryoFeatureEncoder(sft, Set(EncodingOption.WITH_VISIBILITIES))
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
      decoded must equalFeatures(features)
    }

    "be able to decode points with visibility" >> {
      val encoder = new KryoFeatureEncoder(sft, Set(EncodingOption.WITH_VISIBILITIES))
      val decoder = new KryoFeatureDecoder(sft, Set(EncodingOption.WITH_VISIBILITIES))

      val features = getFeaturesWithVisibility
      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(decoder.decode)

      // when decoding any empty visibilities will be transformed to null
      decoded must equalFeatures(features, withLooseVisibilityMatch)
    }

    "be able to extract feature IDs" >> {
      val encoder = new KryoFeatureEncoder(sft)
      val decoder = new KryoFeatureDecoder(sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      encoded.map(decoder.extractFeatureId) mustEqual features.map(_.getID)
    }

    "fail when visibilities were encoded but are not expected by decoder" >> {
      val encoder = new KryoFeatureEncoder(sft, Set(EncodingOption.WITH_VISIBILITIES))
      val encoded = encoder.encode(getFeaturesWithVisibility.head)

      val decoder = new KryoFeatureDecoder(sft, EncodingOptions.none)

      decoder.decode(encoded) must throwA[Exception]
    }

    "fail when visibilities were not encoded but are expected by the decoder" >> {
      val encoder = new KryoFeatureEncoder(sft, EncodingOptions.none)
      val encoded = encoder.encode(getFeaturesWithVisibility.head)

      val decoder = new KryoFeatureDecoder(sft, Set(EncodingOption.WITH_VISIBILITIES))

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

    "not include visibilities when not requested" >> {
      val encoder = new TextFeatureEncoder(sft)
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
        val encoder = new TextFeatureEncoder(sft, EncodingOptions.none)
        getFeatures.map(encoder.encode)
      }
      val withVis = {
        val encoder = new TextFeatureEncoder(sft, Set(EncodingOption.WITH_VISIBILITIES))
        getFeaturesWithVisibility.map(encoder.encode)
      }

      withVis must haveSize(noVis.size)

      forall(withVis.zip(noVis)) {
        case (y, n) => y.length must beGreaterThan(n.length)
      }
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
      decoded must equalFeatures(features)
    }

    "be able to decode points with visibility" >> {
      val encoder = new TextFeatureEncoder(sft, Set(EncodingOption.WITH_VISIBILITIES))
      val decoder = new TextFeatureDecoder(sft, Set(EncodingOption.WITH_VISIBILITIES))

      val features = getFeaturesWithVisibility
      val encoded = features.map(encoder.encode)

      val decoded = encoded.map(decoder.decode)

      // when decoding any empty visibilities will be transformed to null
      decoded must equalFeatures(features, withLooseVisibilityMatch)
    }

    "be able to extract feature IDs" >> {
      val encoder = new TextFeatureEncoder(sft)
      val decoder = new TextFeatureDecoder(sft)

      val features = getFeatures
      val encoded = features.map(encoder.encode)

      encoded.map(decoder.extractFeatureId) mustEqual features.map(_.getID)
    }

    "fail when visibilities were encoded but are not expected by decoder" >> {
      val encoder = new TextFeatureEncoder(sft, Set(EncodingOption.WITH_VISIBILITIES))
      val encoded = encoder.encode(getFeaturesWithVisibility.head)

      val decoder = new TextFeatureDecoder(sft, EncodingOptions.none)

      decoder.decode(encoded) must throwA[Exception]
    }

    "fail when visibilities were not encoded but are expected by the decoder" >> {
      val encoder = new TextFeatureEncoder(sft, EncodingOptions.none)
      val encoded = encoder.encode(getFeaturesWithVisibility.head)

      val decoder = new TextFeatureDecoder(sft, Set(EncodingOption.WITH_VISIBILITIES))

      decoder.decode(encoded) must throwA[Exception]
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

      decoded.map(_.getID) mustEqual features.map(_.getID)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)

      forall(decoded) { sf =>
        sf.getAttributeCount mustEqual 1
        sf.getAttribute(0) must beAnInstanceOf[Point]
        sf.getFeatureType mustEqual projectedSft
      }
    }
  }

  def equalFeatures(expected: Seq[SimpleFeature], withVisibilityMatch: MatcherFactory[String] = withStrictVisibilityMatch): Matcher[Seq[SimpleFeature]] = {
    actual: Seq[SimpleFeature] => {
      actual must not(beNull)
      actual must haveSize(expected.size)

      forall(actual zip expected) {
        case (act, exp) => act must equalSF(exp, withVisibilityMatch)
      }
    }
  }

  type MatcherFactory[T] = (T) => Matcher[T]

  val withStrictVisibilityMatch: MatcherFactory[String] = {
    expected: String => actual: String => actual mustEqual expected
  }

  // when decoding any empty visibilities will be transformed into null
  val withLooseVisibilityMatch: MatcherFactory[String] = {
    expected: String =>
      if (expected == "") {
        actual: String => actual must beNull
      } else {
        actual: String => actual mustEqual expected
      }
  }

  def equalSF(expected: SimpleFeature, matchVisibility: MatcherFactory[String] = withStrictVisibilityMatch): Matcher[SimpleFeature] = {
    sf: SimpleFeature => {
      sf.getID mustEqual expected.getID
      sf.getDefaultGeometry mustEqual expected.getDefaultGeometry
      sf.getAttributes mustEqual expected.getAttributes
      SecurityUtils.getVisibility(sf) must matchVisibility(SecurityUtils.getVisibility(expected))
    }
  }
}
