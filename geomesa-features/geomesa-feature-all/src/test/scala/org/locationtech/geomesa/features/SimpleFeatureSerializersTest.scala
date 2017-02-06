/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.features

import com.vividsolutions.jts.geom.Point
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.avro.{AvroFeatureDeserializer, AvroFeatureSerializer, AvroSimpleFeatureFactory, ProjectingAvroFeatureDeserializer}
import org.locationtech.geomesa.features.kryo.{KryoFeatureSerializer, ProjectingKryoFeatureDeserializer}
import org.locationtech.geomesa.security
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.{MatchResult, Matcher}
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SimpleFeatureSerializersTest extends org.specs2.mutable.Spec with org.specs2.matcher.SequenceMatchersCreation {

  sequential

  val sftName = "SimpleFeatureSerializersTest"
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
    import security._

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
      val opts = SerializationOptions.withUserData

      // AVRO without options
      val avro1 = SimpleFeatureSerializers(sft, SerializationType.AVRO)
      avro1 must beAnInstanceOf[AvroFeatureSerializer]
      avro1.options mustEqual SerializationOptions.none

      // AVRO with options
      val avro2 = SimpleFeatureSerializers(sft, SerializationType.AVRO, opts)
      avro2 must beAnInstanceOf[AvroFeatureSerializer]
      avro2.options mustEqual opts

      // KRYO without options
      val kryo1 = SimpleFeatureSerializers(sft, SerializationType.KRYO)
      kryo1 must beAnInstanceOf[KryoFeatureSerializer]
      kryo1.options mustEqual SerializationOptions.none

      // KRYO with options
      val kryo2 = SimpleFeatureSerializers(sft, SerializationType.KRYO, opts)
      kryo2 must beAnInstanceOf[KryoFeatureSerializer]
      kryo2.options mustEqual opts
    }
  }

  "SimpleFeatureDecoder" should {

    "have a properly working apply() method" >> {
      val opts = SerializationOptions.withUserData

      // AVRO without options
      val avro1 = SimpleFeatureDeserializers(sft, SerializationType.AVRO)
      avro1 must beAnInstanceOf[AvroFeatureDeserializer]
      avro1.options mustEqual SerializationOptions.none

      // AVRO with options
      val avro2 = SimpleFeatureDeserializers(sft, SerializationType.AVRO, opts)
      avro2 must beAnInstanceOf[AvroFeatureDeserializer]
      avro2.options mustEqual opts

      // KRYO without options
      val kryo1 = SimpleFeatureDeserializers(sft, SerializationType.KRYO)
      kryo1 must beAnInstanceOf[KryoFeatureSerializer]
      kryo1.options mustEqual SerializationOptions.none

      // KRYO with options
      val kryo2 = SimpleFeatureDeserializers(sft, SerializationType.KRYO, opts)
      kryo2 must beAnInstanceOf[KryoFeatureSerializer]
      kryo2.options mustEqual opts
    }
  }

  "ProjectingSimpleFeatureDecoder" should {

    "have a properly working apply() method" >> {
      val projectedSft = SimpleFeatureTypes.createType(sftName, "*geom:Point")
      val opts = SerializationOptions.withUserData

      // AVRO without options
      val avro1 = ProjectingSimpleFeatureDeserializers(sft, projectedSft, SerializationType.AVRO)
      avro1 must beAnInstanceOf[ProjectingAvroFeatureDeserializer]
      avro1.options mustEqual SerializationOptions.none

      // AVRO with options
      val avro2 = ProjectingSimpleFeatureDeserializers(sft, projectedSft, SerializationType.AVRO, opts)
      avro2 must beAnInstanceOf[ProjectingAvroFeatureDeserializer]
      avro2.options mustEqual opts

      // KRYO without options
      val kryo1 = ProjectingSimpleFeatureDeserializers(sft, projectedSft, SerializationType.KRYO)
      kryo1 must beAnInstanceOf[ProjectingKryoFeatureDeserializer]
      kryo1.options mustEqual SerializationOptions.none

      // KRYO with options
      val kryo2 = ProjectingSimpleFeatureDeserializers(sft, projectedSft, SerializationType.KRYO, opts)
      kryo2 must beAnInstanceOf[ProjectingKryoFeatureDeserializer]
      kryo2.options mustEqual opts
    }
  }

  "AvroFeatureSerializer" should {

    "be able to encode points" >> {
      val encoder = new AvroFeatureSerializer(sft)
      val features = getFeatures

      val encoded = features.map(encoder.serialize)
      encoded must not(beNull)
      encoded must haveSize(features.size)
    }

    "not include user data when not requested" >> {
      val encoder = new AvroFeatureSerializer(sft)
      val expected = getFeatures.map(encoder.serialize)

      val featuresWithVis = getFeaturesWithVisibility
      val actual = featuresWithVis.map(encoder.serialize)

      actual must haveSize(expected.size)

      forall(actual.zip(expected)) {
        case (a, e) => a mustEqual e
      }
    }

    "include user data when requested" >> {
      val noUserData = {
        val encoder = new AvroFeatureSerializer(sft, SerializationOptions.none)
        getFeatures.map(encoder.serialize)
      }
      val withUserData = {
        val encoder = new AvroFeatureSerializer(sft, SerializationOptions.withUserData)
        getFeaturesWithVisibility.map(encoder.serialize)
      }

      withUserData must haveSize(noUserData.size)

      forall(withUserData.zip(noUserData)) {
        case (y, n) => y.length must beGreaterThan(n.length)
      }
    }
  }

  "AvroFeatureDeserializer" should {

    "be able to decode points" >> {
      val encoder = new AvroFeatureSerializer(sft)
      val decoder = new AvroFeatureDeserializer(sft)

      val features = getFeatures
      val encoded = features.map(encoder.serialize)

      val decoded = encoded.map(decoder.deserialize)

      featuresMustBeEqual(features, withoutUserData)(decoded)
    }

    "be able to decode points with user data" >> {
      val encoder = new AvroFeatureSerializer(sft, SerializationOptions.withUserData)
      val decoder = new AvroFeatureDeserializer(sft, SerializationOptions.withUserData)

      val features = getFeaturesWithVisibility
      val encoded = features.map(encoder.serialize)

      val decoded = encoded.map(decoder.deserialize)

      featuresMustBeEqual(features, withUserData)(decoded)
    }

    "work when user data were encoded but are not expected by decoder" >> {
      // in this case the encoded user data will be ignored
      val sf = getFeaturesWithVisibility.head
      val encoder = new AvroFeatureSerializer(sft, SerializationOptions.withUserData)
      val encoded = encoder.serialize(sf)

      val decoder = new AvroFeatureDeserializer(sft, SerializationOptions.none)

      featureMustBeEqual(sf, withoutUserData)(decoder.deserialize(encoded))
    }

    "fail when user data were not encoded but are expected by the decoder" >> {
      val encoder = new AvroFeatureSerializer(sft, SerializationOptions.none)
      val encoded = encoder.serialize(getFeaturesWithVisibility.head)

      val decoder = new AvroFeatureDeserializer(sft, SerializationOptions.withUserData)

      decoder.deserialize(encoded) must throwA[Exception]
    }
  }

  "ProjectingAvroFeatureDeserializer" should {

    "properly project features" >> {
      val encoder = new AvroFeatureSerializer(sft)

      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")
      val projectingDecoder = new ProjectingAvroFeatureDeserializer(sft, projectedSft)

      val features = getFeatures
      val encoded = features.map(encoder.serialize)
      val decoded = encoded.map(projectingDecoder.deserialize)

      decoded.map(_.getID) mustEqual features.map(_.getID)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)

      forall(decoded) { sf =>
        sf.getAttributeCount mustEqual 1
        sf.getAttribute(0) must beAnInstanceOf[Point]
        sf.getFeatureType mustEqual projectedSft
      }
    }

    "be able to decode points with user data" >> {
      val encoder = new AvroFeatureSerializer(sft, SerializationOptions.withUserData)

      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")
      val decoder = new ProjectingAvroFeatureDeserializer(sft, projectedSft, SerializationOptions.withUserData)

      val features = getFeaturesWithVisibility
      val encoded = features.map(encoder.serialize)

      val decoded = encoded.map(decoder.deserialize)

      forall(features.zip(decoded)) { case (in, out) =>
        out.getUserData.toSeq must containTheSameElementsAs(in.getUserData.toSeq)
      }
    }
  }

  "KryoFeatureEncoder" should {

    "be able to encode points" >> {
      val encoder = new KryoFeatureSerializer(sft)
      val features = getFeatures

      val encoded = features.map(encoder.serialize)
      encoded must not(beNull)
      encoded must haveSize(features.size)
    }

    "not include visibilities when not requested" >> {
      val encoder = new KryoFeatureSerializer(sft)
      val expected = getFeatures.map(encoder.serialize)

      val featuresWithVis = getFeaturesWithVisibility
      val actual = featuresWithVis.map(encoder.serialize)

      actual must haveSize(expected.size)

      forall(actual.zip(expected)) {
        case (a, e) => a mustEqual e
      }
    }

    "include user data when requested" >> {
      val noVis = {
        val encoder = new KryoFeatureSerializer(sft, SerializationOptions.none)
        getFeatures.map(encoder.serialize)
      }
      val withVis = {
        val encoder = new KryoFeatureSerializer(sft, SerializationOptions.withUserData)
        getFeaturesWithVisibility.map(encoder.serialize)
      }

      withVis must haveSize(noVis.size)

      forall(withVis.zip(noVis)) {
        case (y, n) => y.length must beGreaterThan(n.length)
      }
    }
  }

  "KryoFeatureDecoder" should {

    "be able to decode points" >> {
      val encoder = new KryoFeatureSerializer(sft)
      val decoder = new KryoFeatureSerializer(sft)

      val features = getFeatures
      val encoded = features.map(encoder.serialize)

      val decoded = encoded.map(decoder.deserialize)
      featuresMustBeEqual(features, withoutUserData)(decoded)
    }

    "be able to decode points with user data" >> {
      val encoder = new KryoFeatureSerializer(sft, SerializationOptions.withUserData)
      val decoder = new KryoFeatureSerializer(sft, SerializationOptions.withUserData)

      val features = getFeaturesWithVisibility
      val encoded = features.map(encoder.serialize)

      val decoded = encoded.map(decoder.deserialize)

      forall(decoded.zip(features)) { case (d, sf) =>
        d.getID mustEqual sf.getID
        d.getAttributes mustEqual sf.getAttributes
        d.getUserData.toSeq must containTheSameElementsAs(sf.getUserData.filter(_._2 != null).toSeq)
      }
    }

    "work user data were encoded but are not expected by decoder" >> {
      // in this case the encoded user data will be ignored
      val sf = getFeaturesWithVisibility.head
      val encoder = new KryoFeatureSerializer(sft, SerializationOptions.withUserData)
      val encoded = encoder.serialize(sf)

      val decoder = new KryoFeatureSerializer(sft, SerializationOptions.none)

      featureMustBeEqual(sf, withoutUserData)(decoder.deserialize(encoded))
    }

    "fail when user data were not encoded but are expected by the decoder" >> {
      val encoder = new KryoFeatureSerializer(sft, SerializationOptions.none)
      val encoded = encoder.serialize(getFeaturesWithVisibility.head)

      val decoder = new KryoFeatureSerializer(sft, SerializationOptions.withUserData)

      decoder.deserialize(encoded) must throwA[Exception]
    }
  }

  "ProjectingKryoFeatureDecoder" should {

    "properly project features" >> {
      val encoder = new KryoFeatureSerializer(sft)

      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")
      val projectingDecoder = new ProjectingKryoFeatureDeserializer(sft, projectedSft)

      val features = getFeatures
      val encoded = features.map(encoder.serialize)
      val decoded = encoded.map(projectingDecoder.deserialize)

      decoded.map(_.getID) mustEqual features.map(_.getID)
      decoded.map(_.getDefaultGeometry) mustEqual features.map(_.getDefaultGeometry)

      forall(decoded) { sf =>
        sf.getAttributeCount mustEqual 1
        sf.getAttribute(0) must beAnInstanceOf[Point]
        sf.getFeatureType mustEqual projectedSft
      }
    }

    "be able to decode points with user data" >> {
      val encoder = new KryoFeatureSerializer(sft, SerializationOptions.withUserData)

      val projectedSft = SimpleFeatureTypes.createType("projectedTypeName", "*geom:Point")
      val decoder = new ProjectingKryoFeatureDeserializer(sft, projectedSft, SerializationOptions.withUserData)

      val features = getFeaturesWithVisibility
      val encoded = features.map(encoder.serialize)

      val decoded = encoded.map(decoder.deserialize)

      // when decoding any empty visibilities will be transformed to null
      forall(features.zip(decoded)) { case (in, out) =>
        out.getUserData.toSeq must containTheSameElementsAs(in.getUserData.filter(_._2 != null).toSeq)
      }
    }
  }

  type MatcherFactory[T] = (T) => Matcher[T]
  type UserDataMap = java.util.Map[AnyRef, AnyRef]

  val withoutUserData: (UserDataMap, UserDataMap) => MatchResult[Any] =
    (actual, _) => { actual must beEmpty }

  val withUserData: (UserDataMap, UserDataMap) => MatchResult[Any] =
    (actual, expected) => { actual.toSeq must containTheSameElementsAs(expected.toSeq) }

  def featuresMustBeEqual(expected: Seq[SimpleFeature],
                          userDataCheck: (UserDataMap, UserDataMap) => MatchResult[Any])
                         (actual: Seq[SimpleFeature]): MatchResult[Any] = {
    actual must not(beNull)
    actual must haveSize(expected.size)

    forall(actual.zip(expected)) { case (act, exp) => featureMustBeEqual(exp, userDataCheck)(act) }
  }

  def featureMustBeEqual(expected: SimpleFeature,
                         userDataCheck: (UserDataMap, UserDataMap) => MatchResult[Any])
                        (actual: SimpleFeature): MatchResult[Any] = {
    actual.getID mustEqual expected.getID
    actual.getDefaultGeometry mustEqual expected.getDefaultGeometry
    actual.getAttributes mustEqual expected.getAttributes
    userDataCheck(actual.getUserData, expected.getUserData)
  }
}
