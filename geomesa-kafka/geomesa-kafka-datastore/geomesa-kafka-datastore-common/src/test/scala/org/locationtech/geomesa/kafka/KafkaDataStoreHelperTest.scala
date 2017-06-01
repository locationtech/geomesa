/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import java.util.Date

import org.geotools.feature.AttributeTypeBuilder
import org.joda.time.{Duration, Instant}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreHelperTest extends Specification {

  "KafkaDataStoreHelper" should {

    "insert topic correctly into sft" >> {

      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      KafkaDataStoreHelper.insertTopic(sft,"test-topic")

      sft.getUserData.get(KafkaDataStoreHelper.TopicKey).asInstanceOf[String] mustEqual "test-topic"
    }

    "extract topic correctly from sft" >> {

      val sft = SimpleFeatureTypes.createType("test2", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      KafkaDataStoreHelper.insertTopic(sft,"another-test-topic")

      KafkaDataStoreHelper.extractTopic(sft) must beSome("another-test-topic")
    }

    "extract None when no topic present" >> {
      val sft = SimpleFeatureTypes.createType("test3", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

      KafkaDataStoreHelper.extractTopic(sft) must beNone
    }

    "insert ReplayConfig string correctly into sft" >> {

      val sft = SimpleFeatureTypes.createType("testy", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      KafkaDataStoreHelper.insertReplayConfig(sft,"this is a test replay string")

      sft.getUserData.get(KafkaDataStoreHelper.ReplayConfigKey) mustEqual "this is a test replay string"
    }

    "extract ReplayConfig correctly from sft" >> {

      val sft = SimpleFeatureTypes.createType("testy", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
      KafkaDataStoreHelper.insertReplayConfig(sft,ReplayConfig.encode(rc))

      KafkaDataStoreHelper.extractReplayConfig(sft) must beSome(rc)
    }

    "createStreamingSFT method creates a copy of the original sft" >> {
      val sft = SimpleFeatureTypes.createType("testy", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val prepped = KafkaDataStoreHelper.createStreamingSFT(sft,"/my/root/path")

      // check that it is a different instance and a few key properties match
      prepped must not beTheSameAs sft
      prepped.getTypeName mustEqual sft.getTypeName
      prepped.getAttributeCount mustEqual sft.getAttributeCount
      prepped.getAttributeDescriptors.asScala must containTheSameElementsAs(sft.getAttributeDescriptors.asScala)
      prepped.getTypes.size mustEqual sft.getTypes.size
      prepped.getTypes.asScala must containTheSameElementsAs(sft.getTypes.asScala)

    }

    "createStreamingSFT method adds topic data into the sft" >> {
      val sft = SimpleFeatureTypes.createType("testy", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val prepped = KafkaDataStoreHelper.createStreamingSFT(sft,"/my/root/path")

      //check that the topic entry has been added
      prepped.getUserData.size() mustEqual (sft.getUserData.size() + 1)
      prepped.getUserData.get(KafkaDataStoreHelper.TopicKey) must not be null
    }

    "createStreamingSFT topics are unique to (type name, path) combos" >> {
      val sft = SimpleFeatureTypes.createType("typename1", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val prepped = KafkaDataStoreHelper.createStreamingSFT(sft,"/my/root/path")


      val sftDiffSpec = SimpleFeatureTypes.createType("typename1", "age:Int,dtg:Date,*geom:Point:srid=4326")
      val preppedDiffSpec = KafkaDataStoreHelper.createStreamingSFT(sftDiffSpec,"/my/root/path")

      // topic should be same if only specs differ
      KafkaDataStoreHelper.extractTopic(preppedDiffSpec) mustEqual KafkaDataStoreHelper.extractTopic(prepped)

      val sftDiffName = SimpleFeatureTypes.createType("typename2", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val preppedDiffName = KafkaDataStoreHelper.createStreamingSFT(sftDiffName,"/my/root/path")

      // topic should differ if name differs and zkpath same
      KafkaDataStoreHelper.extractTopic(preppedDiffName) mustNotEqual KafkaDataStoreHelper.extractTopic(prepped)

      val preppedDiffPath = KafkaDataStoreHelper.createStreamingSFT(sft,"/my/other/root/path")

      //topic should differ if name is same and path differs
      KafkaDataStoreHelper.extractTopic(preppedDiffPath) mustNotEqual KafkaDataStoreHelper.extractTopic(prepped)

    }

    "createReplaySFT" should {
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,*geom:Point:srid=4326")
      val streamingSft = KafkaDataStoreHelper.createStreamingSFT(sft, "/test/path")

      "create a copy of the original sft" >> {

        val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
        val prepped = KafkaDataStoreHelper.createReplaySFT(streamingSft, rc)

        // check that it is a different instance and a few key properties match
        // don't check typename or attributes!
        prepped must not beTheSameAs streamingSft

        //check that the rc entry has been added
        prepped.getUserData.size() mustEqual (streamingSft.getUserData.size() + 1)
        prepped.getUserData.get(KafkaDataStoreHelper.ReplayConfigKey) must not be null
      }

      "add replay config data into the sft" >> {
        val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
        val prepped = KafkaDataStoreHelper.createReplaySFT(streamingSft, rc)

        //check that the rc entry has been added
        prepped.getUserData.size() mustEqual (streamingSft.getUserData.size() + 1)
        prepped.getUserData.get(KafkaDataStoreHelper.ReplayConfigKey) must not be null
      }

      "change the typename" >> {
        val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
        val prepped = KafkaDataStoreHelper.createReplaySFT(streamingSft, rc)

        prepped.getTypeName mustNotEqual streamingSft.getTypeName
        prepped.getTypeName must contain(streamingSft.getTypeName) and contain("REPLAY")

        "to a unique value" >> {
          val prepped2 = KafkaDataStoreHelper.createReplaySFT(streamingSft, rc)

          prepped.getTypeName mustNotEqual prepped2.getTypeName
        }
      }

      "add message timestamp attribute" >> {
        val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
        val prepped = KafkaDataStoreHelper.createReplaySFT(streamingSft, rc)

        val (expectedAttribType, expectedAttribute) = {
          val builder = new AttributeTypeBuilder()
          builder.setBinding(classOf[Date])
          builder.setName(ReplayTimeHelper.AttributeName)

          val attribType = builder.buildType
          val attrib = builder.buildDescriptor(ReplayTimeHelper.AttributeName, attribType)

          (attribType, attrib)
        }

        val expectedDescriptors = streamingSft.getAttributeDescriptors.asScala :+ expectedAttribute
        val expectedTypes = streamingSft.getTypes.asScala :+ expectedAttribType

        prepped.getAttributeCount mustEqual streamingSft.getAttributeCount + 1
        prepped.getAttributeDescriptors.asScala must containTheSameElementsAs(expectedDescriptors)
        prepped.getTypes.size mustEqual streamingSft.getTypes.size + 1
        prepped.getTypes.asScala must containTheSameElementsAs(expectedTypes)
      }

      "fail if the SFT is not a Streaming SFT" >> {
        val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
        KafkaDataStoreHelper.createReplaySFT(sft, rc) must throwA[IllegalArgumentException]
      }
    }

    "be able to extractLiveTypeName" >> {

      "from a replay type" >> {
        val sft = SimpleFeatureTypes.createType("test-REPLAY-blah", "name:String,age:Int,*geom:Point:srid=4326")
        val result = KafkaDataStoreHelper.extractStreamingTypeName(sft)
        result must beSome("test")
      }

      "or none from a non-replay type" >> {
        val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,*geom:Point:srid=4326")
        val result = KafkaDataStoreHelper.extractStreamingTypeName(sft)
        result must beNone
      }
    }

    "correctly determine if isPreparedForLive" >> {

      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,*geom:Point:srid=4326")
      val live = KafkaDataStoreHelper.createStreamingSFT(sft, "/test/path")
      val replay = KafkaDataStoreHelper.createReplaySFT(live, ReplayConfig(0,0,0))

      KafkaDataStoreHelper.isStreamingSFT(sft) must beFalse
      KafkaDataStoreHelper.isStreamingSFT(live) must beTrue
      KafkaDataStoreHelper.isStreamingSFT(replay) must beFalse
    }

    "correctly determine if isPreparedForReplay" >> {

      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,*geom:Point:srid=4326")
      val live = KafkaDataStoreHelper.createStreamingSFT(sft, "/test/path")
      val replay = KafkaDataStoreHelper.createReplaySFT(live, ReplayConfig(0,0,0))

      KafkaDataStoreHelper.isPreparedForReplay(sft) must beFalse
      KafkaDataStoreHelper.isPreparedForReplay(live) must beFalse
      KafkaDataStoreHelper.isPreparedForReplay(replay) must beTrue
    }

    "cleanZkPath handles works as advertised"  >> {

      // a well formed path starts with a / and does not end with a /
      val wellFormed = "/this/is/well/formed"
      KafkaDataStoreHelper.cleanZkPath(wellFormed) mustEqual wellFormed

      //check trailing slash
      val withTrailing = "/this/has/trailing/slash/"
      KafkaDataStoreHelper.cleanZkPath(withTrailing) mustEqual "/this/has/trailing/slash"

      //check no leading slash
      val withNoLeading = "this/has/no/leading/slash"
      KafkaDataStoreHelper.cleanZkPath(withNoLeading) mustEqual "/this/has/no/leading/slash"

      //check both no leading and with trailing
      val noLeadWithTrail = "no/leading/with/trailing/"
      KafkaDataStoreHelper.cleanZkPath(noLeadWithTrail) mustEqual "/no/leading/with/trailing"

      //check single slash (should be ok)
      val singleSlash = "/"
      KafkaDataStoreHelper.cleanZkPath(singleSlash) mustEqual singleSlash

      // check empty string
      val empty = ""
      KafkaDataStoreHelper.cleanZkPath(empty) mustEqual KafkaDataStoreHelper.DefaultZkPath
      KafkaDataStoreHelper.cleanZkPath(empty,"mydefault") mustEqual "mydefault"

      // check null string
      val nullString = null
      KafkaDataStoreHelper.cleanZkPath(nullString) mustEqual KafkaDataStoreHelper.DefaultZkPath
      KafkaDataStoreHelper.cleanZkPath(nullString,"mydefault2") mustEqual "mydefault2"

      // check // string
      val doubleSlash = "//"
      KafkaDataStoreHelper.cleanZkPath(doubleSlash) mustEqual "/"
    }

    "buildTopicName should combine zkPath and SFT name" >> {

      val zkPath = "/test/zk/path"
      val sft = SimpleFeatureTypes.createType("testType", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

      val topic = KafkaDataStoreHelper.buildTopicName(zkPath, sft)
      topic mustEqual "test-zk-path-testType"
    }
  }
}
