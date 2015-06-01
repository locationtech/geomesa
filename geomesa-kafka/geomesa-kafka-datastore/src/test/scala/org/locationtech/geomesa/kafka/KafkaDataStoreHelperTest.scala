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

    "prepareForLive method creates a copy of the original sft" >> {
      val sft = SimpleFeatureTypes.createType("testy", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val prepped = KafkaDataStoreHelper.prepareForLive(sft,"/my/root/path")

      // check that it is a different instance and a few key properties match
      prepped must not beTheSameAs sft
      prepped.getTypeName mustEqual sft.getTypeName
      prepped.getAttributeCount mustEqual sft.getAttributeCount
      prepped.getAttributeDescriptors.asScala must containTheSameElementsAs(sft.getAttributeDescriptors.asScala)
      prepped.getTypes.size mustEqual sft.getTypes.size
      prepped.getTypes.asScala must containTheSameElementsAs(sft.getTypes.asScala)

    }

    "prepareForLive method adds topic data into the sft" >> {
      val sft = SimpleFeatureTypes.createType("testy", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val prepped = KafkaDataStoreHelper.prepareForLive(sft,"/my/root/path")

      //check that the topic entry has been added
      prepped.getUserData.size() mustEqual (sft.getUserData.size() + 1)
      prepped.getUserData.get(KafkaDataStoreHelper.TopicKey) must not be null
    }

    "prepareForLive topics are unique to (type name, path) combos" >> {
      val sft = SimpleFeatureTypes.createType("typename1", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val prepped = KafkaDataStoreHelper.prepareForLive(sft,"/my/root/path")


      val sftDiffSpec = SimpleFeatureTypes.createType("typename1", "age:Int,dtg:Date,*geom:Point:srid=4326")
      val preppedDiffSpec = KafkaDataStoreHelper.prepareForLive(sftDiffSpec,"/my/root/path")

      // topic should be same if only specs differ
      KafkaDataStoreHelper.extractTopic(preppedDiffSpec) mustEqual KafkaDataStoreHelper.extractTopic(prepped)

      val sftDiffName = SimpleFeatureTypes.createType("typename2", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val preppedDiffName = KafkaDataStoreHelper.prepareForLive(sftDiffName,"/my/root/path")

      // topic should differ if name differs and zkpath same
      KafkaDataStoreHelper.extractTopic(preppedDiffName) mustNotEqual KafkaDataStoreHelper.extractTopic(prepped)

      val preppedDiffPath = KafkaDataStoreHelper.prepareForLive(sft,"/my/other/root/path")

      //topic should differ if name is same and path differs
      KafkaDataStoreHelper.extractTopic(preppedDiffPath) mustNotEqual KafkaDataStoreHelper.extractTopic(prepped)

    }

    "prepareForReplay method creates a copy of the original sft" >> {
      val sft = SimpleFeatureTypes.createType("testy", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
      val prepped = KafkaDataStoreHelper.prepareForReplay(sft,rc)

      // check that it is a different instance and a few key properties match
      // don't check typename or attributes!
      prepped must not beTheSameAs sft

      //check that the rc entry has been added
      prepped.getUserData.size() mustEqual (sft.getUserData.size() + 1)
      prepped.getUserData.get(KafkaDataStoreHelper.ReplayConfigKey) must not be null
    }

    "prepareForReplay method adds replay config data into the sft" >> {
      val sft = SimpleFeatureTypes.createType("testy", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
      val prepped = KafkaDataStoreHelper.prepareForReplay(sft,rc)

      //check that the rc entry has been added
      prepped.getUserData.size() mustEqual (sft.getUserData.size() + 1)
      prepped.getUserData.get(KafkaDataStoreHelper.ReplayConfigKey) must not be null
    }

    "prepareForReplay method changes the typename" >> {
      val sft = SimpleFeatureTypes.createType("testy", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
      val prepped = KafkaDataStoreHelper.prepareForReplay(sft,rc)

      prepped.getTypeName mustNotEqual sft.getTypeName
    }

    "prepareForReplay method creates unique typename" >> {
      val sft = SimpleFeatureTypes.createType("testy", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
      val prepped = KafkaDataStoreHelper.prepareForReplay(sft,rc)
      val prepped2 = KafkaDataStoreHelper.prepareForReplay(sft,rc)

      prepped.getTypeName mustNotEqual prepped2.getTypeName
    }

    "prepareForReplay adds message timestamp attribute" >> {

      val sft = {
        val schema = SimpleFeatureTypes.createType("test", "name:String,age:Int,*geom:Point:srid=4326")
        KafkaDataStoreHelper.prepareForLive(schema, "/test/path")
      }

      val rc = new ReplayConfig(new Instant(123L), new Instant(223L), new Duration(5L))
      val prepped = KafkaDataStoreHelper.prepareForReplay(sft, rc)

      val (expectedAttribType, expectedAttribute) = {
        val builder = new AttributeTypeBuilder()
        builder.setBinding(classOf[Date])
        builder.setName(ReplayTimeHelper.AttributeName)

        val attribType = builder.buildType
        val attrib = builder.buildDescriptor(ReplayTimeHelper.AttributeName, attribType)

        (attribType, attrib)
      }

      val expectedDescriptors = sft.getAttributeDescriptors.asScala :+ expectedAttribute
      val expectedTypes = sft.getTypes.asScala :+ expectedAttribType

      prepped.getAttributeCount mustEqual sft.getAttributeCount + 1
      prepped.getAttributeDescriptors.asScala must containTheSameElementsAs(expectedDescriptors)
      prepped.getTypes.size mustEqual sft.getTypes.size + 1
      prepped.getTypes.asScala must containTheSameElementsAs(expectedTypes)
    }

    "be able to extractLiveTypeName" >> {

      "from a replay type" >> {
        val sft = SimpleFeatureTypes.createType("test-REPLAY-blah", "name:String,age:Int,*geom:Point:srid=4326")
        val result = KafkaDataStoreHelper.extractLiveTypeName(sft)
        result must beSome("test")
      }

      "or none from a non-replay type" >> {
        val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,*geom:Point:srid=4326")
        val result = KafkaDataStoreHelper.extractLiveTypeName(sft)
        result must beNone
      }
    }

    "correctly determine if isPreparedForLive" >> {

      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,*geom:Point:srid=4326")
      val live = KafkaDataStoreHelper.prepareForLive(sft, "/test/path")
      val replay = KafkaDataStoreHelper.prepareForReplay(live, ReplayConfig(0,0,0))

      KafkaDataStoreHelper.isPreparedForLive(sft) must beFalse
      KafkaDataStoreHelper.isPreparedForLive(live) must beTrue
      KafkaDataStoreHelper.isPreparedForLive(replay) must beFalse
    }

    "correctly determine if isPreparedForReplay" >> {

      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,*geom:Point:srid=4326")
      val live = KafkaDataStoreHelper.prepareForLive(sft, "/test/path")
      val replay = KafkaDataStoreHelper.prepareForReplay(live, ReplayConfig(0,0,0))

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
  }
}
