package org.locationtech.geomesa.kafka

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

      // check that it is a different instance and a few key properties match (don't check typename!)
      prepped must not beTheSameAs sft
      prepped.getAttributeCount mustEqual sft.getAttributeCount
      prepped.getAttributeDescriptors.asScala must containTheSameElementsAs(sft.getAttributeDescriptors.asScala)
      prepped.getTypes.size mustEqual sft.getTypes.size
      prepped.getTypes.asScala must containTheSameElementsAs(sft.getTypes.asScala)

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
  }
}
