/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import org.geotools.api.data._
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.KafkaContainerTest
import org.locationtech.geomesa.kafka.data.KafkaDataStoreMergeViewTest.TestAuthsProvider
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner

import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreMergeViewTest extends KafkaContainerTest with Mockito {

  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  lazy private val baseParams = Map(
    "kafka.brokers"            -> brokers,
    "kafka.topic.partitions"   -> 1,
    "kafka.topic.replication"  -> 1,
    "kafka.consumer.read-back" -> "Inf"
  )

  private val paths = new AtomicInteger(0)

  def getUniqueCatalog: String = s"geomesa-${paths.getAndIncrement()}-test"

  "KafkaDataStore" should {

    "support visibilities through merged view stores" >> {
      val sft = SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val catalogUser = getUniqueCatalog
      val catalogAdmin = getUniqueCatalog

      val fUser = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
      fUser.getUserData.put("geomesa.feature.visibility", "USER")
      val fAdmin = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")
      fAdmin.getUserData.put("geomesa.feature.visibility", "USER&ADMIN")

      // set up the layers and features
      Seq((catalogUser, fUser), (catalogAdmin, fAdmin)).foreach { case (catalog, f) =>
        WithClose(DataStoreFinder.getDataStore((baseParams ++ Map("kafka.catalog.topic" -> catalog, "kafka.consumer.count" -> 0)).asJava)) { producer =>
          producer.createSchema(sft)
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            FeatureUtils.write(writer, f, useProvidedFid = true)
          }
        }
      }

      val config =
        s"""stores = [
           |  {
           |    "kafka.brokers" = "$brokers"
           |    "kafka.catalog.topic" = "$catalogUser"
           |    "kafka.consumer.read-back" = "Inf"
           |    "geomesa.security.auths.provider" = "${classOf[TestAuthsProvider].getName}"
           |  },
           |  {
           |    "kafka.brokers" = "$brokers"
           |    "kafka.catalog.topic" = "$catalogAdmin"
           |    "kafka.consumer.read-back" = "Inf"
           |    "geomesa.security.auths.provider" = "${classOf[TestAuthsProvider].getName}"
           |  }
           |]""".stripMargin
      val params = java.util.Map.of(
        "geomesa.merged.stores", config,
        "geomesa.merged.deduplicate", true
      )
      WithClose(DataStoreFinder.getDataStore(params)) { consumer =>
        val store = consumer.getFeatureSource(sft.getTypeName)

        // admin user
        KafkaDataStoreMergeViewTest.auths.set(Seq("USER", "ADMIN").asJava)
        eventually(40, 100.millis)(WithClose(CloseableIterator(store.getFeatures.features))(_.toList) must
          containTheSameElementsAs(Seq(fUser, fAdmin)))

        // regular user
        KafkaDataStoreMergeViewTest.auths.set(Seq("USER").asJava)
        WithClose(CloseableIterator(store.getFeatures.features))(_.toList) mustEqual Seq(fUser)

        // unauthorized
        KafkaDataStoreMergeViewTest.auths.remove()
        WithClose(CloseableIterator(store.getFeatures.features))(_.toList) must beEmpty
      }
    }
  }
}

object KafkaDataStoreMergeViewTest {

  val auths = new ThreadLocal[java.util.List[String]]() {
    override def initialValue(): java.util.List[String] = Collections.emptyList[String]()
  }

  class TestAuthsProvider extends AuthorizationsProvider {
    override def getAuthorizations: java.util.List[String] = auths.get()
    override def configure(params: java.util.Map[String, _]): Unit = {}
  }
}
