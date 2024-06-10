/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult
import org.locationtech.geomesa.kafka.data.GeoMessageProcessorApiTest
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoMessageProcessorJavaTest extends Specification {

  import scala.collection.JavaConverters._

  lazy val sft = SimpleFeatureTypes.createType("KafkaGeoMessageTest", "name:String,*geom:Point:srid=4326")
  lazy val feature = ScalaSimpleFeature.create(sft, "test_id", "foo", "POINT(1 -1)")

  "GeoMessageProcessor" should {
    "work through the java api" in {
      val change = GeoMessage.change(feature)
      val del = GeoMessage.delete(feature.getID)
      val clear = GeoMessage.clear()

      val processor = new GeoMessageProcessorApiTest.TestProcessor()
      processor.consume(Seq(change, del, clear)) mustEqual BatchResult.Commit
      processor.added.asScala mustEqual Seq(change)
      processor.removed.asScala mustEqual Seq(del)
      processor.cleared mustEqual 1
    }
  }
}
