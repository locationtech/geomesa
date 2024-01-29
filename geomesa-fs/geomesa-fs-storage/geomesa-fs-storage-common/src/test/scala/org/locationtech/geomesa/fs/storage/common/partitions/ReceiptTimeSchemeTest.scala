/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.PartitionSchemeFactory
import org.locationtech.geomesa.fs.storage.common.StorageSerialization
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class ReceiptTimeSchemeTest extends Specification {

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-02-03T10:15:30Z", "POINT (10 10)")

  "ReceiptTimeScheme" should {

    "load from conf with named datetime scheme" >> {
      val conf = """{ scheme = "receipt-time", options = { buffer = "60 minutes", datetime-scheme = "daily" }}"""
      val scheme = PartitionSchemeFactory.load(sft, StorageSerialization.deserialize(conf))
      scheme must beAnInstanceOf[ReceiptTimeScheme]
      scheme.asInstanceOf[ReceiptTimeScheme].buffer mustEqual Duration(60, TimeUnit.MINUTES)
      val delegate = scheme.asInstanceOf[ReceiptTimeScheme].delegate
      delegate.dtg mustEqual "dtg"
      delegate.pattern mustEqual "yyyy/MM/dd"
    }

    "load from conf with configured datetime scheme" >> {
      val conf =
        """
          | {
          |   scheme = "receipt-time"
          |   options = {
          |     buffer = "60 minutes"
          |     datetime-scheme = "datetime"
          |     datetime-format = "yyyy"
          |     step-unit = "years"
          |     step = 1
          |   }
          | }
        """.stripMargin

      val scheme = PartitionSchemeFactory.load(sft, StorageSerialization.deserialize(conf))
      scheme must beAnInstanceOf[ReceiptTimeScheme]
      scheme.asInstanceOf[ReceiptTimeScheme].buffer mustEqual Duration(60, TimeUnit.MINUTES)
      val delegate = scheme.asInstanceOf[ReceiptTimeScheme].delegate
      delegate.dtg mustEqual "dtg"
      delegate.pattern mustEqual "yyyy"
    }

    "buffer filters appropriately" >> {
      val conf = """{ scheme = "receipt-time", options = { buffer = "60 minutes", datetime-scheme = "daily" }}"""
      val scheme = PartitionSchemeFactory.load(sft, StorageSerialization.deserialize(conf))

      val filtersAndResults = Seq(
        ("dtg == '2024-01-02T00:01:00.000Z'", Seq("2024/01/01", "2024/01/02")),
        ("dtg == '2024-01-02T01:01:00.000Z'", Seq("2024/01/02")),
        ("dtg between '2024-01-02T00:00:00.000Z' and '2024-01-02T23:59:59.999Z'", Seq("2024/01/01", "2024/01/02", "2024/01/03")),
        ("dtg >= '2024-01-02T00:00:00.000Z' and dtg <= '2024-01-02T23:59:59.999Z'", Seq("2024/01/01", "2024/01/02", "2024/01/03")),
        ("dtg >= '2024-01-02T00:00:00.000Z' and dtg < '2024-01-03T00:00:00.000Z'", Seq("2024/01/01", "2024/01/02", "2024/01/03")),
        ("dtg during 2024-01-02T00:00:00.000Z/2024-01-03T00:00:00.000Z", Seq("2024/01/01", "2024/01/02", "2024/01/03")),
        ("dtg during 2024-01-02T12:00:00.000Z/2024-01-02T13:00:00.000Z", Seq("2024/01/02")),
      )
      foreach(filtersAndResults) { case (filter, expected) =>
        val ecql = ECQL.toFilter(filter)
        scheme.getIntersectingPartitions(ecql) must beSome(containTheSameElementsAs(expected))
        val simplified = scheme.getSimplifiedFilters(ecql).orNull
        simplified must not(beNull)
        simplified.flatMap(_.partitions) must containTheSameElementsAs(expected)
        // verify that we don't remove the date filter from the simplified filter
        foreach(simplified.map(_.filter))(_ mustEqual ecql)
      }
    }

    "buffer inverted filters appropriately" >> {
      val conf = """{ scheme = "receipt-time", options = { buffer = "60 minutes", datetime-scheme = "daily" }}"""
      val scheme = PartitionSchemeFactory.load(sft, StorageSerialization.deserialize(conf))

      val filters = Seq(
        "not(dtg during 2024-01-02T12:00:00.000Z/2024-01-02T13:00:00.000Z)",
        "not(dtg between '2024-01-02T12:00:00.000Z' and '2024-01-02T12:59:59.999Z')"
      )
      foreach(filters) { filter =>
        val ecql = ECQL.toFilter(filter)
        scheme.getIntersectingPartitions(ecql) must beNone
      }
    }
  }
}
