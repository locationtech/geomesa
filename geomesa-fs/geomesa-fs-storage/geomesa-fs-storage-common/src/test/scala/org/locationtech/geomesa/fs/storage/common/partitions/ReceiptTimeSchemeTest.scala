/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.PartitionRange
import org.locationtech.geomesa.fs.storage.api.PartitionSchemeFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class ReceiptTimeSchemeTest extends SpecificationWithJUnit {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val sf = ScalaSimpleFeature.create(sft, "1", "test", 10, "2017-02-03T10:15:30Z", "POINT (10 10)")

  "ReceiptTimeScheme" should {

    "load from conf with named datetime scheme" in {
      val scheme = PartitionSchemeFactory.load(sft, "receipt-time:buffer=60 minutes:datetime-scheme=daily")
      scheme must beAnInstanceOf[ReceiptTimeScheme]
      scheme.asInstanceOf[ReceiptTimeScheme].buffer mustEqual Duration(60, TimeUnit.MINUTES)
      scheme.asInstanceOf[ReceiptTimeScheme].dtg mustEqual "dtg"
      scheme.asInstanceOf[ReceiptTimeScheme].pattern mustEqual "yyyy/MM/dd"
    }

    "load from conf with configured datetime scheme" in {
      val conf =
        """{
          |  scheme = "receipt-time"
          |  options = {
          |    buffer = "60 minutes"
          |    datetime-scheme = "datetime"
          |    datetime-format = "yyyy"
          |    step-unit = "years"
          |    step = 1
          |  }
          |}
        """.stripMargin

      sft.setScheme(conf)
      val names = sft.removeScheme().fold(null: String)(_.headOption.orNull)
      names must not(beNull)
      val scheme = PartitionSchemeFactory.load(sft, names)
      scheme must beAnInstanceOf[ReceiptTimeScheme]
      scheme.asInstanceOf[ReceiptTimeScheme].buffer mustEqual Duration(60, TimeUnit.MINUTES)
      scheme.asInstanceOf[ReceiptTimeScheme].dtg mustEqual "dtg"
      scheme.asInstanceOf[ReceiptTimeScheme].pattern mustEqual "yyyy"
    }

    "buffer filters appropriately" in {
      val ps = PartitionSchemeFactory.load(sft, "receipt-time:buffer=60 minutes:datetime-scheme=daily")

      val filtersAndResults = Seq(
        ("dtg == '2024-01-02T00:01:00.000Z'", Seq(PartitionRange(ps.name, "2024/01/01", "2024/01/03"))),
        ("dtg == '2024-01-02T01:01:00.000Z'", Seq(PartitionRange(ps.name, "2024/01/02", "2024/01/03"))),
        ("dtg between '2024-01-02T00:00:00.000Z' and '2024-01-02T23:59:59.999Z'", Seq(PartitionRange(ps.name, "2024/01/01", "2024/01/04"))),
        ("dtg >= '2024-01-02T00:00:00.000Z' and dtg <= '2024-01-02T23:59:59.999Z'", Seq(PartitionRange(ps.name, "2024/01/01", "2024/01/04"))),
        ("dtg >= '2024-01-02T00:00:00.000Z' and dtg < '2024-01-03T00:00:00.000Z'", Seq(PartitionRange(ps.name, "2024/01/01", "2024/01/04"))),
        ("dtg during 2024-01-02T00:00:00.000Z/2024-01-03T00:00:00.000Z", Seq(PartitionRange(ps.name, "2024/01/01", "2024/01/04"))),
        ("dtg during 2024-01-02T12:00:00.000Z/2024-01-02T13:00:00.000Z", Seq(PartitionRange(ps.name, "2024/01/02", "2024/01/03"))),
      )
      foreach(filtersAndResults) { case (filter, expected) =>
        val ecql = ECQL.toFilter(filter)
        val intersecting = ps.getIntersectingPartitions(ecql).orNull
        intersecting must not(beNull)

        intersecting.flatMap(_.bounds) must containTheSameElementsAs(expected)
        // verify that we don't remove the date filter from the simplified filter
        foreach(intersecting.map(_.filter.orNull))(_ mustEqual ecql)
      }
    }

    "buffer inverted filters appropriately" in {
      val ps = PartitionSchemeFactory.load(sft, "receipt-time:buffer=60 minutes:datetime-scheme=daily")

      val filters = Seq(
        "not(dtg during 2024-01-02T12:00:00.000Z/2024-01-02T13:00:00.000Z)",
        "not(dtg between '2024-01-02T12:00:00.000Z' and '2024-01-02T12:59:59.999Z')"
      )
      foreach(filters) { filter =>
        val ecql = ECQL.toFilter(filter)
        ps.getIntersectingPartitions(ecql) must beNone
      }
    }
  }
}
