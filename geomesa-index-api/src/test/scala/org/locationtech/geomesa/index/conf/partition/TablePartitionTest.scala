/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.partition

import java.util.Date

import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.TestGeoMesaDataStore
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class TablePartitionTest extends Specification {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val ds = new TestGeoMesaDataStore(false)

  val sft = SimpleFeatureTypes.createType("table-partition-test", s"dtg:Date,*geom:Point:srid=4326;" +
      s"${Configs.TABLE_PARTITIONING}=${TimePartition.Name}")

  val timeToBin = BinnedTime.timeToBin(sft.getZ3Interval)

  def sf(date: Date): ScalaSimpleFeature = ScalaSimpleFeature.create(sft, "", date, null)

  "TablePartition" should {
    "check for partitioning" in {
      TablePartition.partitioned(sft) must beTrue
      TablePartition.partitioned(SimpleFeatureTypes.createType("table-partition-test2", "dtg:Date,*geom:Point:srid=4326")) must beFalse
    }

    "partition based on attributes" in {
      val partitionOption = TablePartition(ds, sft)
      partitionOption must beSome
      val partition = partitionOption.get
      val date = Converters.convert("2018-01-01T00:00:00.000Z", classOf[Date])
      Try(partition.partition(sf(date)).toShort) must beASuccessfulTry(timeToBin(date.getTime))
    }

    "recover an attribute from a partition" in {
      val partitionOption = TablePartition(ds, sft)
      partitionOption must beSome
      val partition = partitionOption.get
      val date = Converters.convert("2018-01-01T00:00:00.000Z", classOf[Date])
      val recovered = partition.recover(partition.partition(sf(date)))
      recovered must beAnInstanceOf[Date]
      timeToBin(recovered.asInstanceOf[Date].getTime) mustEqual timeToBin(date.getTime)
    }

    "extract partitions from filters" in {
      val filter = ECQL.toFilter("dtg during 2018-01-01T00:00:00.000Z/2018-02-01T00:00:00.000Z")
      val min = timeToBin(Converters.convert("2018-01-01T00:00:00.000Z", classOf[Date]).getTime)
      val max = timeToBin(Converters.convert("2018-02-01T00:00:00.000Z", classOf[Date]).getTime)
      val partitionOption = TablePartition(ds, sft)
      partitionOption must beSome
      val partition = partitionOption.get
      val partitionsOption = partition.partitions(filter)
      partitionsOption must beSome
      partitionsOption.get.map(_.toShort) must containTheSameElementsAs(Seq.range(min, max + 1).map(_.toShort))
    }

    "extract partitions from disjoint filters" in {
      val filter = ECQL.toFilter("dtg < '2018-01-01T00:00:00.000Z' AND dtg > '2018-02-01T00:00:00.000Z'")
      val partitionOption = TablePartition(ds, sft)
      partitionOption must beSome
      val partition = partitionOption.get
      val partitionsOption = partition.partitions(filter)
      partitionsOption must beSome
      partitionsOption.get must beEmpty
    }

    "extract nothing from non-selective filters" in {
      val filter = ECQL.toFilter("bbox(geom,-10,-10,10,10)")
      val partitionOption = TablePartition(ds, sft)
      partitionOption must beSome
      val partition = partitionOption.get
      val partitionsOption = partition.partitions(filter)
      partitionsOption must beNone
    }
  }
}
