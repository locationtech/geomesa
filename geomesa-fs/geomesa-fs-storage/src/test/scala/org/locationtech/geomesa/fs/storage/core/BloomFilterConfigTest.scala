/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

class BloomFilterConfigTest extends SpecificationWithJUnit {

  private val sft =
    SimpleFeatureTypes.createType("blooms", "name:String,Code-Value:String,age:Int,tags:List[String],dtg:Date,*geom:Point:srid=4326")

  "BloomFilterConfig" should {

    "parse attributes and options" >> {
      BloomFilterConfig.parse(sft, "name:ndv=10000:fpp=0.05, age") mustEqual
        Seq(BloomFilterConfig("name", Some(10000L), Some(0.05)), BloomFilterConfig("age"))
      BloomFilterConfig.parse(sft, "") must beEmpty
    }

    "reject invalid configurations" >> {
      BloomFilterConfig.parse(sft, "missing") must throwAn[IllegalArgumentException]
      BloomFilterConfig.parse(sft, "geom") must throwAn[IllegalArgumentException]
      BloomFilterConfig.parse(sft, "tags") must throwAn[IllegalArgumentException]
      BloomFilterConfig.parse(sft, "name:ndv=lots") must throwAn[IllegalArgumentException]
      BloomFilterConfig.parse(sft, "name:fpp=2") must throwAn[IllegalArgumentException]
      BloomFilterConfig.parse(sft, "name:size=10") must throwAn[IllegalArgumentException]
    }

    "map to iceberg table properties by column name" >> {
      val configs = BloomFilterConfig.parse(sft, "name:ndv=10000:fpp=0.05,Code-Value")
      BloomFilterConfig.tableProperties(configs) mustEqual Map(
        "write.parquet.bloom-filter-enabled.column.name" -> "true",
        "write.parquet.bloom-filter-ndv.column.name" -> "10000",
        "write.parquet.bloom-filter-fpp.column.name" -> "0.05",
        "write.parquet.bloom-filter-enabled.column.code_value" -> "true",
      )
    }

    "be set and removed through the user data" >> {
      val copy = SimpleFeatureTypes.copy(sft)
      copy.setBloomFilters("name:ndv=100") must not(throwAn[Exception])
      copy.setBloomFilters("missing") must throwAn[IllegalArgumentException]
      copy.removeBloomFilters() mustEqual Seq(BloomFilterConfig("name", Some(100L)))
      copy.getUserData.containsKey(StorageKeys.BloomFiltersKey) must beFalse
      copy.removeBloomFilters() must beEmpty
    }
  }
}
