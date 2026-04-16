/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/


package org.locationtech.geomesa.filter.function

import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

class BucketHashFunctionTest extends SpecificationWithJUnit {

  import FilterHelper.ff

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("test",
    "name:String,age:Int,time:Long,weight:Float,precision:Double,dtg:Date,bytes:Bytes,uuid:UUID")

  val nullValues = Seq.fill[AnyRef](sft.getAttributeCount)(null).asJava

  "BucketHashFunction" should {
    "hash strings" in {
      // string	hashBytes(utf8Bytes(v))	iceberg ￫ 1210000089
      val sf = new SimpleFeatureImpl(nullValues, sft, new FeatureIdImpl("1"))
      sf.setAttribute("name", "iceberg")
      ff.function("bucketHash", ff.property("name"), ff.literal(8)).evaluate(sf) mustEqual Int.box(1210000089 % 8)
    }
    "hash ints" in {
      // int	hashLong(long(v)) [1]	34 ￫ 2017239379
      val sf = new SimpleFeatureImpl(nullValues, sft, new FeatureIdImpl("1"))
      sf.setAttribute("age", "34")
      ff.function("bucketHash", ff.property("age"), ff.literal(8)).evaluate(sf) mustEqual Int.box(2017239379 % 8)
    }
    "hash longs" in {
      // long	hashBytes(littleEndianBytes(v))	34L ￫ 2017239379
      val sf = new SimpleFeatureImpl(nullValues, sft, new FeatureIdImpl("1"))
      sf.setAttribute("time", "34")
      ff.function("bucketHash", ff.property("time"), ff.literal(8)).evaluate(sf) mustEqual Int.box(2017239379 % 8)
    }
    "hash floats" in {
      // float	hashLong(doubleToLongBits(double(v)) [5]	1.0F ￫ -142385009, 0.0F ￫ 1669671676, -0.0F ￫ 1669671676
      val sf = new SimpleFeatureImpl(nullValues, sft, new FeatureIdImpl("1"))
      sf.setAttribute("weight", "1")
      ff.function("bucketHash", ff.property("weight"), ff.literal(8)).evaluate(sf) mustEqual Int.box((-142385009 & Int.MaxValue) % 8)
    }
    "hash doubles" in {
      // double	hashLong(doubleToLongBits(v)) [5]	1.0D ￫ -142385009, 0.0D ￫ 1669671676, -0.0D ￫ 1669671676
      val sf = new SimpleFeatureImpl(nullValues, sft, new FeatureIdImpl("1"))
      sf.setAttribute("precision", "1")
      ff.function("bucketHash", ff.property("precision"), ff.literal(8)).evaluate(sf) mustEqual Int.box((-142385009 & Int.MaxValue) % 8)
    }
    "hash dates" in {
      // timestamp	hashLong(microsecsFromUnixEpoch(v))	2017-11-16T22:31:08 ￫ -2047944441, 2017-11-16T22:31:08.000001 ￫ -1207196810
      val sf = new SimpleFeatureImpl(nullValues, sft, new FeatureIdImpl("1"))
      sf.setAttribute("dtg", "2017-11-16T22:31:08")
      ff.function("bucketHash", ff.property("dtg"), ff.literal(8)).evaluate(sf) mustEqual Int.box((-2047944441 & Int.MaxValue) % 8)
    }
    "hash byte arrays" in {
      // binary	hashBytes(v)	00 01 02 03 ￫ -188683207
      val sf = new SimpleFeatureImpl(nullValues, sft, new FeatureIdImpl("1"))
      sf.setAttribute("bytes", Array[Byte](0, 1, 2, 3))
      ff.function("bucketHash", ff.property("bytes"), ff.literal(8)).evaluate(sf) mustEqual Int.box((-188683207 & Int.MaxValue) % 8)
    }
    "hash uuids" in {
      // uuid	hashBytes(uuidBytes(v)) [4]	f79c3e09-677c-4bbd-a479-3f349cb785e7 ￫ 1488055340
      val sf = new SimpleFeatureImpl(nullValues, sft, new FeatureIdImpl("1"))
      sf.setAttribute("uuid", "f79c3e09-677c-4bbd-a479-3f349cb785e7")
      ff.function("bucketHash", ff.property("uuid"), ff.literal(8)).evaluate(sf) mustEqual Int.box(1488055340 % 8)
    }
  }
}
