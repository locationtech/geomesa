/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.splitter

import java.nio.charset.StandardCharsets

import com.google.common.primitives.Shorts
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DefaultSplitterTest extends Specification {

  "Default splitter" should {

    val sft = SimpleFeatureTypes.createType("test",
      "myString:String:index=true,myInt:Int:index=true,dtg:Date,*geom:Point:srid=4326")
    val splitter = new DefaultSplitter

    val id = GeoMesaFeatureIndex.identifier(IdIndex.name, IdIndex.version, Seq.empty)
    val z3 = GeoMesaFeatureIndex.identifier(Z3Index.name, Z3Index.version, Seq("geom", "dtg"))
    val attrString = GeoMesaFeatureIndex.identifier(AttributeIndex.name, AttributeIndex.version, Seq("myString"))
    val attrInt = GeoMesaFeatureIndex.identifier(AttributeIndex.JoinIndexName, AttributeIndex.version, Seq("myInt"))

    "produce correct id splits" in {
      val opts = "id.pattern:[A-Z]"
      val splits = splitter.getSplits(sft, id, opts)
      splits must haveLength(26)
      splits.map(new String(_, StandardCharsets.UTF_8)).mkString mustEqual "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    }

    "produce correct tiered id splits" in {
      val opts = "id.pattern:[A-B][A-C]"
      val splits = splitter.getSplits(sft, id, opts)
      splits must haveLength(6)
      splits.map(new String(_, StandardCharsets.UTF_8)).mkString(",") mustEqual "AA,AB,AC,BA,BB,BC"
    }

    "produce correct aggregated id splits" in {
      val opts = "id.pattern:[0-9],id.pattern2:[8-8][0-9]"
      val splits = splitter.getSplits(sft, id, opts)
      splits.length must be equalTo 20
      splits.map(new String(_, StandardCharsets.UTF_8)).toSeq mustEqual
          (0 to 9).map(_.toString) ++ (0 to 9).map(i => s"8$i")
    }

    "produce correct z3 splits" in {
      val opts = s"z3.min:2017-01-01,z3.max:2017-01-10,z3.bits:4"
      val splits = splitter.getSplits(sft, z3, opts)
      splits must haveLength(32)
      splits.toSeq.map(s => (Shorts.fromByteArray(s.take(2)).toInt, s(2).toInt, s.drop(3).sum.toInt)) must
          containTheSameElementsAs(Seq(2452, 2453).flatMap(w => Range(0, 128, 8).map((w, _, 0))))
    }

    "produce correct string splits" in {
      val opts = "attr.myString.pattern:[A-Z]"
      val splits = splitter.getSplits(sft, attrString, opts)
      splits must haveLength(26)
      splits.map(new String(_, StandardCharsets.UTF_8)).mkString mustEqual "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    }

    "produce correct union string splits" in {
      val opts = "attr.myString.pattern:[A-Z0-9]"
      val splits = splitter.getSplits(sft, attrString, opts)
      splits must haveLength(36)
      splits.map(new String(_, StandardCharsets.UTF_8)).mkString mustEqual "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    }

    "produce correct tiered string splits" in {
      val opts = "attr.myString.pattern:[A-B][A-C]"
      val splits = splitter.getSplits(sft, attrString, opts)
      splits must haveLength(6)
      splits.map(new String(_, StandardCharsets.UTF_8)).mkString(",") mustEqual "AA,AB,AC,BA,BB,BC"
    }

    "produce correct aggregated string splits" in {
      val opts = "attr.myString.pattern:[0-9],attr.myString.pattern2:[8-8][0-9]"
      val splits = splitter.getSplits(sft, attrString, opts)
      splits.length must be equalTo 20
      splits.map(new String(_, StandardCharsets.UTF_8)).toSeq mustEqual
          (0 to 9).map(_.toString) ++ (0 to 9).map(i => s"8$i")
    }

    "produce correct int splits" in {
      val opts = "attr.myInt.pattern:[0-9]"
      val splits = splitter.getSplits(sft, attrInt, opts)
      splits.length must be equalTo 10
      // note: lexicoded values in hex
      splits.map(new String(_, StandardCharsets.UTF_8)).toSeq mustEqual (0 until 10).map(i => s"8000000$i")
    }

    "produce correct union int splits" in {
      val opts = "attr.myInt.pattern:[0-15-6]"
      val splits = splitter.getSplits(sft, attrInt, opts)
      splits must haveLength(4)
      // note: lexicoded values in hex
      splits.map(new String(_, StandardCharsets.UTF_8)).toSeq mustEqual
          Seq("80000000", "80000001", "80000005", "80000006")
    }

    "produce correct tiered int splits" in {
      val opts = "attr.myInt.pattern:[0-1][2-3]"
      val splits = splitter.getSplits(sft, attrInt, opts)
      splits must haveLength(4)
      // note: lexicoded values in hex
      splits.map(new String(_, StandardCharsets.UTF_8)).toSeq mustEqual
          Seq("80000002", "80000003", "8000000c", "8000000d")
    }

    "produce correct aggregated int splits" in {
      val opts = "attr.myInt.pattern:[0-9],attr.myInt.pattern2:[8-8][0-9]"
      val splits = splitter.getSplits(sft, attrInt, opts)
      splits.length must be equalTo 20
      // note: lexicoded values in hex
      splits.map(new String(_, StandardCharsets.UTF_8)).toSeq mustEqual
          (0 until 10).map(i => s"8000000$i") ++ (0 to 9).map(i => s"8000005$i")
    }

    "reject invalid int splits" in {
      val opts = "attr.myInt.pattern:[A-Z]"
      splitter.getSplits(sft, attrInt, opts) must throwAn[IllegalArgumentException]
    }
  }
}
