/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf.splitter

import org.junit.runner.RunWith
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey}
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays
<<<<<<< HEAD
<<<<<<< HEAD
import org.locationtech.geomesa.utils.text.DateParsing
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
import org.locationtech.geomesa.utils.text.DateParsing
>>>>>>> bc50eb4be1 (GEOMESA-3297 Support for tiered date index pre-splits (#2996))
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.nio.charset.StandardCharsets
<<<<<<< HEAD
<<<<<<< HEAD
import java.util.Date
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
import java.util.Date
>>>>>>> bc50eb4be1 (GEOMESA-3297 Support for tiered date index pre-splits (#2996))

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
      splits.toSeq.map(s => (ByteArrays.readShort(s).toInt, s(2).toInt, s.drop(3).sum.toInt)) must
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

    "produce correct string tiered date splits" in {
      val opts =
        "attr.myString.pattern:[A][B][C],attr.myString.pattern2:[B-Z]," +
        "attr.myString.date-range:2023-09-15/2023-09-16/4"
      val splits = splitter.getSplits(sft, attrString, opts)
      splits.length must be equalTo 29
      splits.toSeq must containAllOf(Seq.tabulate(25)(i => Array(('B' + i).toChar.toByte)))
      val dates =
        Seq("2023-09-15T00:00:00Z", "2023-09-15T06:00:00Z", "2023-09-15T12:00:00Z", "2023-09-15T18:00:00Z").map { d =>
          "ABC".getBytes(StandardCharsets.UTF_8) ++
              ByteArrays.ZeroByteArray ++
              AttributeIndexKey.encodeForQuery(DateParsing.parseDate(d), classOf[Date]).getBytes(StandardCharsets.UTF_8)
        }
      splits.toSeq must containAllOf(dates)
    }

<<<<<<< HEAD
    "produce correct string tiered date splits for all dates" in {
      foreach(Seq("2023-01-01", "2023-10-04", "2023-11-11", "2023-12-31")) { date =>
        val opts =
          "attr.myString.pattern:[A][B][C],attr.myString.pattern2:[B-Z]," +
              s"attr.myString.date-range:2023-09-15/$date/4"
        splitter.getSplits(sft, attrString, opts) must not(throwAn[Exception])
      }
      foreach(Seq("2023-01-32", "2023-13-04", "2023-00-11", "2023-12-00")) { date =>
        val opts =
          "attr.myString.pattern:[A][B][C],attr.myString.pattern2:[B-Z]," +
              s"attr.myString.date-range:2023-09-15/$date/4"
        splitter.getSplits(sft, attrString, opts) must throwAn[Exception]
      }
    }

=======
>>>>>>> bc50eb4be1 (GEOMESA-3297 Support for tiered date index pre-splits (#2996))
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

    "produce correct negative int splits" in {
      val opts = "attr.myInt.pattern:[-][0-9]"
      val splits = splitter.getSplits(sft, attrInt, opts)
      splits must haveLength(10)
      // note: lexicoded values in hex
      splits.map(new String(_, StandardCharsets.UTF_8)).toSeq mustEqual
          (-9 to 0).map(AttributeIndexKey.encodeForQuery(_, classOf[Integer]))
    }

    "produce correct union negative int splits" in {
      val opts = "attr.myInt.pattern:[-][8-95-6]"
      val splits = splitter.getSplits(sft, attrInt, opts)
      splits must haveLength(4)
      // note: lexicoded values in hex
      splits.map(new String(_, StandardCharsets.UTF_8)).toSeq mustEqual
          Seq("7ffffff7", "7ffffff8", "7ffffffa", "7ffffffb")
    }

    "produce correct tiered negative int splits" in {
      val opts = "attr.myInt.pattern:[-][0-1][2-3]"
      val splits = splitter.getSplits(sft, attrInt, opts)
      splits must haveLength(4)
      // note: lexicoded values in hex
      splits.map(new String(_, StandardCharsets.UTF_8)).toSeq mustEqual
          Seq("7ffffff3", "7ffffff4", "7ffffffd", "7ffffffe")
    }

    "reject invalid int splits" in {
      val opts = "attr.myInt.pattern:[A-Z]"
      splitter.getSplits(sft, attrInt, opts) must throwAn[IllegalArgumentException]
    }
  }
}
