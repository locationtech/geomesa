/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils.bin

import java.io.ByteArrayOutputStream
import java.util.{Arrays, Date}

import org.locationtech.jts.geom.Point
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.bin.BinaryEncodeCallback.{ByteArrayCallback, ByteStreamCallback}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class BinSorterTest extends Specification {

  val spec = "name:String,dtg:Date,*geom:Point:srid=4326"
  val sft = SimpleFeatureTypes.createType(getClass.getSimpleName, spec)
  val seed = 10 // new Random().nextLong(); println("SEED " + seed)
  val r = new Random(seed)

  val features = (0 until r.nextInt(100) + 1).map { i =>
    val dtg = new Date(r.nextInt(999999))
    val name = s"name$i"
    val geom = s"POINT(40 6$i)"
    val sf = new ScalaSimpleFeature(sft, s"$i")
    sf.setAttributes(Array[AnyRef](name, dtg, geom))
    sf
  }

  val out = new ByteArrayOutputStream(16 * features.length)
  val callback = new ByteStreamCallback(out)
  features.foreach { f =>
    callback.apply(
      f.getAttribute("name").asInstanceOf[String].hashCode,
      f.getDefaultGeometry.asInstanceOf[Point].getY.toFloat,
      f.getDefaultGeometry.asInstanceOf[Point].getX.toFloat,
      f.getAttribute("dtg").asInstanceOf[Date].getTime)
  }
  val bin = out.toByteArray

  "BinAggregatingIterator" should {
    "quicksort" in {
      val bytes = Arrays.copyOf(bin, bin.length)
      BinSorter.quickSort(bytes, 0, bytes.length - 16, 16)
      val result = bytes.grouped(16).map(BinaryOutputEncoder.decode).map(_.dtg).toSeq
      forall(result.sliding(2).toSeq)(s => s.head must beLessThanOrEqualTo(s.drop(1).head))
    }
    "mergesort" in {
      val bytes = Arrays.copyOf(bin, bin.length).grouped(48).toSeq
      bytes.foreach(b => BinSorter.quickSort(b, 0, b.length - 16, 16))
      val result = BinSorter.mergeSort(bytes.iterator, 16).map {
        case (b, o) => BinaryOutputEncoder.decode(b.slice(o, o + 16)).dtg
      }
      forall(result.sliding(2).toSeq)(s => s.head must beLessThanOrEqualTo(s.drop(1).head))
    }
    "mergesort in place" in {
      val bytes = Arrays.copyOf(bin, bin.length).grouped(48)
      val (left, right) = (bytes.next(), bytes.next())
      // sort the left and right arrays
      BinSorter.quickSort(left, 0, left.length - 16, 16)
      BinSorter.quickSort(right, 0, right.length - 16, 16)
      val merged = BinSorter.mergeSort(left, right, 16)
      val result = merged.grouped(16).map(BinaryOutputEncoder.decode).map(_.dtg).toSeq
      forall(result.sliding(2).toSeq)(s => s.head must beLessThanOrEqualTo(s.drop(1).head))
    }
    "quicksort 24 byte records" in {
      val out = new ByteArrayOutputStream(24 * features.length)
      val callback = new ByteStreamCallback(out)
      features.foreach { f =>
        callback.apply(
          f.getAttribute("name").asInstanceOf[String].hashCode,
          f.getDefaultGeometry.asInstanceOf[Point].getY.toFloat,
          f.getDefaultGeometry.asInstanceOf[Point].getX.toFloat,
          f.getAttribute("dtg").asInstanceOf[Date].getTime,
          f.getAttribute("dtg").asInstanceOf[Date].getTime * 1000)
      }
      val bytes = out.toByteArray
      BinSorter.quickSort(bytes, 0, bytes.length - 24, 24)
      val result = bytes.grouped(24).map(BinaryOutputEncoder.decode).map(_.dtg).toSeq
      forall(result.sliding(2).toSeq)(s => s.head must beLessThanOrEqualTo(s.drop(1).head))
    }
    "quicksort edge cases" in {
      val maxLength = 8 // anything more than 8 takes too long to run
      val buffer = Array.ofDim[Byte](maxLength * 16)
      val bins = (1 to maxLength).map { i =>
        ByteArrayCallback.apply(s"name$i".hashCode, 0f, 0f, i * 1000)
        ByteArrayCallback.result
      }
      (1 to maxLength).foreach { i =>
        bins.slice(0, i).permutations.foreach { seq =>
          val right = i * 16 - 16
          seq.zipWithIndex.foreach { case (b, s) => System.arraycopy(b, 0, buffer, s * 16, 16) }
          BinSorter.quickSort(buffer, 0, right, 16)
          val result = buffer.take(right + 16).grouped(16).map(BinaryOutputEncoder.decode).map(_.dtg).toSeq
          result must haveLength(i)
          if (result.length > 1) {
            forall(result.sliding(2).toSeq)(s => s.head must beLessThanOrEqualTo(s.drop(1).head))
          }
        }
      }
      success
    }
  }
}
