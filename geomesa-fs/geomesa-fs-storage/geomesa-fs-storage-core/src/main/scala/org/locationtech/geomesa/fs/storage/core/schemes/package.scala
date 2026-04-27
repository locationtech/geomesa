/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.geotools.api.feature.simple.SimpleFeatureType

import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.collection.mutable.ArrayBuffer

package object schemes {

  // used to create upper bounds based on a prefix
  // note: we use 1 instead of 0 b/c 0 is not a valid char in postgres so breaks jdbc metadata filtering
  val ZeroChar = new String(Array[Byte](1), StandardCharsets.UTF_8)

  private[schemes] def attributeIndex(sft: SimpleFeatureType, name: String, binding: Option[Class[_]] = None): Int = {
    val index = sft.indexOf(name)
    require(index != -1, s"Attribute '$name' does not exist in schema '${sft.getTypeName}'")
    binding.foreach { b =>
      require(b.isAssignableFrom(sft.getDescriptor(index).getType.getBinding), s"Attribute '$name' is not a ${b.getSimpleName}")
    }
    index
  }

  case class SchemeOpts(name: String, opts: Map[String, String], multiOpts: Map[String, Seq[String]]) {
    def getSingle(k: String): Option[String] = {
      if (multiOpts.contains(k)) {
        throw new IllegalArgumentException(s"Expected a single value for '$k' but got: ${multiOpts(k).mkString(", ")}")
      }
      opts.get(k)
    }

    def getMulti(k: String): Seq[String] = multiOpts.get(k).orElse(opts.get(k).map(v => Seq(v))).getOrElse(Seq.empty)
  }

  object SchemeOpts {
    def apply(scheme: String): SchemeOpts = {
      val parts = scheme.split(":")
      val name = parts.head.toLowerCase(Locale.US)
      val opts = parts.drop(1).map(_.split("=", 2) match { case Array(k, v) => k.toLowerCase(Locale.US) -> v }).groupBy(_._1)
      val singleOpts = opts.collect { case (k, Array(v)) => k -> v._2 }
      val multiOpts = opts.collect { case (k, v) if v.lengthCompare(1) > 0 => k -> v.map(_._2).toSeq }
      SchemeOpts(name, singleOpts, multiOpts)
    }
  }

  /**
   * Class to merge overlapping ranges.
   *
   * Our bounds extraction does not produce any overlapping ranges, but once converted to partitions there
   * may be some overlap.
   */
  class RangeBuilder {

    private val ranges = ArrayBuffer.empty[PartitionRange]

    def +=(range: PartitionRange): Unit = ranges += range

    def result(): Seq[PartitionRange] = {
      val all = ranges.sorted(RangeBuilder.BoundsOrdering)
      if (all.lengthCompare(1) <= 0) {
        all.toSeq
      } else {
        // merge any overlapping ranges that resulted
        val result = Seq.newBuilder[PartitionRange]
        var current = all.head
        all.tail.foreach { range =>
          current.merge(range) match {
            case None =>
              result += current
              current = range
            case Some(merged) =>
              current = merged
          }
        }
        result += current
        result.result()
      }
    }
  }

  object RangeBuilder {
    private val BoundsOrdering = Ordering.by[PartitionRange, String](_.lower)
  }
}
