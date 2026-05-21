/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package metadata

import com.typesafe.scalalogging.AnyLogging
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.core.PartitionScheme
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.{ColumnBounds, XZ2Encoder, Z2Encoder}
import org.locationtech.geomesa.fs.storage.core.metadata.SchemeFilterExtraction._
import org.locationtech.jts.geom.{Geometry, Point}

/**
 * Mixin trait for matching partitions against a CQL filter
 */
trait SchemeFilterExtraction extends AnyLogging {

  this: StorageMetadata =>

  protected def getFilters(filter: Filter): Seq[SchemeFilter] = {
    val iter = schemes.iterator
    val start = iter.map(_.getRangesForFilter(filter)).collectFirst { case Some(f) => f }
    val filters = start match {
      case None =>
        // filter did not constrain partitions at all
        Seq(SchemeFilter(filter, Seq.empty, extractAttributes(filter)))

      case Some(ranges) =>
        // set up the initial scheme filters, without any spatial/attribute bounds
        val initialRanges = ranges.map(r => Seq(r))
        // add in the remaining partitions
        val permutations = iter.foldLeft(initialRanges) { case (ranges, scheme) => addPartition(scheme, ranges, filter) }
        // add in the attribute bounds based on the remaining filter for each scheme filter
        val attributeBounds = extractAttributes(filter)
        permutations.map(ranges => SchemeFilter(filter, ranges, attributeBounds))
    }

    logger.debug(s"Extracted filters from ${ECQL.toCQL(filter)}:\n  ${filters.mkString("\n  ")}")
    filters
  }

  // add the next partition to the scheme filter
  private def addPartition(scheme: PartitionScheme, ranges: Seq[Seq[PartitionRange]], filter: Filter): Seq[Seq[PartitionRange]] = {
    val result = Seq.newBuilder[Seq[PartitionRange]]
    ranges.foreach { filesTableFilter =>
      scheme.getRangesForFilter(filter) match {
        case None => result += filesTableFilter
        case Some(ranges) =>
          ranges.foreach { range =>
            result += filesTableFilter :+ range
          }
      }
    }
    result.result()
  }

  private def extractAttributes(filter: Filter): Seq[ColumnOr] = {
    sft.columnBounds().flatMap { i =>
      val d = sft.getDescriptor(i)
      if (classOf[Geometry].isAssignableFrom(d.getType.getBinding)) {
        val ors = FilterHelper.extractGeometries(filter, sft.getDescriptor(i).getLocalName).values.flatMap { g =>
          val env = g.getEnvelopeInternal
          if (d.getType.getBinding == classOf[Point]) {
            // TODO make max ranges configurable
            Z2Encoder.sfc.ranges(Seq((env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)), maxRanges = Some(8)).map { range =>
              ColumnBound(Z2Encoder.encode(range.lower), Z2Encoder.encode(range.upper))
            }
          } else {
            XZ2Encoder.sfc.ranges((env.getMinX, env.getMinY, env.getMaxX, env.getMaxY), Some(8)).map { range =>
              ColumnBound(XZ2Encoder.encode(range.lower), XZ2Encoder.encode(range.upper))
            }
          }
        }
        if (ors.isEmpty) { None } else { Some(ColumnOr(i, ors)) }
      } else {
        val ors = FilterHelper.extractAttributeBounds(filter, d.getLocalName, d.getType.getBinding).values.map { b =>
          val lower = b.lower.value.map(StorageMetadata.TypeRegistry.encode).getOrElse("")
          val upper = b.upper.value.map(StorageMetadata.TypeRegistry.encode).getOrElse("zzz")
          ColumnBound(lower, upper)
        }
        if (ors.isEmpty) { None } else { Some(ColumnOr(i, ors)) }
      }
    }
  }
}

object SchemeFilterExtraction {

  /**
   * Predicates extracted from a filter, based on the partition scheme
   *
   * @param filter remaining ECQL filter that isn't accounted for with the other bounds
   * @param partitions list of partition bound predicates (implicit AND between each element in the seq)
   * @param columnBounds list of column bound predicates (implicit AND between each element in the seq)
   */
  case class SchemeFilter(
    filter: Filter,
    partitions: Seq[PartitionRange],
    columnBounds: Seq[ColumnOr]
  )

  case class ColumnOr(attribute: Int, bounds: Seq[ColumnBound])

  case class ColumnBound(lower: String, upper: String) {
    def intersects(other: ColumnBounds): Boolean = other.lower <= upper && other.upper >= lower
  }
}
