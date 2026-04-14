/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.metadata.filter

import com.typesafe.scalalogging.AnyLogging
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.PartitionRange
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{AttributeBounds, SpatialBounds}
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, StorageMetadata}
import SchemeFilterExtraction._
import org.locationtech.geomesa.fs.storage.core.metadata.StorageMetadata
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.jts.geom.Envelope

/**
 * Mixin trait for matching partitions against a CQL filter
 */
trait SchemeFilterExtraction extends AnyLogging {

  this: StorageMetadata =>

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  protected def getFilters(filter: Filter): Seq[SchemeFilter] = {
    val iter = schemes.iterator
    val start = iter.map(_.getRangesForFilter(filter)).collectFirst { case Some(f) => f }
    val filters = start match {
      case None =>
        // filter did not constrain partitions at all
        Seq(SchemeFilter(filter, Seq.empty, extractGeometries(filter), extractAttributes(filter)))

      case Some(ranges) =>
        // set up the initial scheme filters, without any spatial/attribute bounds
        val initialRanges = ranges.map(r => Seq(r))
        // add in the remaining partitions
        val permutations = iter.foldLeft(initialRanges) { case (ranges, scheme) => addPartition(scheme, ranges, filter) }
        // add in the spatial/attribute bounds based on the remaining filter for each scheme filter
        val spatialBounds = extractGeometries(filter)
        val attributeBounds = extractAttributes(filter)
        permutations.map(ranges => SchemeFilter(filter, ranges, spatialBounds, attributeBounds))
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

  private def extractGeometries(filter: Filter): Seq[SpatialOr] = {
    sft.spatialBounds().flatMap { i =>
      val ors = FilterHelper.extractGeometries(filter, sft.getDescriptor(i).getLocalName).values.flatMap { g =>
        SpatialBound(g.getEnvelopeInternal)
      }
      if (ors.isEmpty) { None } else { Some(SpatialOr(i, ors)) }
    }
  }

  private def extractAttributes(filter: Filter): Seq[AttributeOr] = {
    sft.nonSpatialBounds().flatMap { i =>
      val d = sft.getDescriptor(i)
      val ors = FilterHelper.extractAttributeBounds(filter, d.getLocalName, d.getType.getBinding).values.map { b =>
        val lower = b.lower.value.map(AttributeIndexKey.typeEncode).getOrElse("")
        val upper = b.upper.value.map(AttributeIndexKey.typeEncode).getOrElse("zzz")
        AttributeBound(lower, upper)
      }
      if (ors.isEmpty) { None } else { Some(AttributeOr(i, ors)) }
    }
  }
}

object SchemeFilterExtraction {

  /**
   * Predicates extracted from a filter, based on the partition scheme
   *
   * @param filter remaining ECQL filter that isn't accounted for with the other bounds
   * @param partitions list of partition bound predicates (implicit AND between each element in the seq)
   * @param spatialBounds list of spatial bound predicates (implicit AND between each element in the seq)
   * @param attributeBounds list of attribute bound predicates (implicit AND between each element in the seq)
   */
  case class SchemeFilter(
    filter: Filter,
    partitions: Seq[PartitionRange],
    spatialBounds: Seq[SpatialOr],
    attributeBounds: Seq[AttributeOr]
  )

  case class SpatialOr(attribute: Int, bounds: Seq[SpatialBound])
  case class AttributeOr(attribute: Int, bounds: Seq[AttributeBound])

  case class SpatialBound(xmin: Double, ymin: Double, xmax: Double, ymax: Double) {
    def intersects(other: SpatialBounds): Boolean =
      other.xmin <= xmax && other.xmax >= xmin && other.ymin <= ymax && other.ymax >= ymin
  }

  object SpatialBound {

    /**
     * Converts an envelope to a bounds, handling 'null' (empty) envelopes
     *
     * @param env envelope
     * @return
     */
    def apply(env: Envelope): Option[SpatialBound] =
      if (env.isNull) { None } else { Some(SpatialBound(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)) }
  }

  case class AttributeBound(lower: String, upper: String) {
    def intersects(other: AttributeBounds): Boolean = other.lower <= upper && other.upper >= lower
  }
}
