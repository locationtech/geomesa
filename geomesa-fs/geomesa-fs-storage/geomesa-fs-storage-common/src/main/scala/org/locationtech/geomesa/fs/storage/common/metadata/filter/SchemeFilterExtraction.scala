/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata.filter

import com.typesafe.scalalogging.AnyLogging
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionBounds, PartitionFilter}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{AttributeBounds, SpatialBounds}
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, StorageMetadata}
import org.locationtech.geomesa.fs.storage.common.metadata.filter.SchemeFilterExtraction.SchemeFilter
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey

trait SchemeFilterExtraction extends AnyLogging {

  this: StorageMetadata =>

  import scala.collection.JavaConverters._

  protected def getFilters(filter: Filter): Seq[SchemeFilter] = {
    val iter = schemes.iterator
    var start: Option[Seq[PartitionFilter]] = None
    while (start.isEmpty && iter.hasNext) {
      start = iter.next().getIntersectingPartitions(filter)
    }
    val extracted = start.map { ranges =>
      val filters = ranges.flatMap { range =>
        range.bounds.map { bound =>
          SchemeFilter(range.filter, Seq(bound), Seq.empty, Seq.empty)
        }
      }
      iter.foldLeft(filters) { case (filters, scheme) => addPartition(scheme, filters) }.map { fileTableFilter =>
        val spatialBounds = fileTableFilter.filter.fold(Seq.empty[SpatialBounds])(extractGeometries)
        val attributeBounds = fileTableFilter.filter.fold(Seq.empty[AttributeBounds])(extractAttributes)
        fileTableFilter.copy(spatialBoundsOr = spatialBounds, attributeBoundsOr = attributeBounds)
      }
    }
    val filters = extracted.getOrElse(Seq(SchemeFilter(Some(filter), Seq.empty, extractGeometries(filter), extractAttributes(filter))))
    logger.debug(s"Extracted filters from ${ECQL.toCQL(filter)}:\n  ${filters.mkString("\n  ")}")
    filters
  }

  private def addPartition(scheme: PartitionScheme, filters: Seq[SchemeFilter]): Seq[SchemeFilter] = {
    val result = Seq.newBuilder[SchemeFilter]
    filters.foreach { filesTableFilter =>
      filesTableFilter.filter.foreach { f =>
        scheme.getIntersectingPartitions(f) match {
          case None => result += filesTableFilter
          case Some(ranges) =>
            ranges.foreach { range =>
              range.bounds.foreach { bound =>
                result += filesTableFilter.copy(range.filter, filesTableFilter.partitionsAnd :+ bound)
              }
            }
        }
      }
    }
    result.result()
  }

  private def extractGeometries(filter: Filter): Seq[SpatialBounds] = {
    sft.getAttributeDescriptors.asScala.toSeq
      .filter(_ == sft.getGeometryDescriptor) // TODO non-default geom bounds
      .flatMap { d =>
        FilterHelper.extractGeometries(filter, d.getLocalName).values.flatMap { g =>
          SpatialBounds(sft.indexOf(d.getLocalName), g.getEnvelopeInternal)
        }
      }
  }

  private def extractAttributes(filter: Filter): Seq[AttributeBounds] = {
    sft.getAttributeDescriptors.asScala.toSeq
      .filter(_ => false) // TODO attribute bounds
      .flatMap { d =>
        FilterHelper.extractAttributeBounds(filter, d.getLocalName, d.getType.getBinding).values.map { b =>
          val lower = b.lower.value.map(AttributeIndexKey.typeEncode).getOrElse("")
          val upper = b.upper.value.map(AttributeIndexKey.typeEncode).getOrElse("zzz")
          AttributeBounds(sft.indexOf(d.getLocalName), lower, upper)
        }
      }
  }
}

object SchemeFilterExtraction {

  case class SchemeFilter(
    filter: Option[Filter],
    partitionsAnd: Seq[PartitionBounds],
    spatialBoundsOr: Seq[SpatialBounds],
    attributeBoundsOr: Seq[AttributeBounds]
  )
}
