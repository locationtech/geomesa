/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.date

import java.time.ZonedDateTime
import java.util.Date

import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

object DateIndexKeySpace extends DateIndexKeySpace

/**
  * Encodes dates as lexicoded longs. Null values are encoded as Long.MinValue
  */
trait DateIndexKeySpace extends IndexKeySpace[Seq[Bounds[ZonedDateTime]], Long] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  // note: add 1 to exclude null values
  private val MinLowerBound = Long.MinValue + 1
  private val MaxUpperBound = Long.MaxValue

  override val name: String = "date"

  override val indexKeyByteLength: Int = 16

  override def supports(sft: SimpleFeatureType): Boolean = sft.getDtgField.isDefined

  override def toIndexKey(sft: SimpleFeatureType, lenient: Boolean): SimpleFeature => Seq[Long] = {
    val dtgIndex = sft.getDtgIndex.getOrElse(-1)
    feature => {
      val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
      if (dtg == null) { Seq(Long.MinValue) } else { Seq(dtg.getTime) }
    }
  }

  override def toIndexKeyBytes(sft: SimpleFeatureType, lenient: Boolean): ToIndexKeyBytes = {
    val dtgIndex = sft.getDtgIndex.getOrElse(-1)
    (prefix, feature, suffix) => {
      val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
      val result = Array.ofDim[Byte](prefix.map(_.length).sum + suffix.length + 8)
      var i = 0
      prefix.foreach { p => System.arraycopy(p, 0, result, i, p.length); i += p.length }
      ByteArrays.writeOrderedLong(if (dtg == null) { Long.MinValue } else { dtg.getTime }, result, i)
      System.arraycopy(suffix, 0, result, i + 8, suffix.length)
      Seq(result)
    }
  }

  override def getIndexValues(sft: SimpleFeatureType, filter: Filter, explain: Explainer): Seq[Bounds[ZonedDateTime]] =
    FilterHelper.extractIntervals(filter, sft.getDtgField.get).values

  override def getRanges(values: Seq[Bounds[ZonedDateTime]], multiplier: Int): Iterator[ScanRange[Long]] = {
    values.iterator.map { bounds =>
      if (bounds.isEquals) {
        SingleRowRange(bounds.lower.value.get.toInstant.toEpochMilli)
      } else {
        val lower = bounds.lower.value.map { v =>
          val millis = v.toInstant.toEpochMilli
          if (bounds.lower.inclusive) { millis } else { millis + 1L }
        }
        val upper = bounds.upper.value.map { v =>
          val millis = v.toInstant.toEpochMilli
          if (bounds.upper.inclusive) { millis + 1L } else { millis }
        }
        BoundedRange(lower.getOrElse(MinLowerBound), upper.getOrElse(MaxUpperBound))
      }
    }
  }

  override def getRangeBytes(ranges: Iterator[ScanRange[Long]],
                             prefixes: Seq[Array[Byte]],
                             tier: Boolean): Iterator[ByteRange] = {
    if (prefixes.isEmpty) {
      ranges.map {
        case SingleRowRange(row)  => SingleRowByteRange(ByteArrays.toOrderedBytes(row))
        case BoundedRange(lo, hi) => BoundedByteRange(ByteArrays.toOrderedBytes(lo), ByteArrays.toOrderedBytes(hi))
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    } else {
      ranges.flatMap {
        case SingleRowRange(row) =>
          val bytes = ByteArrays.toOrderedBytes(row)
          prefixes.map(p => SingleRowByteRange(ByteArrays.concat(p, bytes)))

        case BoundedRange(lo, hi) =>
          val lower = ByteArrays.toOrderedBytes(lo)
          val upper = ByteArrays.toOrderedBytes(hi)
          prefixes.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

        case r =>
          throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    }
  }

  override def useFullFilter(values: Option[Seq[Bounds[ZonedDateTime]]],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = false
}
