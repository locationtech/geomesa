/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.legacy

import java.nio.charset.StandardCharsets
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.{FilterHelper, FilterValues}
import org.locationtech.geomesa.index.api.WrappedFeature
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Attribute plus date composite index
  */
trait AttributeDateIndex[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W, R, C]
    extends AttributeShardedIndex[DS, F, W, R, C] {

  import AttributeShardedIndex._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val MinDateTime = ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  private val MaxDateTime = ZonedDateTime.of(9999, 12, 31, 23, 59, 59, 999000000, ZoneOffset.UTC)

  override protected def secondaryIndex(sft: SimpleFeatureType): Option[IndexKeySpace[_, _]] =
    Some(DateIndexKeySpace).filter(_.supports(sft))

  object DateIndexKeySpace extends IndexKeySpace[(Option[String], Filter), Long] {

    override def supports(sft: SimpleFeatureType): Boolean = sft.getDtgField.isDefined

    override val indexKeyByteLength: Int = 12

    override def toIndexKey(sft: SimpleFeatureType, lenient: Boolean): SimpleFeature => Seq[Long] = {
      val dtgIndex = sft.getDtgIndex.getOrElse(-1)
      (feature) => {
        val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
        if (dtg == null) { Seq(0L) } else { Seq(dtg.getTime) }
      }
    }

    override def toIndexKeyBytes(sft: SimpleFeatureType, lenient: Boolean): ToIndexKeyBytes = {
      val dtgIndex = sft.getDtgIndex.getOrElse(-1)
      (prefix, feature, suffix) => {
        val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
        val p = ByteArrays.concat(prefix: _*)
        Seq(ByteArrays.concat(p, timeToBytes(if (dtg == null) { 0L } else { dtg.getTime }), suffix))
      }
    }

    override def getIndexValues(sft: SimpleFeatureType, filter: Filter, explain: Explainer): (Option[String], Filter) =
      (sft.getDtgField, filter)

    override def getRanges(values: (Option[String], Filter), multiplier: Int): Iterator[ScanRange[Long]] = {
      val (dtgField, filter) = values
      val intervals = dtgField.map(FilterHelper.extractIntervals(filter, _)).getOrElse(FilterValues.empty)
      intervals.values.iterator.map { bounds =>
        val lower = bounds.lower.value.getOrElse(MinDateTime).toInstant.toEpochMilli
        val upper = bounds.upper.value.getOrElse(MaxDateTime).toInstant.toEpochMilli
        BoundedRange(lower, upper)
      }
    }

    override def getRangeBytes(ranges: Iterator[ScanRange[Long]],
                               prefixes: Seq[Array[Byte]],
                               tier: Boolean): Iterator[ByteRange] = {
      if (prefixes.isEmpty) {
        ranges.map {
          case BoundedRange(lo, hi) => BoundedByteRange(timeToBytes(lo), roundUpTime(timeToBytes(hi)))
          case r => throw new IllegalArgumentException(s"Unexpected range type $r")
        }
      } else {
        ranges.flatMap {
          case BoundedRange(lo, hi) =>
            val lower = timeToBytes(lo)
            val upper = roundUpTime(timeToBytes(hi))
            prefixes.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

          case r =>
            throw new IllegalArgumentException(s"Unexpected range type $r")
        }
      }
    }

    override def useFullFilter(values: Option[(Option[String], Filter)],
                               config: Option[GeoMesaDataStoreConfig],
                               hints: Hints): Boolean = false // TODO for functions?

    // store the first 12 hex chars of the time - that is roughly down to the minute interval
    private def timeToBytes(t: Long): Array[Byte] =
      typeRegistry.encode(t).substring(0, 12).getBytes(StandardCharsets.UTF_8)

    // rounds up the time to ensure our range covers all possible times given our time resolution
    private def roundUpTime(time: Array[Byte]): Array[Byte] = {
      // find the last byte in the array that is not 0xff
      var changeIndex: Int = time.length - 1
      while (changeIndex > -1 && time(changeIndex) == 0xff.toByte) { changeIndex -= 1 }

      if (changeIndex < 0) {
        // the array is all 1s - it's already at time max given our resolution
        time
      } else {
        // increment the selected byte
        time.updated(changeIndex, (time(changeIndex) + 1).asInstanceOf[Byte])
      }
    }
  }
}
