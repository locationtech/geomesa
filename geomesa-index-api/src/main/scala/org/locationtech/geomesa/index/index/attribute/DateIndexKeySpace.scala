/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute

import java.time.ZonedDateTime
import java.util.Date

import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.index.api.IndexKeySpace.IndexKeySpaceFactory
import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Encodes dates as lexicoded longs. Null values are encoded as Long.MinValue. This class is only used for
  * secondary tiering, so it never has sharing or shards
  */
class DateIndexKeySpace(val sft: SimpleFeatureType, dtgField: String)
    extends IndexKeySpace[Seq[Bounds[ZonedDateTime]], Long] {

  import DateIndexKeySpace.{Empty, MaxUpperBound, MinLowerBound}

  require(classOf[Date].isAssignableFrom(sft.getDescriptor(dtgField).getType.getBinding),
    s"Expected field $dtgField to have a date binding, but instead it has: " +
        sft.getDescriptor(dtgField).getType.getBinding.getName)

  private val dtgIndex = sft.indexOf(dtgField)

  override val attributes: Seq[String] = Seq(dtgField)

  override val sharing: Array[Byte] = Empty

  override val sharding: ShardStrategy = NoShardStrategy

  override val indexKeyByteLength: Right[(Array[Byte], Int, Int) => Int, Int] = Right(8)

  override def toIndexKey(writable: WritableFeature,
                          tier: Array[Byte],
                          id: Array[Byte],
                          lenient: Boolean): RowKeyValue[Long] = {
    // note: only used as a tiered keyspace so tier and id will be empty
    val dtg = writable.getAttribute[Date](dtgIndex)
    val time = if (dtg == null) { Long.MinValue } else { dtg.getTime }
    SingleRowKeyValue(ByteArrays.toOrderedBytes(time), Empty, Empty, time, tier, Empty, Seq.empty)
  }

  override def getIndexValues(filter: Filter, explain: Explainer): Seq[Bounds[ZonedDateTime]] =
    FilterHelper.extractIntervals(filter, dtgField).values

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

  override def getRangeBytes(ranges: Iterator[ScanRange[Long]], tier: Boolean): Iterator[ByteRange] = {
    ranges.map {
      case SingleRowRange(row)  => SingleRowByteRange(ByteArrays.toOrderedBytes(row))
      case BoundedRange(lo, hi) => BoundedByteRange(ByteArrays.toOrderedBytes(lo), ByteArrays.toOrderedBytes(hi))
      case r => throw new IllegalArgumentException(s"Unexpected range type $r")
    }
  }

  override def useFullFilter(values: Option[Seq[Bounds[ZonedDateTime]]],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = false
}

object DateIndexKeySpace extends IndexKeySpaceFactory[Seq[Bounds[ZonedDateTime]], Long] {

  // note: add 1 to exclude null values
  private val MinLowerBound = Long.MinValue + 1
  private val MaxUpperBound = Long.MaxValue

  private val Empty = Array.empty[Byte]

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    attributes.lengthCompare(1) == 0 && sft.indexOf(attributes.head) != -1 &&
        classOf[Date].isAssignableFrom(sft.getDescriptor(attributes.head).getType.getBinding)

  override def apply(sft: SimpleFeatureType, attributes: Seq[String], tier: Boolean): DateIndexKeySpace =
    new DateIndexKeySpace(sft, attributes.head)
}
