/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute.legacy

import java.nio.charset.StandardCharsets
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.attribute.AttributeIndexKey
import org.locationtech.geomesa.index.index.attribute.legacy.AttributeIndexV3.LegacyDateIndexKeySpace
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

// tiered date index, id not serialized in the feature
class AttributeIndexV3 protected (ds: GeoMesaDataStore[_],
                                  sft: SimpleFeatureType,
                                  version: Int,
                                  attribute: String,
                                  dtg: Option[String],
                                  mode: IndexMode)
    extends AttributeIndexV4(ds, sft, version, attribute, dtg.toSeq, mode) {

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, attribute: String, dtg: Option[String], mode: IndexMode) =
    this(ds, sft, 3, attribute, dtg, mode)

  override val tieredKeySpace: Option[IndexKeySpace[_, _]] = dtg.map(new LegacyDateIndexKeySpace(sft, _))
}

object AttributeIndexV3 {

  private val MinDateTime = ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  private val MaxDateTime = ZonedDateTime.of(9999, 12, 31, 23, 59, 59, 999000000, ZoneOffset.UTC)

  class LegacyDateIndexKeySpace(val sft: SimpleFeatureType, dtgField: String)
      extends IndexKeySpace[Seq[Bounds[ZonedDateTime]], Long] {

    require(classOf[Date].isAssignableFrom(sft.getDescriptor(dtgField).getType.getBinding),
      s"Expected field $dtgField to have a date binding, but instead it has: " +
          sft.getDescriptor(dtgField).getType.getBinding.getSimpleName)

    private val dtgIndex = sft.indexOf(dtgField)

    override val attributes: Seq[String] = Seq(dtgField)

    override val sharing: Array[Byte] = Array.empty

    override val sharding: ShardStrategy = NoShardStrategy

    override val indexKeyByteLength: Right[(Array[Byte], Int, Int) => Int, Int] = Right(12)

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[Long] = {
      // note: only used as a tiered keyspace so tier and id will be empty
      val dtg = writable.getAttribute[Date](dtgIndex)
      val time = if (dtg == null) { 0L } else { dtg.getTime }
      SingleRowKeyValue(timeToBytes(time), sharing, sharing, time, tier, sharing, Seq.empty)
    }

    override def getIndexValues(filter: Filter, explain: Explainer): Seq[Bounds[ZonedDateTime]] =
      FilterHelper.extractIntervals(filter, dtgField).values

    override def getRanges(values: Seq[Bounds[ZonedDateTime]], multiplier: Int): Iterator[ScanRange[Long]] = {
      values.iterator.map { bounds =>
        val lower = bounds.lower.value.getOrElse(MinDateTime).toInstant.toEpochMilli
        val upper = bounds.upper.value.getOrElse(MaxDateTime).toInstant.toEpochMilli
        BoundedRange(lower, upper)
      }
    }

    override def getRangeBytes(ranges: Iterator[ScanRange[Long]], tier: Boolean): Iterator[ByteRange] = {
      ranges.map {
        case BoundedRange(lo, hi) => BoundedByteRange(timeToBytes(lo), roundUpTime(timeToBytes(hi)))
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    }

    override def useFullFilter(values: Option[Seq[Bounds[ZonedDateTime]]],
                               config: Option[GeoMesaDataStoreConfig],
                               hints: Hints): Boolean = false // TODO for functions?

    // store the first 12 hex chars of the time - that is roughly down to the minute interval
    private def timeToBytes(t: Long): Array[Byte] =
      AttributeIndexKey.typeEncode(t).substring(0, 12).getBytes(StandardCharsets.UTF_8)

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
