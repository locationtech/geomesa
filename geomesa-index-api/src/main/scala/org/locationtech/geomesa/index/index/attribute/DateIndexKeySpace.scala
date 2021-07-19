/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.filter.{Bounds, FilterHelper, FilterValues}
import org.locationtech.geomesa.index.api.IndexKeySpace.IndexKeySpaceFactory
import org.locationtech.geomesa.index.api.ShardStrategy.NoShardStrategy
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.index.ByteArrays

import java.time.ZonedDateTime
import java.util.Date

/**
  * Encodes dates as lexicoded longs. Null values are encoded as Long.MinValue. This class is only used for
  * secondary tiering, so it never has sharing or shards
  */
class DateIndexKeySpace(val sft: SimpleFeatureType, dtgField: String)
    extends IndexKeySpace[FilterValues[Bounds[ZonedDateTime]], Long] {

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

  override def getIndexValues(filter: Filter, explain: Explainer): FilterValues[Bounds[ZonedDateTime]] =
    FilterHelper.extractIntervals(filter, dtgField)

  override def getRanges(values: FilterValues[Bounds[ZonedDateTime]], multiplier: Int): Iterator[ScanRange[Long]] = {
    if (values.isEmpty) {
      Iterator.single(UnboundedRange(-1))
    } else if (values.disjoint) {
      Iterator.empty
    } else {
      values.values.iterator.map { bounds =>
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
  }

  override def getRangeBytes(ranges: Iterator[ScanRange[Long]], tier: Boolean): Iterator[ByteRange] = {
    ranges.map {
      case BoundedRange(lo, hi) =>
        BoundedByteRange(ByteArrays.toOrderedBytes(lo), ByteArrays.toOrderedBytes(hi))

      case SingleRowRange(row) =>
        val ordered = ByteArrays.toOrderedBytes(row)
        if (tier) {
          SingleRowByteRange(ordered)
        } else {
          BoundedByteRange(ordered, ByteArrays.rowFollowingPrefix(ordered))
        }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 0d3cbc99a2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> aa627812da (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 1690f5dad4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 54fb5ef951 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9b796685be (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 5e66748475 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ce2ad87199 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> bd17bd131e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 5c68fb7d18 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 914ec419f (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> f0b61142b0 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 9b796685b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b5f0f7d072 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> d488f41800 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 8290d4c47d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 914ec419f (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 7ef22a4bb2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 914ec419fe (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> f0b61142b0 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> bd17bd131e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 7ef22a4bb2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b796685be (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 76328bb492 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b1c30e3f26 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> c4bc2d77a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 1690f5dad (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e66748475 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 8290d4c47d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 54fb5ef95 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ce2ad87199 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 914ec419f (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> f0b61142b0 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 9b796685b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> bd17bd131e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 76328bb49 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 5c68fb7d18 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> ac07626365 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> b5f0f7d072 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 1690f5dad (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> d488f41800 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 8290d4c47d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 914ec419f (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 7ef22a4bb2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
      case UnboundedRange(_) =>
        val max = ByteArrays.rowFollowingPrefix(ByteArrays.toOrderedBytes(MaxUpperBound))
        UnboundedByteRange(ByteArrays.toOrderedBytes(MinLowerBound), max)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9bde42cc4b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b26fc9b517 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> aa627812da (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 1690f5dad4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 914ec419fe (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 9b796685be (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> c4bc2d77a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 5e66748475 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> f0b61142b0 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> bd17bd131e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 5c68fb7d18 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7ef22a4bb2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 54fb5ef95 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> ce2ad87199 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 914ec419f (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 9b796685b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ac07626365 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b5f0f7d072 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> d488f41800 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 54fb5ef95 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 8290d4c47d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> 914ec419f (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54fb5ef951 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> c4bc2d77a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ce2ad87199 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 914ec419f (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> f0b61142b0 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> ac07626365 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 8290d4c47d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 914ec419f (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 7ef22a4bb2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 0d3cbc99a (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 914ec419fe (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> f0b61142b0 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9b796685b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> bd17bd131e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b5f0f7d072 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 7ef22a4bb2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> aa627812d (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b796685be (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 76328bb492 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> b1c30e3f26 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> d022c4befe (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> c4bc2d77a8 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> 868625d944 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> bd17bd131e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> d488f41800 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 1690f5dad (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e66748475 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 54fb5ef95 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ce2ad87199 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 914ec419f (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> f0b61142b0 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9b796685b (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> bd17bd131e (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 76328bb49 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
<<<<<<< HEAD
>>>>>>> 5c68fb7d18 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 70f1ed62e1 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 9bde42cc4 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b26fc9b51 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 5b15f98fad (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
>>>>>>> ac07626365 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> b5f0f7d072 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
>>>>>>> d488f41800 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
=======
=======
>>>>>>> 54fb5ef95 (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
>>>>>>> 8290d4c47d (GEOMESA-3203 Short-circuit disjoint filters in index scans (#2862))
=======
=======
>>>>>>> 914ec419f (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
>>>>>>> 7ef22a4bb2 (GEOMESA-3102 Fix removal of user data in FileSystemDataStore.createSchema (#2787))
      case r => throw new IllegalArgumentException(s"Unexpected range type $r")
    }
  }

  override def useFullFilter(
      values: Option[FilterValues[Bounds[ZonedDateTime]]],
      config: Option[GeoMesaDataStoreConfig],
      hints: Hints): Boolean = false
}

object DateIndexKeySpace extends IndexKeySpaceFactory[FilterValues[Bounds[ZonedDateTime]], Long] {

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
