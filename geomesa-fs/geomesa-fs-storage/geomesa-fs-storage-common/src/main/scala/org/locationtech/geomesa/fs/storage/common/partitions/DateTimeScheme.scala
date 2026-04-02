/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.partitions

import org.calrissian.mango.types.LexiTypeEncoders
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.{PartitionRange, RangeBuilder}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionKey
import org.locationtech.geomesa.fs.storage.api.{PartitionScheme, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.text.DateParsing

import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.{Date, Locale}

case class DateTimeScheme(
    dtg: String,
    dtgIndex: Int,
    unit: ChronoUnit,
  ) extends PartitionScheme {

  import FilterHelper.ff

  private val encoder = LexiTypeEncoders.integerEncoder()

  override val name: String = s"${unit.name().toLowerCase(Locale.US)}:attribute=$dtg"

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val instant = feature.getAttribute(dtgIndex).asInstanceOf[Date].toInstant
    PartitionKey(name, encoder.encode(toPartition(ZonedDateTime.ofInstant(instant, ZoneOffset.UTC))))
  }

  override def getRangesForFilter(filter: Filter): Option[Seq[PartitionRange]] = {
    getBounds(filter).map { bounds =>
      val builder = new RangeBuilder()
      bounds.foreach { range =>
        val lower = range.lower.value.fold("")(v => encoder.encode(toPartition(v)))
        val upper = exclusiveUpperBound(range).fold(DateTimeScheme.UnboundedUpper)(v => encoder.encode(v))
        builder += PartitionRange(name, lower, upper)
      }
      builder.result()
    }
  }

  override def getPartitionsForFilter(filter: Filter): Option[Seq[PartitionKey]] = {
    getBounds(filter).map { bounds =>
      bounds.flatMap { bound =>
        if (!bound.isBoundedBothSides) {
          throw new IllegalArgumentException(s"Can't enumerate an unbounded filter: ${ECQL.toCQL(filter)}")
        }
        val lower = toPartition(bound.lower.value.get)
        val upper = exclusiveUpperBound(bound).get
        Range(lower, upper).map(v => PartitionKey(name, encoder.encode(v)))
      }
    }
  }

  override def getCoveringFilter(partition: String): Filter = {
    val offset = encoder.decode(partition)
    val start = DateTimeScheme.Epoch.plus(offset.longValue(), unit)
    val end = start.plus(1, unit)
    ff.and(ff.greaterOrEqual(ff.property(dtg), ff.literal(DateParsing.format(start))), ff.less(ff.property(dtg), ff.literal(DateParsing.format(end))))
  }

  private def toPartition(dt: ZonedDateTime): Int = {
    require(!dt.isBefore(DateTimeScheme.Epoch), s"Date exceeds minimum indexable value (${DateTimeScheme.Epoch}): $dt")
    unit.between(DateTimeScheme.Epoch, dt).toInt
  }

  private def getBounds(filter: Filter): Option[Seq[Bounds[ZonedDateTime]]] = {
    val bounds = FilterHelper.extractIntervals(filter, dtg)
    if (bounds.isEmpty) {
      None
    } else if (bounds.disjoint) {
      Some(Seq.empty)
    } else {
      Some(bounds.values)
    }
  }

  private def exclusiveUpperBound(bounds: Bounds[ZonedDateTime]): Option[Int] = {
    bounds.upper.value.map { v =>
      val partition = toPartition(v)
      if (bounds.upper.exclusive && toPartition(v.minus(1, ChronoUnit.MILLIS)) < partition) {
        partition
      } else {
        partition + 1
      }
    }
  }

//  override def getSimplifiedFilters(filter: Filter, partition: Option[String]): Option[Seq[SimplifiedFilter]] = {
//    getCoveringPartitions(filter).map { case (covered, intersecting) =>
//      val result = Seq.newBuilder[SimplifiedFilter]
//
//      if (covered.nonEmpty) {
//        // remove the temporal filter that we've already accounted for in our covered partitions
//        val coveredFilter = andOption(partitionSubFilters(filter, isTemporalFilter(_, dtg))._2)
//        result += SimplifiedFilter(coveredFilter.getOrElse(Filter.INCLUDE), covered, partial = false)
//      }
//      if (intersecting.nonEmpty) {
//        result += SimplifiedFilter(filter, intersecting, partial = false)
//      }
//
//      partition match {
//        case None => result.result
//        case Some(p) =>
//          val matched = result.result.find(_.partitions.contains(p))
//          matched.map(_.copy(partitions = Seq(p))).toSeq
//      }
//    }
//  }
//
//  override def getIntersectingPartitions(filter: Filter): Option[Seq[String]] =
//    getCoveringPartitions(filter).map { case (covered, intersecting) => (covered ++ intersecting).sorted }
//
//  private def getCoveringPartitions(filter: Filter): Option[(Seq[String], Seq[String])] = {
//    val bounds = FilterHelper.extractIntervals(filter, dtg, handleExclusiveBounds = false)
//    if (bounds.disjoint) {
//      Some((Seq.empty, Seq.empty))
//    } else if (bounds.isEmpty || !bounds.forall(_.isBoundedBothSides)) {
//      None
//    } else {

//      val covered = ListBuffer.empty[String]
//      val intersecting = ListBuffer.empty[String]
//
//      bounds.values.foreach { bound =>
//        // note: we verified both sides are bounded above
//        val lower = bound.lower.value.get
//        val upper = bound.upper.value.get
//        val start = truncateToPartitionStart(lower)
//        val end = truncateToPartitionStart(upper)
//
//        // do our endpoints match the partition boundary, or do we need to apply a filter to the first/last partition?
//        val lowerBoundCovered = bound.lower.inclusive && lower == start
//
//        // `stepUnit.between` claims to be upper endpoint exclusive, but doesn't seem to be...
//        val steps = stepUnit.between(start, end).toInt
//        if (steps < step) {
//          if (lowerBoundCovered &&
//              ((bound.upper.exclusive && upper == end) || (bound.upper.inclusive && upper == end.plus(step, stepUnit).minus(1, MILLIS)))) {
//            covered += formatter.format(start)
//          } else {
//            intersecting += formatter.format(start)
//          }
//        } else {
//          if (lowerBoundCovered) {
//            covered += formatter.format(start)
//          } else {
//            intersecting += formatter.format(start)
//          }
//          covered ++= Iterator.iterate(start)(_.plus(step, stepUnit)).drop(1).takeWhile(_.isBefore(end)).map(formatter.format)
//          if (bound.upper.inclusive || upper != end) {
//            intersecting += formatter.format(end)
//          }
//        }
//      }
//
//      Some((covered.toSeq, intersecting.distinct.toSeq))
//    }
//  }
}

object DateTimeScheme {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val Epoch: ZonedDateTime = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)

  private val UnboundedUpper = "zzz"

  class DateTimePartitionSchemeFactory extends PartitionSchemeFactory {
    override def load(sft: SimpleFeatureType, scheme: String): Option[PartitionScheme] = {
      val opts = SchemeOpts(scheme)
      val unit = opts.name match {
        case "year"  | "years"  | "yearly"  => Some(ChronoUnit.YEARS)
        case "month" | "months" | "monthly" => Some(ChronoUnit.MONTHS)
        case "week"  | "weeks"  | "weekly"  => Some(ChronoUnit.WEEKS)
        case "day"   | "days"   | "daily"   => Some(ChronoUnit.DAYS)
        case "hour"  | "hours"  | "hourly"  => Some(ChronoUnit.HOURS)
        case _ => None
      }
      unit.map { u =>
        val dtg = opts.getSingle("attribute").orElse(sft.getDtgField).orNull
        require(dtg != null, s"Date scheme requires an attribute to be specified with 'attribute=<attribute>'")
        val index = sft.indexOf(dtg)
        require(index != -1, s"Attribute '$dtg' does not exist in schema '${sft.getTypeName}'")
        require(classOf[Date].isAssignableFrom(sft.getDescriptor(index).getType.getBinding), s"Attribute '$dtg' is not a date")
        DateTimeScheme(dtg, index, u)
      }
    }
  }
}
