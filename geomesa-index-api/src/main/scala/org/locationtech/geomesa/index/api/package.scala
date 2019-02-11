/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import java.nio.charset.StandardCharsets

import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.{andOption, filterToString}
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.filter.Filter

package object api {

  /**
    * Full key/value pair, for inserts or deletes
    */
  sealed trait RowKeyValue[T] {

    /**
      * Sharing bytes, pulled out from the row key
      *
      * @return
      */
    def sharing: Array[Byte]

    /**
      * Shard bytes, pulled out from the row key
      *
      * @return
      */
    def shard: Array[Byte]

    /**
      * Tier bytes, pulled out from the row key
      *
      * @return
      */
    def tier: Array[Byte]

    /**
      * Feature id bytes, pulled out from the row key
      *
      * @return
      */
    def id: Array[Byte]

    /**
      * Key values
      *
      * @return
      */
    def values: Seq[KeyValue]

    /**
      * Copy the row keys but use new values
      *
      * @param values new values
      * @return
      */
    def copy(values: Seq[KeyValue]): RowKeyValue[T]
  }

  /**
    * Single row key value
    *
    * @param row full binary row value, incorporates the rest of the member variables (except values)
    * @param sharing sharing bytes, pulled out from the row key
    * @param shard shard bytes, pulled out from the row key
    * @param key raw row key value (not including sharing, shard, tier, or id)
    * @param tier tier bytes, pulled out from the row key
    * @param id feature id bytes, pulled out from the row key
    * @param values key values
    */
  case class SingleRowKeyValue[T](row: Array[Byte],
                                  sharing: Array[Byte],
                                  shard: Array[Byte],
                                  key: T,
                                  tier: Array[Byte],
                                  id: Array[Byte],
                                  values: Seq[KeyValue]) extends RowKeyValue[T] {

    override def copy(values: Seq[KeyValue]): SingleRowKeyValue[T] =
      SingleRowKeyValue(row, sharing, shard, key, tier, id, values)

    override def equals(other: Any): Boolean = other match {
      case k: SingleRowKeyValue[_] => java.util.Arrays.equals(row, k.row) && values == k.values
      case _ => false
    }

    override def hashCode(): Int =
      Seq(java.util.Arrays.hashCode(row), values.hashCode).foldLeft(0)((a, b) => 31 * a + b)

    override def toString: String = {
      val tail = if (values.lengthCompare(2) < 0) { s"value=${values.headOption.getOrElse("")}" } else {
        values.zipWithIndex.map { case (v, i) => s"value$i=$v" }.mkString(",")
      }
      s"SingleRowKeyValue(row=${ByteArrays.toHex(row)},sharing=${ByteArrays.toHex(sharing)}," +
          s"shard=${ByteArrays.toHex(shard)},key=$key,tier=${ByteArrays.toHex(tier)},id=${ByteArrays.toHex(id)},$tail)"
    }
  }

  /**
    * Multiple rows with common key values
    *
    * @param rows full binary row values, incorporates the rest of the member variables (except values)
    * @param sharing sharing bytes, pulled out from the row key
    * @param shard shard bytes, pulled out from the row key
    * @param keys raw row key values (not including sharing, shard, tier, or id)
    * @param tier tier bytes, pulled out from the row key
    * @param id feature id bytes, pulled out from the row key
    * @param values key values
    */
  case class MultiRowKeyValue[T](rows: Seq[Array[Byte]],
                                 sharing: Array[Byte],
                                 shard: Array[Byte],
                                 keys: Seq[T],
                                 tier: Array[Byte],
                                 id: Array[Byte],
                                 values: Seq[KeyValue]) extends RowKeyValue[T] {

    def split: Seq[SingleRowKeyValue[T]] = {
      val key = keys.iterator
      rows.map(row => SingleRowKeyValue(row, sharing, shard, key.next, tier, id, values))
    }

    override def copy(values: Seq[KeyValue]): MultiRowKeyValue[T] =
      MultiRowKeyValue(rows, sharing, shard, keys, tier, id, values)

    override def equals(other: Any): Boolean = other match {
      case k: MultiRowKeyValue[_] =>
        rows.length == k.rows.length && values == k.values && {
          var i = 0
          var equals = true
          while (i < rows.length) {
            if (!java.util.Arrays.equals(rows(i), k.rows(i))) {
              equals = false
              i = rows.length
            } else {
              i += 1
            }
          }
          equals
        }

      case _ => false
    }

    override def hashCode(): Int =
      (rows.map(java.util.Arrays.hashCode) :+ values.hashCode).foldLeft(0)((a, b) => 31 * a + b)

    override def toString: String = {
      val tail = if (values.lengthCompare(2) < 0) { s"value=${values.headOption.getOrElse("")}" } else {
        values.zipWithIndex.map { case (v, i) => s"value$i=$v" }.mkString(",")
      }
      s"MultiRowKeyValue(rows=${rows.map(ByteArrays.toHex).mkString(",")},sharing=${ByteArrays.toHex(sharing)}," +
          s"shard=${ByteArrays.toHex(shard)},keys=${keys.mkString(",")},tier=${ByteArrays.toHex(tier)}," +
          s"id=${ByteArrays.toHex(id)},$tail)"
    }
  }

  /**
    * Key value
    *
    * @param cf column family
    * @param cq column qualifier
    * @param vis visibility
    * @param toValue serialized simple feature value (lazily evaluated)
    */
  class KeyValue(val cf: Array[Byte],
                 val cq: Array[Byte],
                 val vis: Array[Byte],
                 toValue: => Array[Byte]) {

    lazy val value: Array[Byte] = toValue

    private val state = Stream(cf, cq, vis) #::: Stream(value)

    def copy(cf: Array[Byte] = cf, cq: Array[Byte] = cq, vis: Array[Byte] = vis, toValue: => Array[Byte] = toValue): KeyValue =
      new KeyValue(cf, cq, vis, toValue)

    override def equals(other: Any): Boolean = other match {
      case k: KeyValue => val iter = k.state.iterator; state.forall(java.util.Arrays.equals(_, iter.next))
      case _ => false
    }

    override def hashCode(): Int = state.map(java.util.Arrays.hashCode).foldLeft(0)((a, b) => 31 * a + b)

    override def toString: String = s"KeyValue(cf=${ByteArrays.toHex(cf)},cq=${ByteArrays.toHex(cq)}" +
        s",vis=${new String(vis, StandardCharsets.UTF_8)},value=${ByteArrays.toHex(value)})"
  }

  object KeyValue {
    def apply(cf: Array[Byte], cq: Array[Byte], vis: Array[Byte], toValue: => Array[Byte]): KeyValue =
      new KeyValue(cf, cq, vis, toValue)
    def unapply(kv: KeyValue): Option[(Array[Byte], Array[Byte], Array[Byte], Array[Byte])] =
      Some((kv.cf, kv.cq, kv.vis, kv.value))
  }

  /**
    * Ranges, filters, and hints for executing a query
    *
    * @param filter filter strategy
    * @param ranges ranges, as bytes
    * @param keyRanges ranges, as raw values (for columnar data stores)
    * @param tieredKeyRanges tiered ranges, as raw values (for columnar data stores)
    * @param ecql secondary filter not encapsulated in the ranges
    * @param hints query hints
    * @param values raw query values (e.g. extracted geometries, dates, etc)
    */
  case class QueryStrategy(filter: FilterStrategy,
                           ranges: Seq[ByteRange],
                           keyRanges: Seq[ScanRange[_]],
                           tieredKeyRanges: Seq[ByteRange],
                           ecql: Option[Filter],
                           hints: Hints,
                           values: Option[_]) {
    def index: GeoMesaFeatureIndex[_, _] = filter.index
  }

  /**
    * A query filter split up into a 'primary' that will be used with the given feature index for range planning,
    * and a 'secondary' that is not captured in the ranges.
    *
    * @param index feature index to scan
    * @param primary primary filter used for range generation
    * @param secondary secondary filter not captured in the ranges
    * @param toCost estimated cost of executing the query against this index
    */
  class FilterStrategy(val index: GeoMesaFeatureIndex[_, _],
                       val primary: Option[Filter],
                       val secondary: Option[Filter],
                       toCost: => Long) {

    lazy val cost: Long = toCost
    lazy val filter: Option[Filter] = andOption(primary.toSeq ++ secondary)

    def getQueryStrategy(hints: Hints, explain: Explainer = ExplainNull): QueryStrategy =
      index.getQueryStrategy(this, hints, explain)

    def copy(secondary: Option[Filter]): FilterStrategy = new FilterStrategy(index, primary, secondary, toCost)

    override lazy val toString: String =
      s"$index[${primary.map(filterToString).getOrElse("INCLUDE")}][${secondary.map(filterToString).getOrElse("None")}]"
  }

  object FilterStrategy {
    def apply(index: GeoMesaFeatureIndex[_, _],
              primary: Option[Filter],
              secondary: Option[Filter],
              toCost: => Long): FilterStrategy = new FilterStrategy(index, primary, secondary, toCost)

    def unapply(f: FilterStrategy): Option[(GeoMesaFeatureIndex[_, _], Option[Filter], Option[Filter], Long)] =
      Some((f.index, f.primary, f.secondary, f.cost))
  }

  /**
    * A series of queries required to satisfy a filter - basically split on ORs
    */
  case class FilterPlan(strategies: Seq[FilterStrategy]) {
    override lazy val toString: String = s"FilterPlan[${strategies.mkString(",")}]"
  }

  /**
    * Trait for ranges of keys that have been converted into bytes
    */
  sealed trait ByteRange

  // normal range with two endpoints
  case class BoundedByteRange(lower: Array[Byte], upper: Array[Byte]) extends ByteRange
  // special case where a range matches a single row - needs to be handled differently sometimes
  case class SingleRowByteRange(row: Array[Byte]) extends ByteRange

  // the following classes are only returned from `getRangeBytes` if 'tier = true'

  // indicates that the upper bound is unbounded and can't be tiered
  case class LowerBoundedByteRange(lower: Array[Byte], upper: Array[Byte]) extends ByteRange
  // indicates that the lower bound is unbounded and can't be tiered
  case class UpperBoundedByteRange(lower: Array[Byte], upper: Array[Byte]) extends ByteRange
  // indicates that both bounds are unbounded and can't be tiered
  case class UnboundedByteRange(lower: Array[Byte], upper: Array[Byte]) extends ByteRange

  object ByteRange {

    import ByteArrays.ByteOrdering
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    val UnboundedLowerRange: Array[Byte] = Array.empty
    val UnboundedUpperRange: Array[Byte] = Array.fill(3)(ByteArrays.MaxByte)

    def min(ranges: Seq[ByteRange]): Array[Byte] = {
      ranges.collect {
        case BoundedByteRange(lo, _) => lo
        case SingleRowByteRange(row) => row
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }.minOption.getOrElse(UnboundedLowerRange)
    }

    def max(ranges: Seq[ByteRange]): Array[Byte] = {
      ranges.collect {
        case BoundedByteRange(_, hi) => hi
        case SingleRowByteRange(row) => row
        case r => throw new IllegalArgumentException(s"Unexpected range type $r")
      }.maxOption.getOrElse(UnboundedUpperRange)
    }
  }

  /**
    * Ranges of native key objects, that haven't been converted to bytes yet
    *
    * @tparam T key type
    */
  sealed trait ScanRange[T]

  // specialize long to avoid boxing for z2/xz2 index
  case class BoundedRange[@specialized(Long) T](lower: T, upper: T) extends ScanRange[T]
  case class SingleRowRange[T](row: T) extends ScanRange[T]
  case class PrefixRange[T](prefix: T) extends ScanRange[T]
  case class LowerBoundedRange[T](lower: T) extends ScanRange[T]
  case class UpperBoundedRange[T](upper: T) extends ScanRange[T]
  case class UnboundedRange[T](empty: T) extends ScanRange[T]
}
