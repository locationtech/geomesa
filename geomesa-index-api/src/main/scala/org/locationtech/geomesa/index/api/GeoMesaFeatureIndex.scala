/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import java.nio.charset.StandardCharsets
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex.IdFromRow
import org.locationtech.geomesa.index.api.WriteConverter.{TieredWriteConverter, WriteConverterImpl}
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.conf.splitter.TableSplitter
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.NamedIndex
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.conf.IndexId
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Represents a particular indexing strategy
  *
  * @param ds data store
  * @param sft simple feature type stored in this index
  * @param name name of the index
  * @param version version of the index
  * @param attributes attributes used to create the index keys
  * @param mode mode of the index (read/write/both)
  * @tparam T values extracted from a filter and used for creating ranges - extracted geometries, z-ranges, etc
  * @tparam U a single key space index value, e.g. Long for a z-value, etc
  */
abstract class GeoMesaFeatureIndex[T, U](val ds: GeoMesaDataStore[_],
                                         val sft: SimpleFeatureType,
                                         val name: String,
                                         val version: Int,
                                         val attributes: Seq[String],
                                         val mode: IndexMode) extends NamedIndex with LazyLogging {

  protected val tableNameKey: String = GeoMesaFeatureIndex.baseTableNameKey(name, attributes, version)

  // note: needs to be lazy to allow subclasses to define keyspace
  protected lazy val idFromRow: IdFromRow = IdFromRow(sft, keySpace, tieredKeySpace)

  /**
    * Unique (for the given sft) identifier string for this index.
    *
    * Can be parsed with `IndexId.parse`, although note that it does not include the read/write mode
    */
  val identifier: String = GeoMesaFeatureIndex.identifier(name, version, attributes)

  /**
    * Is the feature id serialized with the feature? Needed for back-compatibility with old data formats
    */
  val serializedWithId: Boolean = false

  /**
    * Primary key space used by this index
    *
    * @return
    */
  def keySpace: IndexKeySpace[T, U]

  /**
    * Tiered key space beyond the primary one, if any
    *
    * @return
    */
  def tieredKeySpace: Option[IndexKeySpace[_, _]]

  /**
    * The metadata key used to store the table name for this index
    *
    * @param partition partition
    * @return
    */
  def tableNameKey(partition: Option[String] = None): String =
    partition.map(p => s"$tableNameKey.$p").getOrElse(tableNameKey)

  /**
    * Create the metadata entry for the initial index table or a new partition
    *
    * @param partition partition
    * @return table name
    */
  def configureTableName(partition: Option[String] = None, limit: Option[Int] = None): String = {
    val key = tableNameKey(partition)
    ds.metadata.read(sft.getTypeName, key).getOrElse {
      val name = generateTableName(partition, limit)
      ds.metadata.insert(sft.getTypeName, key, name)
      name
    }
  }

  /**
    * Deletes the entire index
    *
    * @param partition only delete a single partition, instead of the whole index
    */
  def deleteTableNames(partition: Option[String] = None): Seq[String] = {
    val tables = getTableNames(partition)
    partition match {
      case Some(p) => ds.metadata.remove(sft.getTypeName, s"$tableNameKey.$p")
      case None =>
        ds.metadata.scan(sft.getTypeName, tableNameKey, cache = false).foreach { case (k, _) =>
          ds.metadata.remove(sft.getTypeName, k)
        }
    }
    tables
  }

  /**
    * Gets the initial splits for a table
    *
    * @param partition partition, if any
    * @return
    */
  def getSplits(partition: Option[String] = None): Seq[Array[Byte]] = {
    def nonEmpty(bytes: Seq[Array[Byte]]): Seq[Array[Byte]] = if (bytes.nonEmpty) { bytes } else { Seq(Array.empty) }

    val shards = nonEmpty(keySpace.sharding.shards)
    val splits = nonEmpty(TableSplitter.getSplits(sft, identifier, partition))

    val result = for (shard <- shards; split <- splits) yield { ByteArrays.concat(shard, split) }

    // drop the first split, which will otherwise be empty
    result.drop(1)
  }

  /**
    * Gets the partitions for this index, assuming that the schema is partitioned
    *
    * @return
    */
  def getPartitions: Seq[String] = {
    if (!TablePartition.partitioned(sft)) { Seq.empty } else {
      val offset = tableNameKey.length + 1
      ds.metadata.scan(sft.getTypeName, tableNameKey).map { case (k, _) => k.substring(offset) }
    }
  }

  /**
    * Gets the table name for this index
    *
    * @param partition get the name for a particular partition, or all partitions
    * @return
    */
  def getTableNames(partition: Option[String] = None): Seq[String] = {
    partition match {
      case None => ds.metadata.scan(sft.getTypeName, tableNameKey).map(_._2)
      case Some(p) => ds.metadata.read(sft.getTypeName, s"$tableNameKey.$p").toSeq
    }
  }

  /**
    * Gets the tables that should be scanned to satisfy a query
    *
    * @param filter filter
    * @return
    */
  def getTablesForQuery(filter: Option[Filter]): Seq[String] = {
    val partitioned = for { f <- filter; tp <- TablePartition(ds, sft); partitions <- tp.partitions(f) } yield {
      partitions.flatMap(p => getTableNames(Some(p)))
    }
    partitioned.getOrElse(getTableNames(None))
  }

  /**
    * Creates a function to generate row keys from features
    *
    * @return
    */
  def createConverter(): WriteConverter[U] = {
    tieredKeySpace match {
      case None => new WriteConverterImpl(keySpace)
      case Some(tier) => new TieredWriteConverter(keySpace, tier)
    }
  }

  /**
    * Retrieve an ID from a row. All indices are assumed to encode the feature ID into the row key.
    *
    * The simple feature in the returned function signature is optional (null ok) - if provided the
    * parsed UUID will be cached in the feature user data, if the sft is marked as using UUIDs
    *
    * @param row row bytes
    * @param offset offset into the row bytes to the first valid byte for this row
    * @param length number of valid bytes for this row
    * @param feature simple feature (optional)
    * @return
    */
  def getIdFromRow(row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature): String =
    idFromRow(row, offset, length, feature)

  /**
    * Gets the offset (start) of the feature id from a row. All indices are assumed to encode the
    * feature ID into the row key.
    *
    * @param row row bytes
    * @param offset offset into the row bytes to the first valid byte for this row
    * @param length number of valid bytes for this row
    * @return
    */
  def getIdOffset(row: Array[Byte], offset: Int, length: Int): Int =
    idFromRow.start(row, offset, length)

  /**
    * Gets options for a 'simple' filter, where each OR is on a single attribute, e.g.
    *   (bbox1 OR bbox2) AND dtg
    *   bbox AND dtg AND (attr1 = foo OR attr = bar)
    * not:
    *   bbox OR dtg
    *
    * Because the inputs are simple, each one can be satisfied with a single query filter.
    * The returned values will each satisfy the query.
    *
    * @param filter input filter
    * @param transform attribute transforms
    * @return a filter strategy which can satisfy the query, if available
    */
  def getFilterStrategy(filter: Filter,
                        transform: Option[SimpleFeatureType],
                        stats: Option[GeoMesaStats]): Option[FilterStrategy]

  /**
    * Plans the query
    *
    * @param filter filter strategy
    * @param hints query hints
    * @param explain explainer
    * @return
    */
  def getQueryStrategy(filter: FilterStrategy, hints: Hints, explain: Explainer = ExplainNull): QueryStrategy = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val sharing = keySpace.sharing
    val indexValues = filter.primary.map(keySpace.getIndexValues(_, explain))

    val useFullFilter = keySpace.useFullFilter(indexValues, Some(ds.config), hints)
    val ecql = if (useFullFilter) { filter.filter } else { filter.secondary }

    indexValues match {
      case None =>
        // check that full table scans are allowed
        if (hints.getMaxFeatures.forall(_ > QueryProperties.BlockMaxThreshold.toInt.get)) {
          // check that full table scans are allowed
          QueryProperties.BlockFullTableScans.onFullTableScan(sft.getTypeName, filter.filter.getOrElse(Filter.INCLUDE))
        }
        val keyRanges = Seq(UnboundedRange(null))
        val byteRanges = Seq(BoundedByteRange(sharing, ByteArrays.rowFollowingPrefix(sharing)))

        QueryStrategy(filter, byteRanges, keyRanges, Seq.empty, ecql, hints, indexValues)

      case Some(values) =>
        val keyRanges = keySpace.getRanges(values).toSeq
        val tier = tieredKeySpace.orNull.asInstanceOf[IndexKeySpace[Any, Any]]

        if (tier == null) {
          val byteRanges = keySpace.getRangeBytes(keyRanges.iterator).toSeq
          QueryStrategy(filter, byteRanges, keyRanges, Seq.empty, ecql, hints, indexValues)
        } else {
          val secondary = filter.secondary.orNull
          if (secondary == null) {
            val byteRanges = keySpace.getRangeBytes(keyRanges.iterator, tier = true).map {
              case BoundedByteRange(lo, hi)      => BoundedByteRange(lo, ByteArrays.concat(hi, ByteRange.UnboundedUpperRange))
              case SingleRowByteRange(row)       => BoundedByteRange(row, ByteArrays.concat(row, ByteRange.UnboundedUpperRange))
              case UpperBoundedByteRange(lo, hi) => BoundedByteRange(lo, ByteArrays.concat(hi, ByteRange.UnboundedUpperRange))
              case LowerBoundedByteRange(lo, hi) => BoundedByteRange(lo, hi)
              case UnboundedByteRange(lo, hi)    => BoundedByteRange(lo, hi)
              case r => throw new IllegalArgumentException(s"Unexpected range type $r")
            }.toSeq
            QueryStrategy(filter, byteRanges, keyRanges, Seq.empty, ecql, hints, indexValues)
          } else {
            val bytes = keySpace.getRangeBytes(keyRanges.iterator, tier = true).toSeq

            val tiers = {
              val multiplier = math.max(1, bytes.count(_.isInstanceOf[SingleRowByteRange]))
              tier.getRangeBytes(tier.getRanges(tier.getIndexValues(secondary, explain), multiplier)).toSeq
            }
            lazy val minTier = ByteRange.min(tiers)
            lazy val maxTier = ByteRange.max(tiers)

            val byteRanges = bytes.flatMap {
              case SingleRowByteRange(row) =>
                // single row - we can use all the tiered ranges appended to the end
                if (tiers.isEmpty) {
                  Iterator.single(BoundedByteRange(row, ByteArrays.concat(row, ByteRange.UnboundedUpperRange)))
                } else {
                  tiers.map {
                    case BoundedByteRange(lo, hi) => BoundedByteRange(ByteArrays.concat(row, lo), ByteArrays.concat(row, hi))
                    case SingleRowByteRange(trow) => SingleRowByteRange(ByteArrays.concat(row, trow))
                  }
                }

              case BoundedByteRange(lo, hi) =>
                // bounded ranges - we can use the min/max tier on the endpoints
                Iterator.single(BoundedByteRange(ByteArrays.concat(lo, minTier), ByteArrays.concat(hi, maxTier)))

              case LowerBoundedByteRange(lo, hi) =>
                // we can't use the upper tier
                Iterator.single(BoundedByteRange(ByteArrays.concat(lo, minTier), hi))

              case UpperBoundedByteRange(lo, hi) =>
                // we can't use the lower tier
                Iterator.single(BoundedByteRange(lo, ByteArrays.concat(hi, maxTier)))

              case UnboundedByteRange(lo, hi) =>
                // we can't use either side of the tiers
                Iterator.single(BoundedByteRange(lo, hi))

              case r =>
                throw new IllegalArgumentException(s"Unexpected range type $r")
            }

            QueryStrategy(filter, byteRanges, keyRanges, tiers, ecql, hints, indexValues)
          }
        }
    }
  }

  /**
    * Creates a valid, unique string for the underlying table
    *
    * @param partition partition
    * @param limit limit on the length of a table name in the underlying database
    * @return
    */
  protected def generateTableName(partition: Option[String] = None, limit: Option[Int] = None): String = {
    import StringSerialization.alphaNumericSafeString

    val prefix = (ds.config.catalog +: Seq(sft.getTypeName, name).map(alphaNumericSafeString)).mkString("_")
    val suffix = s"v$version${partition.map(p => s"_$p").getOrElse("")}"

    def build(attrs: Seq[String]): String = (prefix +: attrs :+ suffix).mkString("_")

    val full = build(attributes.map(alphaNumericSafeString))

    limit match {
      case Some(lim) if full.lengthCompare(lim) > 0 =>
        // try using the attribute numbers instead
        val nums = build(attributes.map(a => s"${sft.indexOf(a)}"))
        if (nums.lengthCompare(lim) <= 0) { nums } else {
          logger.warn(s"Table name length exceeds configured limit ($lim), falling back to UUID: $full")
          IndexAdapter.truncateTableName(full, lim)
        }

      case _ => full
    }
  }

  override def toString: String = s"${getClass.getSimpleName}${attributes.mkString("(", ",", ")")}"

  override def equals(other: Any): Boolean = other match {
    case that: GeoMesaFeatureIndex[_, _] =>
      identifier == that.identifier && mode == that.mode && ds == that.ds && sft == that.sft
    case _ => false
  }

  override def hashCode(): Int = Seq(identifier, ds, sft, mode).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
}

object GeoMesaFeatureIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  /**
    * Some predefined indexing schemes
    */
  object Schemes {
    val Z3TableScheme: List[String]  = List(Z3Index, IdIndex, AttributeIndex).map(_.name)
    val Z2TableScheme: List[String]  = List(Z2Index, IdIndex, AttributeIndex).map(_.name)
    val XZ3TableScheme: List[String] = List(XZ3Index, IdIndex, AttributeIndex).map(_.name)
    val XZ2TableScheme: List[String] = List(XZ2Index, IdIndex, AttributeIndex).map(_.name)
  }

  /**
    * Identifier string for an index. Can be parsed with `IndexId.parse`
    *
    * @param name name
    * @param version version
    * @param attributes attributes
    * @return
    */
  def identifier(name: String, version: Int, attributes: Seq[String]): String =
    s"$name:$version:${attributes.mkString(":")}"

  /**
    * Identifier string for an index. Can be parsed with `IndexId.parse`
    *
    * @param id id to encode
    * @return
    */
  def identifier(id: IndexId): String = identifier(id.name, id.version, id.attributes)

  /**
    * Identifier string for an index. Can be parsed with `IndexId.parse`
    *
    * @param index index
    * @return
    */
  def identifier(index: GeoMesaFeatureIndex[_, _]): String = identifier(index.name, index.version, index.attributes)

  /**
    * Base table name key used for storing in metadata
    *
    * @param name index name
    * @param attributes index attributes
    * @param version index version
    * @return
    */
  def baseTableNameKey(name: String, attributes: Seq[String], version: Int): String = {
    val attrs = if (attributes.isEmpty) { "" } else {
      attributes.map(StringSerialization.alphaNumericSafeString).mkString(".", ".", "")
    }
    s"table.$name$attrs.v$version"
  }

  /**
    * Converts a feature id to bytes, for indexing or querying
    *
    * @param sft simple feature type
    * @return
    */
  def idToBytes(sft: SimpleFeatureType): String => Array[Byte] =
    if (sft.isUuidEncoded) { uuidToBytes } else { stringToBytes }

  /**
    * Converts a byte array to a feature id. Return method takes an optional (null accepted) simple feature,
    * which will be used to cache the parsed feature ID if it is a UUID.
    *
    * @param sft simple feature type
    * @return (bytes, offset, length, SimpleFeature) => id
    */
  def idFromBytes(sft: SimpleFeatureType): (Array[Byte], Int, Int, SimpleFeature) => String =
    if (sft.isUuidEncoded) { uuidFromBytes } else { stringFromBytes }

  /**
    * Trait for parsing feature ids out of row keys
    */
  sealed trait IdFromRow {

    /**
      * Return the id from the given row
      *
      * @param row row value as bytes
      * @param offset start of the row in the byte array
      * @param length length of the row in the byte array
      * @param feature a simple feature used to cache the feature id, optional (may be null)
      * @return the feature id
      */
    def apply(row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature): String

    /**
      * Return the start index of the id in the row
      *
      * @param row row value as bytes
      * @param offset start of the row in the byte array
      * @param length length of the row in the byte array
      * @return the start of the id in the row, relative to the row offset
      */
    def start(row: Array[Byte], offset: Int, length: Int): Int
  }

  object IdFromRow {

    def apply(
        sft: SimpleFeatureType,
        keySpace: IndexKeySpace[_, _],
        tieredKeySpace: Option[IndexKeySpace[_, _]]): IdFromRow = {
      val tieredLength = tieredKeySpace.map(_.indexKeyByteLength).getOrElse(Right(0))
      (keySpace.indexKeyByteLength, tieredLength) match {
        case (Right(i), Right(j)) =>
          new FixedIdFromRow(idFromBytes(sft), i + j)

        case (Right(i), Left(j)) =>
          val dynamic: (Array[Byte], Int, Int) => Int = (row, offset, length) => i + j(row, offset + i, length - i)
          new DynamicIdFromRow(idFromBytes(sft), dynamic)

        case (Left(i), Right(j)) =>
          val dynamic: (Array[Byte], Int, Int) => Int = (row, offset, length) => i(row, offset, length - j) + j
          new DynamicIdFromRow(idFromBytes(sft), dynamic)

        case (Left(i), Left(j))   =>
          val dynamic: (Array[Byte], Int, Int) => Int =
            (row, offset, length) => {
              val first = i(row, offset, length)
              j(row, offset + first, length - first)
            }
          new DynamicIdFromRow(idFromBytes(sft), dynamic)
      }
    }

    /**
      * Id is always at a fixed offset into the row
      *
      * @param idFromBytes deserialization for the feature id
      * @param prefix the length of the fixed prefix preceding the feature id bytes
      */
    final class FixedIdFromRow(idFromBytes: (Array[Byte], Int, Int, SimpleFeature) => String, prefix: Int)
        extends IdFromRow {

      override def apply(row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature): String =
        idFromBytes(row, offset + prefix, length - prefix, feature)

      override def start(row: Array[Byte], offset: Int, length: Int): Int = prefix
    }

    /**
      * Id is dynamically located in the row
      *
      * @param idFromBytes deserialization of the feature id
      * @param prefix a function to find the length of the prefix preceding the feature id bytes
      */
    final class DynamicIdFromRow(
        idFromBytes: (Array[Byte], Int, Int, SimpleFeature) => String,
        prefix: (Array[Byte], Int, Int) => Int
      ) extends IdFromRow {

      override def apply(row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature): String = {
        val prefix = this.prefix(row, offset, length)
        idFromBytes(row, offset + prefix, length - prefix, feature)
      }

      override def start(row: Array[Byte], offset: Int, length: Int): Int = prefix(row, offset, length)
    }
  }

  private def uuidToBytes(id: String): Array[Byte] = {
    val uuid = UUID.fromString(id)
    ByteArrays.uuidToBytes(uuid.getMostSignificantBits, uuid.getLeastSignificantBits)
  }

  private def uuidFromBytes(bytes: Array[Byte], offset: Int, ignored: Int, sf: SimpleFeature): String = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
    val uuid = ByteArrays.uuidFromBytes(bytes, offset)
    if (sf != null) {
      sf.cacheUuid(uuid)
    }
    new UUID(uuid._1, uuid._2).toString
  }

  private def stringToBytes(id: String): Array[Byte] = id.getBytes(StandardCharsets.UTF_8)

  private def stringFromBytes(bytes: Array[Byte], offset: Int, length: Int, ignored: SimpleFeature): String =
    new String(bytes, offset, length, StandardCharsets.UTF_8)
}
