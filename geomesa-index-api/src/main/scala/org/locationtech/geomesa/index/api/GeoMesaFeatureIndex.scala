/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import java.nio.charset.StandardCharsets
import java.util.{Locale, UUID}

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.binary.Hex
import org.geotools.factory.Hints
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Represents a particular indexing strategy
  *
  * @tparam DS type of related data store
  * @tparam F wrapper around a simple feature - used for caching write calculations
  * @tparam WriteResult feature writers will transform simple features into these
  */
trait GeoMesaFeatureIndex[DS <: GeoMesaDataStore[DS, F, WriteResult], F <: WrappedFeature, WriteResult]
    extends LazyLogging {

  type TypedFilterStrategy = FilterStrategy[DS, F, WriteResult]

  lazy val identifier: String = s"$name:$version"

  lazy private val tableNameKey: String = s"table.$name.v$version"

  /**
    * The name used to identify the index
    */
  def name: String

  /**
    * Current version of the index
    *
    * @return
    */
  def version: Int

  /**
    * Is the index compatible with the given feature type
    *
    * @param sft simple feature type
    * @return
    */
  def supports(sft: SimpleFeatureType): Boolean

  /**
    * The metadata key used to store the table name for this index
    *
    * @param partition partition
    * @return
    */
  def tableNameKey(partition: Option[String] = None): String =
    partition.map(p => s"$tableNameKey.$p").getOrElse(tableNameKey)

  /**
    * Configure the index upon initial creation
    *
    * @param sft simple feature type
    * @param ds data store
    * @param partition partition
    * @return table name
    */
  def configure(sft: SimpleFeatureType, ds: DS, partition: Option[String] = None): String = {
    val key = tableNameKey(partition)
    ds.metadata.read(sft.getTypeName, key).getOrElse {
      val name = generateTableName(sft, ds, partition)
      ds.metadata.insert(sft.getTypeName, key, name)
      name
    }
  }

  /**
    * Deletes the entire index
    *
    * @param sft simple feature type
    * @param ds data store
    * @param partition only delete a single partition, instead of the whole index
    */
  def delete(sft: SimpleFeatureType, ds: DS, partition: Option[String] = None): Unit = {
    partition match {
      case None => ds.metadata.scan(sft.getTypeName, tableNameKey).foreach(k => ds.metadata.remove(sft.getTypeName, k._1))
      case Some(p) => ds.metadata.remove(sft.getTypeName, s"$tableNameKey.$p")
    }
  }

  /**
    * Gets the table name for this index
    *
    * @param sft simple feature type
    * @param ds data store
    * @return
    */
  def getTableNames(sft: SimpleFeatureType, ds: DS, partition: Option[String] = None): Seq[String] = {
    partition match {
      case None => ds.metadata.scan(sft.getTypeName, tableNameKey).map(_._2)
      case Some(p) => ds.metadata.read(sft.getTypeName, s"$tableNameKey.$p").toSeq
    }
  }

  /**
    * Gets the partitions for this index, assuming that the schema is partitioned
    *
    * @param sft simple feature type
    * @param ds data store
    * @return
    */
  def getPartitions(sft: SimpleFeatureType, ds: DS): Seq[String] = {
    if (!TablePartition.partitioned(sft)) { Seq.empty } else {
      val offset = tableNameKey.length + 1
      ds.metadata.scan(sft.getTypeName, tableNameKey).map { case (k, _) => k.substring(offset) }
    }
  }

  /**
    * Creates a function to write a feature to the index
    *
    * @param sft simple feature type
    * @param ds data store
    * @return
    */
  def writer(sft: SimpleFeatureType, ds: DS): F => Seq[WriteResult]

  /**
    * Creates a function to delete a feature from the index
    *
    * @param sft simple feature type
    * @param ds data store
    * @return
    */
  def remover(sft: SimpleFeatureType, ds: DS): F => Seq[WriteResult]

  /**
    * Removes all values from the index
    *
    * @param sft simple feature type
    * @param ds data store
    */
  def removeAll(sft: SimpleFeatureType, ds: DS): Unit

  /**
    * Indicates whether the ID for each feature is serialized with the feature or in the row
    *
    * @return
    */
  @deprecated
  def serializedWithId: Boolean = false

  /**
    *
    * Retrieve an ID from a row. All indices are assumed to encode the feature ID into the row key.
    *
    * The simple feature in the returned function signature is optional (null ok) - if provided the
    * parsed UUID will be cached in the feature user data, if the sft is marked as using UUIDs
    *
    * @param sft simple feature type
    * @return a function to retrieve an ID from a row - (row: Array[Byte], offset: Int, length: Int, feature: SimpleFeature)
    */
  def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int, SimpleFeature) => String

  /**
    * Gets the initial splits for a table
    *
    * @param sft simple feature type
    * @param partition partition, if any
    * @return
    */
  def getSplits(sft: SimpleFeatureType, partition: Option[String] = None): Seq[Array[Byte]]

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
    * @return sequence of options, any of which can satisfy the query
    */
  def getFilterStrategy(sft: SimpleFeatureType,
                        filter: Filter,
                        transform: Option[SimpleFeatureType]): Seq[TypedFilterStrategy]

  /**
    * Gets the estimated cost of running the query. In general, this is the estimated
    * number of features that will have to be scanned.
    */
  def getCost(sft: SimpleFeatureType,
              stats: Option[GeoMesaStats],
              filter: TypedFilterStrategy,
              transform: Option[SimpleFeatureType]): Long

  /**
    * Plans the query
    */
  def getQueryPlan(sft: SimpleFeatureType,
                   ds: DS,
                   filter: TypedFilterStrategy,
                   hints: Hints,
                   explain: Explainer = ExplainNull): QueryPlan[DS, F, WriteResult]

  /**
    * Gets the tables that should be scanned to satisfy a query
    *
    * @param sft simple feature type
    * @param ds data store
    * @param filter filter
    * @return
    */
  def getTablesForQuery(sft: SimpleFeatureType, ds: DS, filter: Option[Filter]): Seq[String] = {
    val partitions = filter.toSeq.flatMap(f => TablePartition(ds, sft).toSeq.flatMap(_.partitions(f)))
    if (partitions.isEmpty) {
      getTableNames(sft, ds, None)
    } else {
      partitions.flatMap(p => getTableNames(sft, ds, Some(p)))
    }
  }

  /**
    * Creates a valid, unique string for the underlying table
    *
    * @param sft simple feature type
    * @param ds data store
    * @return
    */
  protected def generateTableName(sft: SimpleFeatureType, ds: DS, partition: Option[String] = None): String =
    GeoMesaFeatureIndex.formatTableName(ds.config.catalog, GeoMesaFeatureIndex.tableSuffix(this, partition), sft)
}

object GeoMesaFeatureIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  // only alphanumeric is safe
  private val SAFE_FEATURE_NAME_PATTERN = "^[a-zA-Z0-9]+$"
  private val alphaNumeric = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')

  /**
    * Format a table name with a namespace. Non alpha-numeric characters present in
    * featureType names will be underscore hex encoded (e.g. _2a) including multibyte
    * UTF8 characters (e.g. _2a_f3_8c) to make them safe for accumulo table names
    * but still human readable.
    */
  def formatTableName(catalog: String, suffix: String, sft: SimpleFeatureType): String = {
    if (sft.isTableSharing) {
      formatSharedTableName(catalog, suffix)
    } else {
      formatSoloTableName(catalog, suffix, sft.getTypeName)
    }
  }

  def formatSoloTableName(prefix: String, suffix: String, typeName: String): String =
    concatenate(prefix, hexEncodeNonAlphaNumeric(typeName), suffix)

  def formatSharedTableName(prefix: String, suffix: String): String =
    concatenate(prefix, suffix)

  def tableSuffix(index: GeoMesaFeatureIndex[_, _, _], partition: Option[String] = None): String = {
    val base = if (index.version == 1) { index.name } else { concatenate(index.name, s"v${index.version}") }
    partition.map(concatenate(base, _)).getOrElse(base)
  }

  /**
    * Format a table name for the shared tables
    */
  def concatenate(parts: String *): String = parts.mkString("_")

  /**
    * Encode non-alphanumeric characters in a string with
    * underscore plus hex digits representing the bytes. Note
    * that multibyte characters will be represented with multiple
    * underscores and bytes...e.g. _8a_2f_3b
    */
  def hexEncodeNonAlphaNumeric(input: String): String = {
    if (input.matches(SAFE_FEATURE_NAME_PATTERN)) {
      input
    } else {
      val sb = new StringBuilder
      input.toCharArray.foreach { c =>
        if (alphaNumeric.contains(c)) {
          sb.append(c)
        } else {
          val hex = Hex.encodeHex(c.toString.getBytes(StandardCharsets.UTF_8))
          val encoded = hex.grouped(2).map(arr => "_" + arr(0) + arr(1)).mkString.toLowerCase(Locale.US)
          sb.append(encoded)
        }
      }
      sb.toString()
    }
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
