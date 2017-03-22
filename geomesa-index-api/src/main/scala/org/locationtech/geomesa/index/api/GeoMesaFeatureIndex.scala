/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import java.nio.charset.StandardCharsets
import java.util.Locale

import org.apache.commons.codec.binary.Hex
import org.geotools.factory.Hints
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Represents a particular indexing strategy
  *
  * @tparam DS type of related data store
  * @tparam F wrapper around a simple feature - used for caching write calculations
  * @tparam WriteResult feature writers will transform simple features into these
  */
trait GeoMesaFeatureIndex[DS <: GeoMesaDataStore[DS, F, WriteResult], F <: WrappedFeature, WriteResult] {

  type TypedFilterStrategy = FilterStrategy[DS, F, WriteResult]

  lazy val identifier: String = s"$name:$version"

  lazy val tableNameKey: String = s"table.$name.v$version"

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
    * Configure the index upon initial creation
    *
    * @param sft simple feature type
    * @param ds data store
    */
  def configure(sft: SimpleFeatureType, ds: DS): Unit =
    ds.metadata.insert(sft.getTypeName, tableNameKey, generateTableName(sft, ds))

  /**
    * Creates a function to write a feature to the index
    *
    * @param sft simple feature type
    * @param ds data store
    * @return
    */
  def writer(sft: SimpleFeatureType, ds: DS): (F) => Seq[WriteResult]

  /**
    * Creates a function to delete a feature from the index
    *
    * @param sft simple feature type
    * @param ds data store
    * @return
    */
  def remover(sft: SimpleFeatureType, ds: DS): (F) => Seq[WriteResult]

  /**
    * Deletes the entire index
    *
    * @param sft simple feature type
    * @param ds data store
    * @param shared true if this index shares physical space with another (e.g. shared tables)
    */
  def delete(sft: SimpleFeatureType, ds: DS, shared: Boolean): Unit

  /**
    *
    * Retrieve an ID from a row. All indices are assumed to encode the feature ID into the row key
    *
    * @param sft simple feature type
    * @return a function to retrieve an ID from a row - (row: Array[Byte], offset: Int, length: Int)
    */
  def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String

  /**
    * Gets the initial splits for a table
    *
    * @param sft simple feature type
    * @return
    */
  def getSplits(sft: SimpleFeatureType): Seq[Array[Byte]]

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
    * @return sequence of options, any of which can satisfy the query
    */
  def getFilterStrategy(sft: SimpleFeatureType, filter: Filter): Seq[TypedFilterStrategy]

  /**
    * Gets the estimated cost of running the query. In general, this is the estimated
    * number of features that will have to be scanned.
    */
  def getCost(sft: SimpleFeatureType,
              ds: Option[DS],
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
    * Gets the table name for this index
    *
    * @param typeName simple feature type name
    * @param ds data store
    * @return
    */
  def getTableName(typeName: String, ds: DS): String = ds.metadata.read(typeName, tableNameKey).getOrElse {
    throw new RuntimeException(s"Could not read table name from metadata for index $identifier")
  }

  /**
    * Creates a valid, unique string for the underlying table
    *
    * @param sft simple feature type
    * @param ds data store
    * @return
    */
  protected def generateTableName(sft: SimpleFeatureType, ds: DS): String =
    GeoMesaFeatureIndex.formatTableName(ds.config.catalog, GeoMesaFeatureIndex.tableSuffix(this), sft)
}

object GeoMesaFeatureIndex {

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
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
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

  def tableSuffix(index: GeoMesaFeatureIndex[_, _, _]): String =
    if (index.version == 1) index.name else concatenate(index.name, s"v${index.version}")

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
}