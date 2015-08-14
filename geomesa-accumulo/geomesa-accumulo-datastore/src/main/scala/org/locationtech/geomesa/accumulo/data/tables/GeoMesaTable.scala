/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

import org.apache.accumulo.core.client.BatchDeleter
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

trait GeoMesaTable {

  /**
   * Is the table compatible with the given feature type
   */
  def supports(sft: SimpleFeatureType): Boolean

  /**
   * Create a name usable for an accumulo table
   */
  def formatTableName(prefix: String, sft: SimpleFeatureType) =
    GeoMesaTable.formatTableName(prefix, suffix, sft)

  /**
   * The name used to identify the table
   */
  def suffix: String

  /**
   * Creates a function to write a feature to the table
   */
  def writer(sft: SimpleFeatureType): FeatureToMutations

  /**
   * Creates a function to delete a feature to the table
   */
  def remover(sft: SimpleFeatureType): FeatureToMutations

  /**
   * Deletes all features from the table
   */
  def deleteFeaturesForType(sft: SimpleFeatureType, bd: BatchDeleter): Unit = {
    val prefix = new Text(sft.getTableSharingPrefix)
    bd.setRanges(Seq(new AccRange(prefix, true, AccRange.followingPrefix(prefix), false)))
    bd.delete()
  }

  def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit
}

object GeoMesaTable {

  def getTables(sft: SimpleFeatureType): Seq[GeoMesaTable] = {
    val enabled = {
      val s = sft.getEnabledTables.toString
      if (s.nonEmpty) AvailableTables.toTables(s.split(",").toList) else AvailableTables.AllTables
    }
    Seq(RecordTable, SpatioTemporalTable, AttributeTableV5, AttributeTable, Z3Table)
      .filter(_.supports(sft))
      .filter(enabled.contains)
  }

  def getTableNames(sft: SimpleFeatureType, acc: AccumuloConnectorCreator): Seq[String] =
    getTables(sft).map(acc.getTableName(sft.getTypeName, _))

  // only alphanumeric is safe
  private val SAFE_FEATURE_NAME_PATTERN = "^[a-zA-Z0-9]+$"
  private val alphaNumeric = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')

  /**
   * Format a table name with a namespace. Non alpha-numeric characters present in
   * featureType names will be underscore hex encoded (e.g. _2a) including multibyte
   * UTF8 characters (e.g. _2a_f3_8c) to make them safe for accumulo table names
   * but still human readable.
   */
  protected[tables] def formatTableName(prefix: String, suffix: String, sft: SimpleFeatureType): String =
    if (sft.isTableSharing) {
      formatSharedTableName(prefix, suffix)
    } else {
      formatSoloTableName(prefix, suffix, sft)
    }

  protected[tables] def formatSoloTableName(prefix: String, suffix: String, sft: SimpleFeatureType): String = {
    val typeName = sft.getTypeName
    val safeTypeName = if (typeName.matches(SAFE_FEATURE_NAME_PATTERN)) {
      typeName
    } else {
      hexEncodeNonAlphaNumeric(typeName)
    }
    concatenateNameParts(prefix, safeTypeName, suffix)
  }

  protected[tables] def formatSharedTableName(prefix: String, suffix: String): String =
    concatenateNameParts(prefix, suffix)

  /**
   * Format a table name for the shared tables
   */
  protected[data] def concatenateNameParts(parts: String *): String = parts.mkString("_")

  /**
   * Encode non-alphanumeric characters in a string with
   * underscore plus hex digits representing the bytes. Note
   * that multibyte characters will be represented with multiple
   * underscores and bytes...e.g. _8a_2f_3b
   */
  protected[data] def hexEncodeNonAlphaNumeric(input: String): String = {
    val sb = new StringBuilder
    input.toCharArray.foreach { c =>
      if (alphaNumeric.contains(c)) {
        sb.append(c)
      } else {
        val encoded =
          Hex.encodeHex(c.toString.getBytes("UTF8")).grouped(2)
              .map{ arr => "_" + arr(0) + arr(1) }.mkString.toLowerCase
        sb.append(encoded)
      }
    }
    sb.toString()
  }
}