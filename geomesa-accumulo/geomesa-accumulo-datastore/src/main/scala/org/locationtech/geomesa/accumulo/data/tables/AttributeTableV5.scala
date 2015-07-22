/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */


package org.locationtech.geomesa.accumulo.data.tables

import com.google.common.collect.ImmutableSortedSet
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

/**
 * Contains logic for converting between accumulo and geotools for the attribute index
 */
@deprecated
object AttributeTableV5 extends GeoMesaTable with Logging {

  override def supports(sft: SimpleFeatureType) =
    sft.getSchemaVersion < 6 && SimpleFeatureTypes.getSecondaryIndexedAttributes(sft).nonEmpty

  override val suffix: String = "attr_idx"

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val indexedAttributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft)
    val indexesOfIndexedAttributes = indexedAttributes.map { a => sft.indexOf(a.getName) }
    val attributesToIdx = indexedAttributes.zip(indexesOfIndexedAttributes)
    val rowIdPrefix = sft.getTableSharingPrefix
    (toWrite: FeatureToWrite) => getAttributeIndexMutations(toWrite, attributesToIdx, rowIdPrefix)
  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
    val indexedAttributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft)
    val indexesOfIndexedAttributes = indexedAttributes.map { a => sft.indexOf(a.getName)}
    val attributesToIdx = indexedAttributes.zip(indexesOfIndexedAttributes)
    val rowIdPrefix = sft.getTableSharingPrefix
    (toWrite: FeatureToWrite) => getAttributeIndexMutations(toWrite, attributesToIdx, rowIdPrefix, delete = true)
  }

  private val NULLBYTE = "\u0000"

  /**
   * Gets mutations for the attribute index table
   *
   * @param toWrite
   * @param indexedAttributes attributes that will be indexed
   * @param rowIdPrefix
   * @param delete whether we are writing or deleting
   * @return
   */
  def getAttributeIndexMutations(toWrite: FeatureToWrite,
                                 indexedAttributes: Seq[(AttributeDescriptor, Int)],
                                 rowIdPrefix: String,
                                 delete: Boolean = false): Seq[Mutation] = {
    val cq = new Text(toWrite.feature.getID)
    indexedAttributes.flatMap { case (descriptor, idx) =>
      val attribute = toWrite.feature.getAttribute(idx)
      val mutations = getAttributeIndexRows(rowIdPrefix, descriptor, attribute).map(new Mutation(_))
      if (delete) {
        mutations.foreach(_.putDelete(EMPTY_COLF, cq, toWrite.columnVisibility))
      } else {
        val value = descriptor.getIndexCoverage() match {
          case IndexCoverage.FULL => toWrite.dataValue
          case IndexCoverage.JOIN => toWrite.indexValue
        }
        mutations.foreach(_.put(EMPTY_COLF, cq, toWrite.columnVisibility, value))
      }
      mutations
    }
  }

  /**
   * Gets row keys for the attribute index. Usually a single row will be returned, but collections
   * will result in multiple rows.
   *
   * @param rowIdPrefix
   * @param descriptor
   * @param value
   * @return
   */
  def getAttributeIndexRows(rowIdPrefix: String,
                            descriptor: AttributeDescriptor,
                            value: Any): Seq[String] = {
    val prefix = getAttributeIndexRowPrefix(rowIdPrefix, descriptor)
    AttributeTable.encodeForIndex(value, descriptor).map(prefix + _)
  }

  /**
   * Gets a prefix for an attribute row - useful for ranges over a particular attribute
   *
   * @param rowIdPrefix
   * @param descriptor
   * @return
   */
  def getAttributeIndexRowPrefix(rowIdPrefix: String, descriptor: AttributeDescriptor): String =
    rowIdPrefix ++ descriptor.getLocalName ++ NULLBYTE

  /**
   * Decodes an attribute value out of row string
   *
   * @param rowIdPrefix table sharing prefix
   * @param descriptor the attribute we're decoding
   * @param row
   * @return
   */
  def decodeAttributeIndexRow(rowIdPrefix: String,
                              descriptor: AttributeDescriptor,
                              row: String): Try[AttributeIndexRow] =
    for {
      suffix <- Try(row.substring(rowIdPrefix.length))
      separator = suffix.indexOf(NULLBYTE)
      name = suffix.substring(0, separator)
      encodedValue = suffix.substring(separator + 1)
      decodedValue = AttributeTable.decode(encodedValue, descriptor)
    } yield {
      AttributeIndexRow(name, decodedValue)
    }

  override def configureTable(featureType: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
    tableOps.setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey, "128M")
    val indexedAttrs = SimpleFeatureTypes.getSecondaryIndexedAttributes(featureType)
    if (indexedAttrs.nonEmpty) {
      val prefix = featureType.getTableSharingPrefix
      val prefixFn = getAttributeIndexRowPrefix(prefix, _: AttributeDescriptor)
      val names = indexedAttrs.map(prefixFn).map(new Text(_))
      val splits = ImmutableSortedSet.copyOf(names.toArray)
      tableOps.addSplits(table, splits)
    }
  }
}

case class AttributeIndexRow(attributeName: String, attributeValue: Any)
