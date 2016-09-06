/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.index.attribute

import com.google.common.collect.ImmutableSortedSet
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.FeatureToMutations
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex
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
trait AttributeWritableIndexV5 extends AccumuloWritableIndex with LazyLogging {

  override def writer(sft: SimpleFeatureType, ops: AccumuloDataStore): FeatureToMutations = {
    val indexedAttributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft)
    val indexesOfIndexedAttributes = indexedAttributes.map { a => sft.indexOf(a.getName) }
    val attributesToIdx = indexedAttributes.zip(indexesOfIndexedAttributes)
    val rowIdPrefix = sft.getTableSharingPrefix
    (toWrite: WritableFeature) => getAttributeIndexMutations(toWrite, attributesToIdx, rowIdPrefix)
  }

  override def remover(sft: SimpleFeatureType, ops: AccumuloDataStore): FeatureToMutations = {
    val indexedAttributes = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft)
    val indexesOfIndexedAttributes = indexedAttributes.map { a => sft.indexOf(a.getName)}
    val attributesToIdx = indexedAttributes.zip(indexesOfIndexedAttributes)
    val rowIdPrefix = sft.getTableSharingPrefix
    (toWrite: WritableFeature) => getAttributeIndexMutations(toWrite, attributesToIdx, rowIdPrefix, delete = true)
  }

  override def getIdFromRow(sft: SimpleFeatureType): (Text) => String = (_) => ""

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
  def getAttributeIndexMutations(toWrite: WritableFeature,
                                 indexedAttributes: Seq[(AttributeDescriptor, Int)],
                                 rowIdPrefix: String,
                                 delete: Boolean = false): Seq[Mutation] = {
    val cq = new Text(toWrite.feature.getID)
    indexedAttributes.flatMap { case (descriptor, idx) =>
      val attribute = toWrite.feature.getAttribute(idx)
      val mutations = getAttributeIndexRows(rowIdPrefix, descriptor, attribute).map(new Mutation(_))
      if (delete) {
        mutations.foreach(_.putDelete(EMPTY_COLF, cq, toWrite.indexValues.head.vis))
      } else {
        val value = descriptor.getIndexCoverage() match {
          case IndexCoverage.FULL => toWrite.fullValues.head
          case IndexCoverage.JOIN => toWrite.indexValues.head
        }
        mutations.foreach(_.put(EMPTY_COLF, cq, value.vis, value.value))
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
    AttributeWritableIndex.encodeForIndex(value, descriptor).map(prefix + _)
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
      decodedValue = AttributeWritableIndex.decode(encodedValue, descriptor)
    } yield {
      AttributeIndexRow(name, decodedValue)
    }

  override def configure(sft: SimpleFeatureType, ops: AccumuloDataStore): Unit = {
    val table = Try(ops.getTableName(sft.getTypeName, this)).getOrElse {
      val table = GeoMesaTable.formatTableName(ops.catalogTable, tableSuffix, sft)
      ops.metadata.insert(sft.getTypeName, tableNameKey, table)
      table
    }
    AccumuloVersion.ensureTableExists(ops.connector, table)
    ops.tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
    val indexedAttrs = SimpleFeatureTypes.getSecondaryIndexedAttributes(sft)
    if (indexedAttrs.nonEmpty) {
      val prefix = sft.getTableSharingPrefix
      val prefixFn = getAttributeIndexRowPrefix(prefix, _: AttributeDescriptor)
      val names = indexedAttrs.map(prefixFn).map(new Text(_))
      val splits = ImmutableSortedSet.copyOf(names.toArray)
      ops.tableOps.addSplits(table, splits)
    }
  }
}

case class AttributeIndexRow(attributeName: String, attributeValue: Any)
