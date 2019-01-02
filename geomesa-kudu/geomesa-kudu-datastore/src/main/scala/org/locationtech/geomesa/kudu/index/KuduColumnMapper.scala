/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.index

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.kudu.client.{CreateTableOptions, KuduTable, PartialRow}
import org.apache.kudu.{ColumnSchema, Schema}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.VisibilityAdapter
import org.locationtech.geomesa.kudu.schema.{KuduColumnAdapter, KuduSimpleFeatureSchema}
import org.locationtech.geomesa.kudu.{KuduSplitterOptions, KuduValue, Partitioning}
import org.locationtech.geomesa.utils.text.KVPairParser
import org.opengis.feature.simple.SimpleFeatureType

object KuduColumnMapper {

  private val mappers = Caffeine.newBuilder().build(
    new CacheLoader[GeoMesaFeatureIndex[_, _], KuduColumnMapper]() {
      override def load(key: GeoMesaFeatureIndex[_, _]): KuduColumnMapper = {
        key.name match {
          case IdIndex.name                 => new IdColumnMapper(key)
          case Z3Index.name | XZ3Index.name => new Z3ColumnMapper(key)
          case Z2Index.name | XZ2Index.name => new Z2ColumnMapper(key)
          case AttributeIndex.name          => new AttributeColumnMapper(key)
          case _ => throw new IllegalArgumentException(s"Unexpected index: ${key.name}")
        }
      }
    }
  )

  def apply(index: GeoMesaFeatureIndex[_, _]): KuduColumnMapper = mappers.get(index)

  private def splitters(sft: SimpleFeatureType): Map[String, String] =
    Option(sft.getUserData.get(KuduSplitterOptions).asInstanceOf[String]).map(KVPairParser.parse).getOrElse(Map.empty)
}

/**
  *
  * @param index feature index
  * @param keyColumns columns that are part of the primary key, used for range planning
  */
abstract class KuduColumnMapper(val index: GeoMesaFeatureIndex[_, _], val keyColumns: Seq[KuduColumnAdapter[_]]) {

  protected val splitters: Map[String, String] = KuduColumnMapper.splitters(index.sft)

  val schema: KuduSimpleFeatureSchema = KuduSimpleFeatureSchema(index.sft)

  /**
    * Table schema, which includes all the primary key columns and the feature type columns
    *
    * @return
    */
  val tableSchema: Schema = {
    val cols = new java.util.ArrayList[ColumnSchema]()
    keyColumns.flatMap(_.columns).foreach(cols.add)
    cols.add(VisibilityAdapter.column)
    schema.writeSchema.foreach(cols.add)
    new Schema(cols)
  }

  /**
    * Create initial partitions based on table splitting config
    */
  def configurePartitions(): CreateTableOptions

  /**
    * Creates a new ranges partition that will cover the time period. Only implemented by indices
    * with a leading time period, as otherwise we have to partition up front. Kudu only supports
    * adding new range partitions that don't overlap any existing partitions - you can't modify or
    * split existing partitions
    *
    * @param table kudu table
    * @param bin time period being covered (e.g. week)
    * @return
    */
  def createPartition(table: KuduTable, bin: Short): Option[Partitioning] = None

  /**
    * Turns scan ranges into kudu ranges
    *
    * @param ranges scan ranges
    * @param tieredKeyRanges tiered ranges
    * @return
    */
  def toRowRanges(ranges: Seq[ScanRange[_]],
                  tieredKeyRanges: Seq[ByteRange]): Seq[(Option[PartialRow], Option[PartialRow])]

  /**
    * Creates key values for insert
    *
    * @param value index key
    * @return
    */
  def createKeyValues(value: SingleRowKeyValue[_]): Seq[KuduValue[_]]
}
