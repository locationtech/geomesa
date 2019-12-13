/***********************************************************************
 * Copyright (c) 2017-2019 IBM
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.index

import java.nio.ByteBuffer

import com.datastax.driver.core.{PreparedStatement, Session}
import org.locationtech.geomesa.cassandra.{NamedColumn, RowSelect}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index}

object CassandraColumnMapper {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val FeatureIdColumnName = "fid"
  val SimpleFeatureColumnName = "sf"

  val ShardColumn = NamedColumn("shard", 0, "tinyint", classOf[Byte], partition = true)

  def binColumn(i: Int): NamedColumn = NamedColumn("period", i, "smallint", classOf[Short], partition = true)
  def zColumn(i: Int): NamedColumn = NamedColumn("z", i, "bigint", classOf[Long])

  def featureIdColumn(i: Int): NamedColumn = NamedColumn(FeatureIdColumnName, i, "text", classOf[String])
  def featureColumn(i: Int): NamedColumn = NamedColumn(SimpleFeatureColumnName, i, "blob", classOf[ByteBuffer])

  def apply(index: GeoMesaFeatureIndex[_, _]): CassandraColumnMapper = {
    index.name match {
      case IdIndex.name                             => IdColumnMapper(index.sft)
      case Z3Index.name | XZ3Index.name             => Z3ColumnMapper(index.sft.getZShards)
      case Z2Index.name | XZ2Index.name             => Z2ColumnMapper(index.sft.getZShards)
      case AttributeIndex.name if index.version > 7 => AttributeColumnMapper(index.sft.getAttributeShards)
      case AttributeIndex.name                      => SharedAttributeColumnMapper
      case _ => throw new IllegalArgumentException(s"Unexpected index: ${index.identifier}")
    }
  }
}

trait CassandraColumnMapper {

  import CassandraColumnMapper.SimpleFeatureColumnName

  def columns: Seq[NamedColumn]
  def bind(value: SingleRowKeyValue[_]): Seq[AnyRef]
  def bindDelete(value: SingleRowKeyValue[_]): Seq[AnyRef]

  def select(range: ScanRange[_], tieredKeyRanges: Seq[ByteRange]): Seq[RowSelect]

  def insert(session: Session, table: String): PreparedStatement = {
    val cql = s"INSERT INTO $table (${columns.map(_.name).mkString(", ")}) " +
        s"values (${Seq.fill(columns.length)("?").mkString(", ")})"
    session.prepare(cql)
  }

  def delete(session: Session, table: String): PreparedStatement = {
    val cols = columns.collect { case c if c.name != SimpleFeatureColumnName => c.name }
    val cql = s"DELETE FROM $table WHERE ${cols.mkString("", " = ? and ", " = ?")}"
    session.prepare(cql)
  }
}
