/***********************************************************************
 * Copyright (c) 2017-2018 IBM
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.querybuilder._
import org.locationtech.geomesa.cassandra._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class CassandraAppendFeatureWriter(sft: SimpleFeatureType,
                                   ds: CassandraDataStore,
                                   indices: Option[Seq[CassandraFeatureIndexType]])
      extends CassandraFeatureWriterType(sft, ds, indices) with CassandraAppendFeatureWriterType with CassandraFeatureWriter

class CassandraModifyFeatureWriter(sft: SimpleFeatureType,
                                   ds: CassandraDataStore,
                                   indices: Option[Seq[CassandraFeatureIndexType]],
                                   val filter: Filter)
    extends CassandraFeatureWriterType(sft, ds, indices) with CassandraModifyFeatureWriterType with CassandraFeatureWriter

trait CassandraFeatureWriter extends CassandraFeatureWriterType {

  private val wrapper = CassandraFeature.wrapper(sft)

  override protected def createMutators(tables: IndexedSeq[String]): IndexedSeq[String] = tables

  override protected def executeWrite(table: String, writes: Seq[Seq[RowValue]]): Unit = {
    writes.foreach { row =>
      val insert = s"INSERT INTO $table (${row.map(_.column.name).mkString(", ")}) " +
          s"values (${Seq.fill(row.length)("?").mkString(", ")})"
      val values = row.map(_.value)
      logger.trace(s"$insert : ${values.mkString(",")}")
      ds.session.execute(insert, values: _*)
    }
  }

  override protected def executeRemove(tname: String, removes: Seq[Seq[RowValue]]): Unit = {
    removes.foreach { values =>
      val delete = QueryBuilder.delete.all.from(tname)
      values.foreach { value =>
        if (value.value != null) {
          delete.where(QueryBuilder.eq(value.column.name, value.value))
        }
      }
      logger.trace(delete.toString)
      ds.session.execute(delete)
    }
  }

  override def wrapFeature(feature: SimpleFeature): CassandraFeature = wrapper(feature)
}
