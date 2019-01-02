/***********************************************************************
 * Copyright (c) 2017-2019 IBM
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.querybuilder._
import org.locationtech.geomesa.cassandra._
import org.locationtech.geomesa.index.FlushableFeatureWriter
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

abstract class CassandraFeatureWriter(val sft: SimpleFeatureType,
                                      val ds: CassandraDataStore,
                                      val indices: Seq[CassandraFeatureIndexType],
                                      val filter: Filter,
                                      val partition: TablePartition) extends CassandraFeatureWriterType {

  private val wrapper = CassandraFeature.wrapper(sft)

  override protected def createMutator(table: String): String = table

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

  override def flush(): Unit = {}
  override def close(): Unit = {}
}

object CassandraFeatureWriter {

  class CassandraFeatureWriterFactory(ds: CassandraDataStore) extends CassandraFeatureWriterFactoryType {
    override def createFeatureWriter(sft: SimpleFeatureType,
                                     indices: Seq[CassandraFeatureIndexType],
                                     filter: Option[Filter]): FlushableFeatureWriter = {
      (TablePartition(ds, sft), filter) match {
        case (None, None) =>
          new CassandraFeatureWriter(sft, ds, indices, null, null)
              with CassandraTableFeatureWriterType with CassandraAppendFeatureWriterType

        case (None, Some(f)) =>
          new CassandraFeatureWriter(sft, ds, indices, f, null)
              with CassandraTableFeatureWriterType with CassandraModifyFeatureWriterType

        case (Some(p), None) =>
          new CassandraFeatureWriter(sft, ds, indices, null, p)
              with CassandraPartitionedFeatureWriterType with CassandraAppendFeatureWriterType

        case (Some(p), Some(f)) =>
          new CassandraFeatureWriter(sft, ds, indices, f, p)
              with CassandraPartitionedFeatureWriterType with CassandraModifyFeatureWriterType
      }
    }
  }
}
