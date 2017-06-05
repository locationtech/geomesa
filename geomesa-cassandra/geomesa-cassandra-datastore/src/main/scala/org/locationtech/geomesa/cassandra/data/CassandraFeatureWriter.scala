/***********************************************************************
 * Copyright (c) 2017 IBM
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.querybuilder._
import org.locationtech.geomesa.cassandra._
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
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
  private val serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId)

  override protected def createMutators(tables: IndexedSeq[String]): IndexedSeq[String] = tables

  override protected def executeWrite(table: String, writes: Seq[Seq[RowValue]]): Unit = {
    writes.foreach { values =>
      val insert = s"INSERT INTO $table (${values.map(_.column.name).mkString(", ")}) " +
          s"values (${values.map(_ => "?").mkString(", ")})"
      ds.session.execute(insert, values.map(_.value): _*)
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
      ds.session.execute(delete)
    }
  }

  override def wrapFeature(feature: SimpleFeature): CassandraFeature = new CassandraFeature(feature, serializer)
}
