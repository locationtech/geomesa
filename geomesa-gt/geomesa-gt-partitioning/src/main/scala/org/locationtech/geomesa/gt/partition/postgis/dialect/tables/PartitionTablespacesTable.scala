/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect
package tables

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.SftUserData
import org.locationtech.geomesa.utils.io.WithClose

import java.sql.{Connection, DatabaseMetaData}

/**
 * Stores tablespaces used by each feature type
 */
@deprecated("Tablespaces are stored in user data table")
object PartitionTablespacesTable extends PartitionTablespacesTable with AdvisoryLock {
  override protected val lockId: Long = 2005234735580322669L
}

@deprecated("Tablespaces are stored in user data table")
class PartitionTablespacesTable extends Sql with LazyLogging {

  val Name: TableName = TableName("partition_tablespaces")

  override def create(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    // note: we no longer insert data here, as it goes in the user table
    // delete any existing data in the table - this code will only be invoked when calling updateSchema, which will
    // store the table spaces in the user data table instead
    drop(info)
  }

  override def drop(info: TypeInfo)(implicit ex: ExecutionContext): Unit = {
    if (exists(info)) {
      ex.executeUpdate(s"DELETE FROM ${info.schema.quoted}.${Name.quoted} WHERE type_name = ?;", Seq(info.typeName))
    }
  }

  /**
   * Read partition table spaces for the specified type
   *
   * @param cx connection
   * @param metadata metadata
   * @param schema database schema name
   * @param typeName feature type name
   * @return
   */
  def read(cx: Connection, metadata: DatabaseMetaData, schema: String, typeName: String): Map[String, String] = {
    val map = Map.newBuilder[String, String]
    if (exists(schema, metadata)) {
      val tablespaceSql =
        s"select table_space, table_type from " +
          s"${escape(schema)}.${PartitionTablespacesTable.Name.quoted} where type_name = ?"
      WithClose(cx.prepareStatement(tablespaceSql)) { statement =>
        statement.setString(1, typeName)
        WithClose(statement.executeQuery()) { rs =>
          while (rs.next()) {
            val ts = rs.getString(1)
            if (ts != null && ts.nonEmpty) {
              rs.getString(2) match {
                case WriteAheadTableSuffix.raw => map += SftUserData.WriteAheadTableSpace.key -> ts
                case PartitionedWriteAheadTableSuffix.raw => map += SftUserData.WriteAheadPartitionsTableSpace.key -> ts
                case PartitionedTableSuffix.raw => map += SftUserData.MainTableSpace.key -> ts
                case s => logger.warn(s"Ignoring unexpected tablespace table: $s")
              }
            }
          }
        }
      }
    }
    map.result()
  }

  private def exists(info: TypeInfo)(implicit ex: ExecutionContext): Boolean =
    exists(info.schema.raw, ex.cx.getMetaData)

  private def exists(schema: String, metadata: DatabaseMetaData): Boolean =
    WithClose(metadata.getTables(null, schema, Name.raw, null))(_.next())
}
