/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> fb4b9418a7 (GEOMESA-3215 Postgis - support List-type attributes)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect

<<<<<<< HEAD
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
<<<<<<< HEAD
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> a8e0698bf72 (GEOMESA-3215 Postgis - support List-type attributes)
import org.geotools.data.postgis.PostGISPSDialect
import org.geotools.jdbc.JDBCDataStore

<<<<<<< HEAD
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
=======
import java.sql.{Connection, DatabaseMetaData}
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)

class PartitionedPostgisPsDialect(store: JDBCDataStore, delegate: PartitionedPostgisDialect)
    extends PostGISPSDialect(store, delegate){

<<<<<<< HEAD
  import PartitionedPostgisPsDialect.PreparedStatementKey

  import scala.collection.JavaConverters._

  // cache for tracking json-type columns
  private val jsonColumns: LoadingCache[PreparedStatementKey, java.lang.Boolean] =
    Caffeine.newBuilder()
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .build(new CacheLoader[PreparedStatementKey, java.lang.Boolean]() {
          override def load(key: PreparedStatementKey): java.lang.Boolean = {
            key.ps.getParameterMetaData.getParameterTypeName(key.column).toLowerCase(Locale.US) match {
              case "jsonb" | "json" => true
              case _ => false
            }
          }
        })

  override def setValue(value: Any, binding: Class[_], ps: PreparedStatement, column: Int, cx: Connection): Unit = {
    // json columns are string type in geotools, but we have to use setObject or else we get a binding error
    if (binding == classOf[String] && jsonColumns.get(new PreparedStatementKey(ps, column))) {
      ps.setObject(column, value, Types.OTHER)
    } else if (binding == classOf[java.util.List[_]]) {
      // handle bug in jdbc store not calling setArrayValue in update statements
      value match {
        case null =>
          ps.setNull(column, Types.ARRAY)

        case list: java.util.Collection[_] =>
          if (list.isEmpty) {
            ps.setNull(column, Types.ARRAY)
          } else {
            setArray(list.toArray(), ps, column, cx)
          }

        case array: Array[_] =>
          if (array.isEmpty) {
            ps.setNull(column, Types.ARRAY)
          } else {
            setArray(array, ps, column, cx)
          }

        case _ =>
          // this will almost certainly fail...
          super.setValue(value, binding, ps, column, cx)
      }
    } else {
      super.setValue(value, binding, ps, column, cx)
    }
  }

  // based on setArrayValue, but we don't have the attribute descriptor to use
  private def setArray(array: Array[_], ps: PreparedStatement, column: Int, cx: Connection): Unit = {
    val componentType = array(0).getClass
    val sqlType = dataStore.getSqlTypeNameToClassMappings.asScala.collectFirst { case (k, v) if v == componentType => k }
    val componentTypeName = sqlType.getOrElse {
      throw new java.sql.SQLException(s"Failed to find a SQL type for $componentType")
    }
    ps.setArray(column, super.convertToArray(array, componentTypeName, componentType, cx))
  }

=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
  override protected def convertToArray(
      value: Any, componentTypeName: String, componentType: Class[_], connection: Connection): java.sql.Array = {
    val array = value match {
      case list: java.util.List[_] => list.toArray()
      case _ => value
    }
    super.convertToArray(array, componentTypeName, componentType, connection)
  }

  // fix bug with PostGISPSDialect dialect not delegating these methods
<<<<<<< HEAD
  override def encodeCreateTable(sql: StringBuffer): Unit = delegate.encodeCreateTable(sql)
  override def getDefaultVarcharSize: Int = delegate.getDefaultVarcharSize
  override def encodeTableName(raw: String, sql: StringBuffer): Unit = delegate.encodeTableName(raw, sql)
  override def encodePostCreateTable(tableName: String, sql: StringBuffer): Unit =
    delegate.encodePostCreateTable(tableName, sql)
  override def postCreateAttribute(att: AttributeDescriptor, tableName: String, schemaName: String, cx: Connection): Unit =
    delegate.postCreateAttribute(att, tableName, schemaName, cx)
=======

  override def getDefaultVarcharSize: Int = delegate.getDefaultVarcharSize
  override def encodeTableName(raw: String, sql: StringBuffer): Unit = delegate.encodeTableName(raw, sql)
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
  override def postCreateFeatureType(
      featureType: SimpleFeatureType,
      metadata: DatabaseMetaData,
      schemaName: String,
      cx: Connection): Unit = {
    delegate.postCreateFeatureType(featureType, metadata, schemaName, cx)
  }
  override def splitFilter(filter: Filter, schema: SimpleFeatureType): Array[Filter] =
    delegate.splitFilter(filter, schema)
  override def getDesiredTablesType: Array[String] = delegate.getDesiredTablesType
  override def encodePostColumnCreateTable(att: AttributeDescriptor, sql: StringBuffer): Unit =
    delegate.encodePostColumnCreateTable(att, sql)
}
<<<<<<< HEAD

object PartitionedPostgisPsDialect {

  // uses eq on the prepared statement to ensure that we compute json fields exactly once per prepared statement/col
  class PreparedStatementKey(val ps: PreparedStatement, val column: Int) {

    override def equals(other: Any): Boolean = {
      other match {
        case that: PreparedStatementKey => ps.eq(that.ps) && column == that.column
        case _ => false
      }
    }

    override def hashCode(): Int = {
      val state = Seq(ps, column)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }
}
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
