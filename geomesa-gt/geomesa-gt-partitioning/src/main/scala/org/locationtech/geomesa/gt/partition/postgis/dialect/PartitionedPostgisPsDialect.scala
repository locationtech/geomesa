/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> fb4b9418a7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 1a6dc23ec5 (GEOMESA-3215 Postgis - support List-type attributes)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
<<<<<<< HEAD
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
<<<<<<< HEAD
import org.geotools.api.filter.expression.{Expression, PropertyName}
import org.geotools.data.postgis.{PostGISPSDialect, PostgisPSFilterToSql}
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.jdbc.{JDBCDataStore, PreparedFilterToSQL}
import org.geotools.util.Version
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisPsDialect.PartitionedPostgisPsFilterToSql
=======
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> a8e0698bf72 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> e68704b1b09 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 2590b561114 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 8362c3a15dd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 716b38183d9 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 5ef1a7e6d88 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 09aa9c40cda (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 0853537f254 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
import org.geotools.data.postgis.PostGISPSDialect
import org.geotools.jdbc.JDBCDataStore
>>>>>>> 133f17e7484 (GEOMESA-3215 Postgis - support List-type attributes)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import java.lang.invoke.{MethodHandle, MethodHandles, MethodType}
=======
<<<<<<< HEAD
>>>>>>> ddbcac1d597 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
>>>>>>> c33b798d64a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cb4ed87d048 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 544774dddc6 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 25a4e567deb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ca40199e24c (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9f108f6bb05 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a41df7703e1 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> d49cab89735 (Merge branch 'a0x8o' into stag0)
=======
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 25a4e567deb (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> ca40199e24c (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 9f108f6bb05 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> a41df7703e1 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
=======
import java.sql.{Connection, DatabaseMetaData}
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
=======
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)

class PartitionedPostgisPsDialect(store: JDBCDataStore, delegate: PartitionedPostgisDialect)
    extends PostGISPSDialect(store, delegate){

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
  import PartitionedPostgisPsDialect.PreparedStatementKey

  import scala.collection.JavaConverters._

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
  import PartitionedPostgisPsDialect.PreparedStatementKey

<<<<<<< HEAD
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
  import PartitionedPostgisPsDialect.PreparedStatementKey

>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
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

  // reference to super.setValue, for back-compatibility with gt 30
  private lazy val superSetValue: MethodHandle = {
    val methodType =
      MethodType.methodType(classOf[Unit], classOf[Object], classOf[Class[_]], classOf[PreparedStatement], classOf[Int], classOf[Connection])
    MethodHandles.lookup.findSpecial(classOf[PostGISPSDialect], "setValue", methodType, classOf[PartitionedPostgisPsDialect])
  }

  override def createPreparedFilterToSQL: PreparedFilterToSQL = {
    val fts = new PartitionedPostgisPsFilterToSql(this, delegate.getPostgreSQLVersion(null))
    fts.setFunctionEncodingEnabled(delegate.isFunctionEncodingEnabled)
    fts.setLooseBBOXEnabled(delegate.isLooseBBOXEnabled)
    fts.setEncodeBBOXFilterAsEnvelope(delegate.isEncodeBBOXFilterAsEnvelope)
    fts.setEscapeBackslash(delegate.isEscapeBackslash)
    fts
  }

  override def setValue(
      value: AnyRef,
      binding: Class[_],
      att: AttributeDescriptor,
      ps: PreparedStatement,
      column: Int,
      cx: Connection): Unit = {
    // json columns are string type in geotools, but we have to use setObject or else we get a binding error
    if (binding == classOf[String] && jsonColumns.get(new PreparedStatementKey(ps, column))) {
      ps.setObject(column, value, Types.OTHER)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    } else if (binding.isArray || binding == classOf[java.util.List[_]]) {
=======
<<<<<<< HEAD
=======
>>>>>>> 2613b3c509c (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b9c06c6ec11 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
    } else if (binding == classOf[java.util.List[_]]) {
>>>>>>> f915b893c79 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
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

        case singleton =>
          setArray(Array(singleton), ps, column, cx)
      }
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
    } else {
      super.setValue(value, binding, att, ps, column, cx)
    }
  }

  // for back-compatibility with gt 30
  // noinspection ScalaUnusedSymbol
  def setValue(value: AnyRef, binding: Class[_], ps: PreparedStatement, column: Int, cx: Connection): Unit = {
    // json columns are string type in geotools, but we have to use setObject or else we get a binding error
    if (binding == classOf[String] && jsonColumns.get(new PreparedStatementKey(ps, column))) {
      ps.setObject(column, value, Types.OTHER)
    } else if (binding.isArray || binding == classOf[java.util.List[_]]) {
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

        case singleton =>
          setArray(Array(singleton), ps, column, cx)
      }
    } else {
      superSetValue.invoke(this, value, binding, ps, column, cx)
    }
  }

<<<<<<< HEAD
  // based on setArrayValue, but we don't have the attribute descriptor to use
  private def setArray(array: Array[_], ps: PreparedStatement, column: Int, cx: Connection): Unit = {
    val componentType = array(0).getClass
    val sqlType = dataStore.getSqlTypeNameToClassMappings.asScala.collectFirst { case (k, v) if v == componentType => k }
    val componentTypeName = sqlType.getOrElse {
      throw new java.sql.SQLException(s"Failed to find a SQL type for $componentType")
    }
    ps.setArray(column, super.convertToArray(array, componentTypeName, componentType, cx))
  }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
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
<<<<<<< HEAD
  override def getDefaultVarcharSize: Int = delegate.getDefaultVarcharSize
  override def encodeTableName(raw: String, sql: StringBuffer): Unit = delegate.encodeTableName(raw, sql)
  override def encodePostCreateTable(tableName: String, sql: StringBuffer): Unit =
    delegate.encodePostCreateTable(tableName, sql)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  override def postCreateAttribute(att: AttributeDescriptor, tableName: String, schemaName: String, cx: Connection): Unit =
    delegate.postCreateAttribute(att, tableName, schemaName, cx)
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======

  override def getDefaultVarcharSize: Int = delegate.getDefaultVarcharSize
  override def encodeTableName(raw: String, sql: StringBuffer): Unit = delegate.encodeTableName(raw, sql)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
  override def getDefaultVarcharSize: Int = delegate.getDefaultVarcharSize
  override def encodeTableName(raw: String, sql: StringBuffer): Unit = delegate.encodeTableName(raw, sql)
  override def encodePostCreateTable(tableName: String, sql: StringBuffer): Unit =
    delegate.encodePostCreateTable(tableName, sql)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)

object PartitionedPostgisPsDialect {

  class PartitionedPostgisPsFilterToSql(dialect: PartitionedPostgisPsDialect, pgVersion: Version)
      extends PostgisPSFilterToSql(dialect, pgVersion) {

    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    import scala.collection.JavaConverters._

    override def setFeatureType(featureType: SimpleFeatureType): Unit = {
      // convert List-type attributes to Array-types so that prepared statement bindings work correctly
      if (featureType.getAttributeDescriptors.asScala.exists(_.getType.getBinding == classOf[java.util.List[_]])) {
        val builder = new SimpleFeatureTypeBuilder() {
          override def init(`type`: SimpleFeatureType): Unit = {
            super.init(`type`)
            attributes().clear()
          }
        }
        builder.init(featureType)
        featureType.getAttributeDescriptors.asScala.foreach { descriptor =>
          val ab = new AttributeTypeBuilder(builder.getFeatureTypeFactory)
          ab.init(descriptor)
          if (descriptor.getType.getBinding == classOf[java.util.List[_]]) {
            ab.setBinding(java.lang.reflect.Array.newInstance(Option(descriptor.getListType()).getOrElse(classOf[String]), 0).getClass)
          }
          builder.add(ab.buildDescriptor(descriptor.getLocalName))
        }
        this.featureType = builder.buildFeatureType()
        this.featureType.getUserData.putAll(featureType.getUserData)
      } else {
        this.featureType = featureType
      }
    }

    // note: this would be a cleaner solution, but it doesn't get invoked due to explicit calls to
    // super.getExpressionType in PostgisPSFilterToSql :/
    override def getExpressionType(expression: Expression): Class[_] = {
      val result = Option(expression).collect { case p: PropertyName => p }.flatMap { p =>
        Option(p.evaluate(featureType).asInstanceOf[AttributeDescriptor]).map { descriptor =>
          val binding = descriptor.getType.getBinding
          if (binding == classOf[java.util.List[_]]) {
            val listType = descriptor.getListType()
            if (listType == null) {
              classOf[Array[String]]
            } else {
              java.lang.reflect.Array.newInstance(listType, 0).getClass
            }
          } else {
            binding
          }
        }
      }

      result.getOrElse(super.getExpressionType(expression))
    }
  }

  // uses eq on the prepared statement to ensure that we compute json fields exactly once per prepared statement/col
  private class PreparedStatementKey(val ps: PreparedStatement, val column: Int) {

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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
