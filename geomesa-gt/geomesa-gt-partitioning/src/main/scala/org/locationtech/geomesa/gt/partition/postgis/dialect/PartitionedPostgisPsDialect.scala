/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.expression.{Expression, PropertyName}
import org.geotools.api.filter.{BinaryLogicOperator, Filter, Or}
import org.geotools.data.postgis.{PostGISPSDialect, PostgisPSFilterToSql}
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.jdbc.{JDBCDataStore, PreparedFilterToSQL}
import org.geotools.util.Version
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisPsDialect.PartitionedPostgisPsFilterToSql
import org.locationtech.geomesa.utils.geotools.converters.FastConverter

import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}

class PartitionedPostgisPsDialect(store: JDBCDataStore, delegate: PartitionedPostgisDialect)
    extends PostGISPSDialect(store, delegate){

  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

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
    if (binding == classOf[String] && att != null && att.isJson) {
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
            setArrayValue(list.toArray(), att, ps, column, cx)
          }

        case array: Array[_] =>
          if (array.isEmpty) {
            ps.setNull(column, Types.ARRAY)
          } else {
            setArrayValue(array, att, ps, column, cx)
          }

        case singleton =>
          setArrayValue(Array(singleton), att, ps, column, cx)
      }
    } else {
      super.setValue(value, binding, att, ps, column, cx)
    }
  }

  override protected def convertToArray(
      value: Any, componentTypeName: String, componentType: Class[_], connection: Connection): java.sql.Array = {
    val array = value match {
      case list: java.util.List[_] => list.toArray()
      case _ => value
    }
    super.convertToArray(array, componentTypeName, componentType, connection)
  }

  override protected def convert[T](value: AnyRef, binding: Class[T]): T = FastConverter.convert(value, binding)

  // fix bug with PostGISPSDialect dialect not delegating these methods
  override def encodeCreateTable(sql: StringBuffer): Unit = delegate.encodeCreateTable(sql)
  override def getDefaultVarcharSize: Int = delegate.getDefaultVarcharSize
  override def encodeTableName(raw: String, sql: StringBuffer): Unit = delegate.encodeTableName(raw, sql)
  override def encodePostCreateTable(tableName: String, sql: StringBuffer): Unit =
    delegate.encodePostCreateTable(tableName, sql)
  override def postCreateAttribute(att: AttributeDescriptor, tableName: String, schemaName: String, cx: Connection): Unit =
    delegate.postCreateAttribute(att, tableName, schemaName, cx)
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

    override def visit(filter: Or, extraData: AnyRef): AnyRef = {
      // for array-types, skip the super-class implementation, which merges ORs into INs, as it breaks array OR queries
      // for other types, keep the super handling as INs may be more efficient that ORs
      if (FilterHelper.propertyNames(filter).flatMap(name => Option(featureType.getDescriptor(name))).exists(_.getType.getBinding.isArray)) {
        visit(filter.asInstanceOf[BinaryLogicOperator], "OR")
      } else {
        super.visit(filter, extraData)
      }
    }
  }
}
