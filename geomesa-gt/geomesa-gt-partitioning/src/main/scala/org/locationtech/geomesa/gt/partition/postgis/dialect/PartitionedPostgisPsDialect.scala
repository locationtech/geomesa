/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect

import org.geotools.data.postgis.PostGISPSDialect
import org.geotools.jdbc.JDBCDataStore
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import java.sql.{Connection, DatabaseMetaData}

class PartitionedPostgisPsDialect(store: JDBCDataStore, delegate: PartitionedPostgisDialect)
    extends PostGISPSDialect(store, delegate){

  override protected def convertToArray(
      value: Any, componentTypeName: String, componentType: Class[_], connection: Connection): java.sql.Array = {
    val array = value match {
      case list: java.util.List[_] => list.toArray()
      case _ => value
    }
    super.convertToArray(array, componentTypeName, componentType, connection)
  }

  // fix bug with PostGISPSDialect dialect not delegating these methods

  override def getDefaultVarcharSize: Int = delegate.getDefaultVarcharSize
  override def encodeTableName(raw: String, sql: StringBuffer): Unit = delegate.encodeTableName(raw, sql)
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
