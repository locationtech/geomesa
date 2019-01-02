/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.compute.spark.sql

import java.util.{List => jList, Map => jMap, UUID}

import org.locationtech.jts.geom.Geometry
import org.apache.metamodel
import org.apache.metamodel.DataContext
import org.apache.metamodel.query.parser.QueryParser
import org.apache.metamodel.query.{CompiledQuery, Query}
import org.apache.metamodel.schema.builder.SimpleTableDefSchemaBuilder
import org.apache.metamodel.schema.{ColumnType, Table}
import org.apache.metamodel.util.SimpleTableDef
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

/**
 * Metamodel data context implementation. Only implements the methods required to parse a SQL query.
 *
 * Tables all are identified the the SFT name, which must be unique. They all have a null schema.
 * This is required as SparkSQL doesn't support schemas.
 */
@deprecated
class GeoMesaDataContext(sfts: Map[String, SimpleFeatureType]) extends DataContext {

  import GeoMesaDataContext._

  /**
   * Maps a simple feature type to a SQL table definition.
   *
   * @param label the SFT name
   * @return a table corresponding to the simple feature type
   */
  override def getTableByQualifiedLabel(label: String): Table = {
    cache.synchronized {
      cache.getOrElseUpdate(label, {
        sfts.get(label).map { sft =>
          val descriptors = sft.getAttributeDescriptors
          val names = descriptors.map(_.getLocalName)
          val types = descriptors.map(_.getType.getBinding).map {
            case c if classOf[java.lang.String].isAssignableFrom(c)  => ColumnType.VARCHAR
            case c if classOf[java.lang.Integer].isAssignableFrom(c) => ColumnType.INTEGER
            case c if classOf[java.lang.Long].isAssignableFrom(c)    => ColumnType.BIGINT
            case c if classOf[java.lang.Float].isAssignableFrom(c)   => ColumnType.FLOAT
            case c if classOf[java.lang.Double].isAssignableFrom(c)  => ColumnType.DOUBLE
            case c if classOf[java.lang.Boolean].isAssignableFrom(c) => ColumnType.BOOLEAN
            case c if classOf[java.util.Date].isAssignableFrom(c)    => ColumnType.TIMESTAMP
            case c if classOf[UUID].isAssignableFrom(c)              => ColumnType.UUID
            case c if classOf[Geometry].isAssignableFrom(c)          => ColumnType.OTHER
            case c if classOf[jList[_]].isAssignableFrom(c)          => ColumnType.LIST
            case c if classOf[jMap[_, _]].isAssignableFrom(c)        => ColumnType.MAP
            case _                                                   => ColumnType.OTHER
          }
          val tableDef = new SimpleTableDef(label, names.toArray, types.toArray)
          // schema has to be null for spark sql compatibility
          new SimpleTableDefSchemaBuilder(null, tableDef).build().getTableByName(label)
        }.orNull
      })
    }
  }

  override def parseQuery(queryString: String): Query = new QueryParser(this, queryString).parse

  override def getSchemaByName(name: String) = ???

  override def getDefaultSchema = ???

  override def getSchemaNames = ???

  override def executeQuery(query: metamodel.query.Query) = ???

  override def getColumnByQualifiedLabel(columnName: String) = ???

  override def executeQuery(compiledQuery: CompiledQuery, values: AnyRef*) = ???

  override def executeQuery(queryString: String) = ???

  override def refreshSchemas() = ???

  override def getSchemas = ???

  override def compileQuery(query: Query) = ???

  override def query() = ???
}

@deprecated
object GeoMesaDataContext {
  private val cache = scala.collection.mutable.Map.empty[String, Table]
}