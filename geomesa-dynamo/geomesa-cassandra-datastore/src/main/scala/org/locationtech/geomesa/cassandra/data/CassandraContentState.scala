/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core._
import org.geotools.data.store._
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.cassandra.data.CassandraDataStore.FieldSerializer
import org.locationtech.geomesa.dynamo.core.DynamoContentState
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.SimpleFeatureType

class CassandraContentState(entry: ContentEntry, val session: Session, val tableMetadata: TableMetadata) extends ContentState(entry) with DynamoContentState {

  import scala.collection.JavaConversions._

  val sft: SimpleFeatureType = CassandraDataStore.getSchema(entry.getName, tableMetadata)
  val attrNames = sft.getAttributeDescriptors.map(_.getLocalName)
  val selectClause = (Array("fid") ++ attrNames).mkString(",")
  val table = sft.getTypeName
  val deserializers = sft.getAttributeDescriptors.map { ad => FieldSerializer(ad) }.zipWithIndex.toSeq
  val ALL_QUERY            = session.prepare(s"select $selectClause from $table")
  val ALL_COUNT_QUERY      = session.prepare(s"select count(*) from $table")
  val GEO_TIME_QUERY       = session.prepare(s"select $selectClause from $table where (pkz = ?) and (z31 >= ?) and (z31 <= ?)")
  val GEO_TIME_COUNT_QUERY = session.prepare(s"select count(*) from $table where (pkz = ?) and (z31 >= ?) and (z31 <= ?)")
  val builderPool = ObjectPoolFactory(getBuilder, 10)

  private def getBuilder = {
    val builder = new SimpleFeatureBuilder(sft)
    builder.setValidating(java.lang.Boolean.FALSE)
    builder
  }

}
