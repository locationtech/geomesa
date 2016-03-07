/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import com.amazonaws.services.dynamodbv2.document.{RangeKeyCondition, Table}
import com.amazonaws.services.dynamodbv2.document.spec.{QuerySpec, ScanSpec}
import com.amazonaws.services.dynamodbv2.model.Select
import com.google.common.primitives.{Ints, Longs}
import org.geotools.data.store.{ContentEntry, ContentState}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.dynamo.core.DynamoContentState
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.SimpleFeatureType

class DynamoDBContentState(entry: ContentEntry, catalogTable: Table, sftTable: Table) extends ContentState(entry) with DynamoContentState {

  val sft: SimpleFeatureType = DynamoDBDataStore.getSchema(entry, catalogTable)
  val table: Table = sftTable
  val builderPool = ObjectPoolFactory(getBuilder, 10)

  val serializer = new KryoFeatureSerializer(sft)

  val ALL_QUERY = new ScanSpec().withAttributesToGet(DynamoDBDataStore.serId)

  //TODO: do I need a Select or a Projection?
  def geoTimeQuery(pkz: Int, z3min: Long, z3max: Long): QuerySpec = new QuerySpec()
    .withHashKey(DynamoDBDataStore.geomesaKeyHash, Ints.toByteArray(pkz))
    .withRangeKeyCondition(genRangeKey(z3min, z3max))
    .withAttributesToGet(DynamoDBDataStore.serId)

  def geoTimeCountQuery(pkz: Int, z3min: Long, z3max: Long): QuerySpec = new QuerySpec()
    .withHashKey(DynamoDBDataStore.geomesaKeyHash, Ints.toByteArray(pkz))
    .withRangeKeyCondition(genRangeKey(z3min, z3max))
    .withSelect(Select.COUNT)

  private def genRangeKey(z3min: Long, z3max: Long): RangeKeyCondition = {
    val minZ3 = Longs.toByteArray(z3min)
    val maxZ3 = Longs.toByteArray(z3max)
    new RangeKeyCondition(DynamoDBDataStore.geomesaKeyRange).between(minZ3, maxZ3) // TODO: may need to add 1 to max
  }


  private def getBuilder = {
    val builder = new SimpleFeatureBuilder(sft)
    builder.setValidating(java.lang.Boolean.FALSE)
    builder
  }

}
