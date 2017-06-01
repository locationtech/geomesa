/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig

trait IteratorTest {
  val TEST_TABLE_NAME: String = "query_test"
  def setup(data: Map[Key, Value]) {
    conn = buildTestConnection
    resetTestTable(conn, TEST_TABLE_NAME)
    initializeTables(data)
  }

  def buildTestConnection: Connector = {
    new MockInstance().getConnector("root", new PasswordToken(Array[Byte]()))
  }

  def resetTestTable(conn: Connector, testTableName: String) {
    try {
      conn.tableOperations.delete(testTableName)
    }
    catch {
      case tnfe: TableNotFoundException => {
      }
    }
    conn.tableOperations.create(testTableName)
  }

  def initializeTables(data: Map[Key, Value]) {
    val writer: BatchWriter = conn.createBatchWriter(TEST_TABLE_NAME, GeoMesaBatchWriterConfig())
    for (key <- data.keys) {
      val m1: Mutation = new Mutation(key.getRow)
      m1.put(key.getColumnFamily, key.getColumnQualifier, data(key))
      writer.addMutation(m1)
    }
  }

  var conn: Connector = null
}