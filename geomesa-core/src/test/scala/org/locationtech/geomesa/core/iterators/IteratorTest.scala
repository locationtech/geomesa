/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.core.iterators

import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Mutation, Value}

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
    val writer: BatchWriter = conn.createBatchWriter(TEST_TABLE_NAME, new BatchWriterConfig)
    for (key <- data.keys) {
      val m1: Mutation = new Mutation(key.getRow)
      m1.put(key.getColumnFamily, key.getColumnQualifier, data(key))
      writer.addMutation(m1)
    }
  }

  var conn: Connector = null
}