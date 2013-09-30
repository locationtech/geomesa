/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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
package geomesa.core.iterators

import AbstractIteratorTest._
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Value

object AbstractIteratorTest {
  val TEST_TABLE_NAME: String = "query_test"
}


abstract class AbstractIteratorTest {
  protected val conn = new MockInstance().getConnector("mockuser", "mockpass".getBytes)

  def setup(data: Map[Key, Value]) {
    resetTestTable(conn, TEST_TABLE_NAME)
    initializeTables(data)
  }

  protected def resetTestTable(conn: Connector, testTableName: String) {
    try {
      conn.tableOperations.delete(testTableName)
    } catch {
      case tnfe: TableNotFoundException => {}
    }
    conn.tableOperations.create(testTableName)
  }

  protected def initializeTables(data: Map[Key, Value]) {
    val writer = conn.createBatchWriter(TEST_TABLE_NAME, 90L, 90L, 1)
    data.foreach({case (key, value) => {
      val m1 = new Mutation(key.getRow)
      m1.put(key.getColumnFamily, key.getColumnQualifier, value)
      writer.addMutation(m1)
    }})
  }
}
