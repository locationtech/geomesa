/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.locationtech.geomesa.accumulo.iterators.AbstractIteratorTest._
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig

object AbstractIteratorTest {
  val TEST_TABLE_NAME: String = "query_test"
}


abstract class AbstractIteratorTest {
  protected val conn = new MockInstance().getConnector("mockuser", new PasswordToken(Array[Byte]()))

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
    val writer = conn.createBatchWriter(TEST_TABLE_NAME, GeoMesaBatchWriterConfig())
    data.foreach({case (key, value) => {
      val m1 = new Mutation(key.getRow)
      m1.put(key.getColumnFamily, key.getColumnQualifier, value)
      writer.addMutation(m1)
    }})
  }
}
