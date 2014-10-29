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

import java.nio.ByteBuffer

import org.apache.accumulo.core.data.{Key, Range, Value}
import org.apache.accumulo.core.security.Authorizations
import org.junit.{Before, Test}
import org.locationtech.geomesa.core.iterators.AbstractIteratorTest._

import scala.collection.JavaConverters._


class RowOnlyIteratorTest
    extends AbstractIteratorTest {
  @Before
  def setup() {
    val rows = Seq("dqb6b46", "dqb6b40", "dqb6b43")
    val cfs = Seq("cf1")
    val cqs = Seq("cqA", "cqb")
    val timestamps = Seq(0, 5, 100)
    setup(
           (for {
             row <- rows
             cf <- cfs
             cq <- cqs
             timestamp <- timestamps
           } yield {
             val bytes = new Array[Byte](8)
             ByteBuffer.wrap(bytes).putDouble(5.0)
             new Key(row, cf, cq, timestamp) -> new Value(bytes)
           }).toMap
         )
  }

  @Test
  def nocfts() {
    val scanner = conn.createScanner(TEST_TABLE_NAME, new Authorizations)
    scanner.setRange(new Range)
    RowOnlyIterator.setupRowOnlyIterator(scanner, 1000)
    scanner.asScala.foreach(entry => {
      System.out.println(entry.getKey + " " + ByteBuffer.wrap(entry.getValue.get).getDouble)
    })
  }

  @Test
  def comparison() {
    val scanner = conn.createScanner(TEST_TABLE_NAME, new Authorizations)
    scanner.setRange(new Range)
    scanner.asScala.foreach(entry => {
      System.out.println(entry.getKey + " " + ByteBuffer.wrap(entry.getValue.get).getDouble)
    })
  }
}
