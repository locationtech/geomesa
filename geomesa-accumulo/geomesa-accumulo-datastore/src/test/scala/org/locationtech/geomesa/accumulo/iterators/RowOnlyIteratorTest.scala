/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.nio.ByteBuffer

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{Key, Range, Value}
import org.apache.accumulo.core.security.Authorizations
import org.junit.{Before, Test}
import org.locationtech.geomesa.accumulo.iterators.AbstractIteratorTest._

import scala.collection.JavaConverters._


class RowOnlyIteratorTest extends AbstractIteratorTest with Logging {
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
      logger.debug(entry.getKey + " " + ByteBuffer.wrap(entry.getValue.get).getDouble)
    })
  }

  @Test
  def comparison() {
    val scanner = conn.createScanner(TEST_TABLE_NAME, new Authorizations)
    scanner.setRange(new Range)
    scanner.asScala.foreach(entry => {
      logger.debug(entry.getKey + " " + ByteBuffer.wrap(entry.getValue.get).getDouble)
    })
  }
}
