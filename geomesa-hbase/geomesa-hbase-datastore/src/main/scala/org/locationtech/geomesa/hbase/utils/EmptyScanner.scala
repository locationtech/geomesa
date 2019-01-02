/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.utils

import java.util.Collections

import org.apache.hadoop.hbase.client.metrics.ScanMetrics
import org.apache.hadoop.hbase.client.{Result, ResultScanner}

object EmptyScanner extends ResultScanner {
  override def next(): Result = Iterator.empty.next()
  override def next(i: Int): Array[Result] = Iterator.empty.next()
  override def close(): Unit = {}
  override def iterator(): java.util.Iterator[Result] = Collections.emptyIterator()

  // override for methods in hbase 1.4 - can't mark them as override as it won't compile with 1.3

  // noinspection AccessorLikeMethodIsEmptyParen
  def getScanMetrics(): ScanMetrics = null
  def renewLease(): Boolean = false
}
