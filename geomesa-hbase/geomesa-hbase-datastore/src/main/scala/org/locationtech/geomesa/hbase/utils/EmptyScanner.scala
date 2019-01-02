/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.utils

import java.io.IOException
import java.util.Collections

import org.apache.hadoop.hbase.client.{Result, ResultScanner}
import org.apache.hadoop.hbase.client.metrics.ScanMetrics

object EmptyScanner extends ResultScanner {
  override def next(): Result = throw new IOException("Next on an empty iterator")
  override def next(i: Int): Array[Result] = throw new IOException("Next on an empty iterator")
  override def close(): Unit = {}
  override def iterator(): java.util.Iterator[Result] = Collections.emptyIterator()
  def getScanMetrics(): ScanMetrics = null
  def renewLease(): Boolean = false
}
