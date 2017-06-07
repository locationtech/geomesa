/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.utils

import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue

import com.google.protobuf.ByteString
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback

import scala.collection.JavaConversions._

class GeoMesaHBaseCallBack extends Callback[ByteString] {

  private val finalResult: Queue[ByteString] = new ConcurrentLinkedQueue[ByteString]()

  def getResult: List[ByteString] = {
    var list: List[ByteString] = List[ByteString]()
    for (s <- finalResult) {
      list ::= s
    }
    list
  }

  override def update(region: Array[Byte], row: Array[Byte], result: ByteString): Unit = {
    finalResult.offer(result)
  }
}