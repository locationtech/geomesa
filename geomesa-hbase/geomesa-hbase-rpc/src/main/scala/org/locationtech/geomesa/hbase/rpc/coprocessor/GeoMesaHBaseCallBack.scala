/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.coprocessor

import java.util.concurrent.LinkedBlockingQueue

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.GeoMesaCoprocessorResponse
import org.locationtech.geomesa.utils.index.ByteArrays

class GeoMesaHBaseCallBack(result: LinkedBlockingQueue[ByteString]) extends Callback[GeoMesaCoprocessorResponse] with LazyLogging {
  var isDone = false
  var lastRow: Array[Byte] = _

  //val result = new LinkedBlockingQueue[ByteString]()

  override def update(region: Array[Byte], row: Array[Byte], response: GeoMesaCoprocessorResponse): Unit = {
    logger.debug(s"In update for region ${ByteArrays.printable(region)} for row ${ByteArrays.printable(row)}")
    val result =  Option(response).map(_.getPayloadList).orNull
    val lastscanned: ByteString = Option(response).map(_.getLastscanned).orNull
    //println(s"Lastscanned: ${response.getLastscanned}.  Condition: ${lastscanned != null && !lastscanned.isEmpty}  Lastscanned is not null: ${lastscanned != null}. lastscanned.isEmpty: ${lastscanned.isEmpty}")

    if (lastscanned != null && !lastscanned.isEmpty) {
      if (lastRow != null) {
        logger.error("LAST ROW WAS NOT NULL")
      }
      lastRow = lastscanned.toByteArray
      //println(s"Last Scanned is not null and is not Empty with length ${lastRow.length}: ${ByteArrays.printable(lastscanned.toByteArray)}")
      isDone = false
    } else {
      isDone = true
      ////println("Last scanned is null")
    }

    if (result != null) {
      //println(s"In update for region ${region} row: ${ByteArrays.printable(row)}")
      this.result.addAll(result)
    }
  }
}
