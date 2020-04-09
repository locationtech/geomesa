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
  var lastRow: Array[Byte] = _

  override def update(region: Array[Byte], row: Array[Byte], response: GeoMesaCoprocessorResponse): Unit = {
    logger.trace(s"In update for region ${ByteArrays.printable(region)} and row ${ByteArrays.printable(row)}")

    val opt = Option(response)
    opt.collect { case r if r.hasVersion => r.getVersion }.foreach { i =>
      logger.warn(s"Coprocessors responded with incompatible version: $i")
    }

    val result = opt.map(_.getPayloadList).orNull
    val lastScanned = opt.collect { case r if r.hasLastScanned => r.getLastScanned }.orNull

    if (lastScanned != null && !lastScanned.isEmpty) {
      if (lastRow != null) {
        logger.error(
          s"Last row was not null for region ${ByteArrays.printable(region)} and row ${ByteArrays.printable(row)}\n" +
              "This indicates that one range spanned multiple regions - results might be incorrect.")
      }
      lastRow = lastScanned.toByteArray
    }

    if (result != null) {
      this.result.addAll(result)
    }
  }
}
