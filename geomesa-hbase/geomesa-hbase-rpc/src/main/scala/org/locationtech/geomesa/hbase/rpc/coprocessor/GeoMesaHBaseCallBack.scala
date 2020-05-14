/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.coprocessor

import java.io.InterruptedIOException
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CancellationException, LinkedBlockingQueue}

import com.google.protobuf.{ByteString, RpcCallback}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.coprocessor.Batch.{Call, Callback}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.{GeoMesaCoprocessorRequest, GeoMesaCoprocessorResponse, GeoMesaCoprocessorService}
import org.locationtech.geomesa.hbase.rpc.coprocessor.GeoMesaCoprocessor.{GeoMesaHBaseRequestVersion, ScanOpt}
import org.locationtech.geomesa.utils.index.ByteArrays

/**
 * Callback class for invoking htable services
 *
 * @param scan initial scan (warning: may be mutated)
 * @param options coprocessor configuration options
 * @param result queue for storing results
 */
class GeoMesaHBaseCallBack(scan: Scan, options: Map[String, String], result: LinkedBlockingQueue[ByteString])
    extends Callback[GeoMesaCoprocessorResponse] with Iterator[Scan] with LazyLogging {

  private var updated: Array[Byte] = _ // tracks our status per invocation to detect multiple region updates

  val callable: Call[GeoMesaCoprocessorService, GeoMesaCoprocessorResponse] =
    new Call[GeoMesaCoprocessorService, GeoMesaCoprocessorResponse]() {
      override def call(instance: GeoMesaCoprocessorService): GeoMesaCoprocessorResponse = {
        val controller = new GeoMesaHBaseRpcController()
        val callback = new RpcCallbackImpl()
        var cancelled = false
        // note: synchronous call
        try { instance.getResult(controller, buildRequest, callback) } catch {
          case _: InterruptedException | _: InterruptedIOException | _: CancellationException =>
            logger.warn("Cancelling remote coprocessor call")
            controller.startCancel()
            cancelled = true
        }

        if (controller.failed() && !cancelled) {
          logger.error(s"Controller failed with error:\n${controller.errorText()}")
        }

        callback.get()
      }
    }

  // in `update` (below), we use scan.startRow as a sentinel to indicate we're done
  // we won't ever start with a null row, as we use an empty byte array to indicate no start row
  override def hasNext: Boolean = scan.getStartRow != null

  override def next: Scan = {
    updated = null // reset the status for the next region update
    scan
  }

  override def update(region: Array[Byte], row: Array[Byte], response: GeoMesaCoprocessorResponse): Unit = {
    logger.trace(s"In update for region ${ByteArrays.printable(region)} and row ${ByteArrays.printable(row)}")

    val opt = Option(response)
    opt.collect { case r if r.hasVersion => r.getVersion }.foreach { i =>
      logger.warn(s"Coprocessors responded with incompatible version: $i")
    }

    val lastScanned = opt.collect { case r if r.hasLastScanned => r.getLastScanned }.orNull
    if (lastScanned != null && !lastScanned.isEmpty) {
      if (updated != null) {
        logger.error(
          s"Last row was not null for region ${ByteArrays.printable(region)} and row ${ByteArrays.printable(row)}\n" +
              "This indicates that one range spanned multiple regions - results might be incorrect.")
      }
      updated = lastScanned.toByteArray
      scan.setStartRow(ByteArrays.rowFollowingRow(updated))
    } else {
      scan.setStartRow(null)
    }

    val result = opt.map(_.getPayloadList).orNull
    if (result != null) {
      this.result.addAll(result)
    }
  }

  // since we mutate the scan start row, we need to rebuild the request for each call
  private def buildRequest: GeoMesaCoprocessorRequest = {
    val opts = options ++ Map(ScanOpt -> Base64.getEncoder.encodeToString(ProtobufUtil.toScan(scan).toByteArray))
    GeoMesaCoprocessorRequest.newBuilder()
        .setVersion(GeoMesaHBaseRequestVersion)
        .setOptions(ByteString.copyFrom(GeoMesaCoprocessor.serializeOptions(opts)))
        .build()
  }

  /**
   * Unsynchronized rpc callback
   */
  class RpcCallbackImpl extends RpcCallback[GeoMesaCoprocessorResponse] {

    private var response: GeoMesaCoprocessorResponse = _

    def get(): GeoMesaCoprocessorResponse = response

    override def run(parameter: GeoMesaCoprocessorResponse): Unit = {
      response = parameter
    }
  }
}
