/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.coprocessor

import java.io.{InterruptedIOException, _}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Base64, Collections}

import com.google.protobuf.{ByteString, RpcCallback, RpcController}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call
import org.apache.hadoop.hbase.client.{Scan, Table}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.{GeoMesaCoprocessorRequest, GeoMesaCoprocessorResponse, GeoMesaCoprocessorService}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose

/**
 * Client-side coprocessor execution and common functions
 */
object GeoMesaCoprocessor extends LazyLogging {

  val AggregatorClass = "geomesa.hbase.aggregator.class"

  val FilterOpt  = "filter"
  val ScanOpt    = "scan"
  val TimeoutOpt = "timeout"

  private val service = classOf[GeoMesaCoprocessorService]

  def deserializeOptions(bytes: Array[Byte]): Map[String, String] = {
    WithClose(new ByteArrayInputStream(bytes)) { bais =>
      WithClose(new ObjectInputStream(bais)) { ois =>
        ois.readObject.asInstanceOf[Map[String, String]]
      }
    }
  }

  @throws[IOException]
  private def serializeOptions(map: Map[String, String]): Array[Byte] = {
    WithClose(new ByteArrayOutputStream) { baos =>
      WithClose(new ObjectOutputStream(baos)) { oos =>
        oos.writeObject(map)
        oos.flush()
      }
      baos.toByteArray
    }
  }

  /**
    * Executes a geomesa coprocessor
    *
   * @param table table to execute against (not closed by this method)
    * @param scan scan to execute
    * @param options configuration options
    * @return serialized results
    */
  def execute(table: Table, scan: Scan, options: Map[String, String]): CloseableIterator[ByteString] =
    new RpcIterator(table, scan, options)

  /**
   * Timeout configuration option
   *
   * @param millis milliseconds
   * @return
   */
  def timeout(millis: Long): (String, String) = TimeoutOpt -> (millis + System.currentTimeMillis()).toString

  /**
   * Closeable iterator implementation for invoking coprocessor rpcs
   *
   * @param table hbase table (not closed by this iterator)
   * @param scan scan
   * @param options coprocessor options
   */
  class RpcIterator(table: Table, scan: Scan, options: Map[String, String]) extends CloseableIterator[ByteString] {

    private val closed = new AtomicBoolean(false)

    private val request = {
      val opts = options
          .updated(FilterOpt, Base64.getEncoder.encodeToString(scan.getFilter.toByteArray))
          .updated(ScanOpt, Base64.getEncoder.encodeToString(ProtobufUtil.toScan(scan).toByteArray))
      GeoMesaCoprocessorRequest.newBuilder().setOptions(ByteString.copyFrom(serializeOptions(opts))).build()
    }

    private val callable = new Call[GeoMesaCoprocessorService, java.util.List[ByteString]]() {
      override def call(instance: GeoMesaCoprocessorService): java.util.List[ByteString] = {
        if (closed.get) { Collections.emptyList() } else {
          val controller: RpcController = new GeoMesaHBaseRpcController()
          val callback = new RpcCallbackImpl()
          // note: synchronous call
          try { instance.getResult(controller, request, callback) } catch {
            case _: InterruptedException | _: InterruptedIOException | _: CancellationException =>
              logger.warn("Cancelling remote coprocessor call")
              controller.startCancel()
          }

          if (controller.failed()) {
            logger.error(s"Controller failed with error:\n${controller.errorText()}")
            throw new IOException(controller.errorText())
          }

          callback.get()
        }
      }
    }

    lazy private val result = {
      val callBack = new GeoMesaHBaseCallBack()
      try { table.coprocessorService(service, scan.getStartRow, scan.getStopRow, callable, callBack) } catch {
        case e @ (_ :InterruptedException | _ :InterruptedIOException) =>
          logger.warn("Interrupted executing coprocessor query:", e)
      }
      callBack.getResult.iterator
    }

    override def hasNext: Boolean = result.hasNext

    override def next(): ByteString = result.next

    override def close(): Unit = closed.set(true)
  }

  /**
    * Unsynchronized rpc callback
    */
  class RpcCallbackImpl extends RpcCallback[GeoMesaCoprocessorResponse] {

    private var result: java.util.List[ByteString] = _

    def get(): java.util.List[ByteString] = result

    override def run(parameter: GeoMesaCoprocessorResponse): Unit =
      result = Option(parameter).map(_.getPayloadList).orNull
  }
}
