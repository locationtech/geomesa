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
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call
import org.apache.hadoop.hbase.client.{Connection, Scan}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.{GeoMesaCoprocessorRequest, GeoMesaCoprocessorResponse, GeoMesaCoprocessorService}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
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

  private val terminator = ByteString.EMPTY

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
   * @param connection connection
   * @param table table to execute against
   * @param scan scan to execute
   * @param options configuration options
   * @param executor executor service to use for hbase rpc calls
   * @return serialized results
   */
  def execute(
      connection: Connection,
      table: TableName,
      scan: Scan,
      options: Map[String, String],
      executor: ExecutorService): CloseableIterator[ByteString] = {
    new RpcIterator(connection, table, scan, options, executor)
  }

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
  private class RpcIterator(
      connection: Connection,
      table: TableName,
      scan: Scan,
      options: Map[String, String],
      pool: ExecutorService
    ) extends CloseableIterator[ByteString] {

    private val htable = connection.getTable(table, pool)
    private val closed = new AtomicBoolean(false)
    private val callback = new GeoMesaHBaseCallBack()

    private val request = {
      val opts = options ++ Map(
        FilterOpt -> Base64.getEncoder.encodeToString(scan.getFilter.toByteArray),
        ScanOpt   -> Base64.getEncoder.encodeToString(ProtobufUtil.toScan(scan).toByteArray)
      )
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
          }

          callback.get()
        }
      }
    }

    private var staged: ByteString = _

    private val coprocessor = CachedThreadPool.submit(new Runnable() {
      override def run(): Unit = {
        try {
          if (!closed.get) {
            htable.coprocessorService(service, scan.getStartRow, scan.getStopRow, callable, callback)
          }
        } catch {
          case e @ (_ :InterruptedException | _ :InterruptedIOException) =>
            logger.warn("Interrupted executing coprocessor query:", e)
        } finally {
          callback.result.add(terminator)
        }
      }
    })

    override def hasNext: Boolean = {
      if (staged != null) { true } else {
        try {
          staged = callback.result.take()
        } catch {
          case _ :InterruptedException =>
            // propagate the interruption through to the rpc call
            coprocessor.cancel(true)
            return false
        }
        if (terminator eq staged) {
          callback.result.add(staged)
          staged = null
          false
        } else {
          true
        }
      }
    }

    override def next(): ByteString = {
      val res = staged
      staged = null
      res
    }

    override def close(): Unit = {
      closed.set(true)
      htable.close()
    }
  }

  private class CoprocessorCall extends Runnable {
    override def run(): Unit = {

    }
  }

  /**
    * Unsynchronized rpc callback
    */
  private class RpcCallbackImpl extends RpcCallback[GeoMesaCoprocessorResponse] {

    private var result: java.util.List[ByteString] = _

    def get(): java.util.List[ByteString] = result

    override def run(parameter: GeoMesaCoprocessorResponse): Unit =
      result = Option(parameter).map(_.getPayloadList).orNull
  }

}
