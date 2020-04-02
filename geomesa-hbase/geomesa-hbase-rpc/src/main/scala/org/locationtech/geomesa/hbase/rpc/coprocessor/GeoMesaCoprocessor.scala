/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.coprocessor

import java.io.{InterruptedIOException, _}
import java.util.Base64
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import com.google.protobuf.{ByteString, RpcCallback, RpcController}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call
import org.apache.hadoop.hbase.client.{Connection, Scan}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.{GeoMesaCoprocessorRequest, GeoMesaCoprocessorResponse, GeoMesaCoprocessorService}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.index.ByteArrays
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

    private val resultQueue = new LinkedBlockingQueue[ByteString]()

    private def request = {
      val opts = options ++ Map(
        FilterOpt -> Base64.getEncoder.encodeToString(scan.getFilter.toByteArray),
        ScanOpt   -> Base64.getEncoder.encodeToString(ProtobufUtil.toScan(scan).toByteArray)
      )
      GeoMesaCoprocessorRequest.newBuilder().setOptions(ByteString.copyFrom(serializeOptions(opts))).build()
    }

    private def callable: Call[GeoMesaCoprocessorService, GeoMesaCoprocessorResponse] = new Call[GeoMesaCoprocessorService, GeoMesaCoprocessorResponse]() {
      val interalRequest: GeoMesaCoprocessorRequest = request

      override def call(instance: GeoMesaCoprocessorService): GeoMesaCoprocessorResponse = {
        logger.debug(s"Working in the callback.")
        if (closed.get) {
          // JNH: Should this be an empty response instead?  Maybe not since we can do a null check later?
          null
        } else {
          val controller: RpcController = new GeoMesaHBaseRpcController()
          val callback = new RpcCallbackImpl()
          // note: synchronous call
          try { instance.getResult(controller, interalRequest, callback) } catch {
            case _: InterruptedException | _: InterruptedIOException | _: CancellationException =>
              logger.warn("Cancelling remote coprocessor call")
              controller.startCancel()
          }

          if (controller.failed()) {
            logger.error(s"Controller failed with error:\n${controller.errorText()}")
          }

          logger.debug("Finishing work in the callback")
          callback.get()
        }
      }
    }

    private var staged: ByteString = _

    private val coprocessor = CachedThreadPool.submit(new Runnable() {
      override def run(): Unit = {
        val callback = new GeoMesaHBaseCallBack(resultQueue)
        try {
          if (!closed.get) {
            logger.debug(s"Calling htable.coproService (first time): ${ByteArrays.printable(scan.getStartRow)} to ${ByteArrays.printable(scan.getStopRow)}")
            htable.coprocessorService(service, scan.getStartRow, scan.getStopRow, callable, callback)
          }
          //println(s"Close: ${ByteArrays.printable(scan.getStartRow)} to ${ByteArrays.printable(scan.getStopRow)}")
          logger.debug(s"Closed: ${closed.get}. Callback $callback Callback.lastRow: ${ByteArrays.printable(callback.lastRow)}")

          // If the scan hasn't been killed and we are not done, then re-issue the request!
          while (!closed.get() && callback.lastRow != null) {
            // TODO: Use 'nextRow mojo to advance and not re-read a row.
            val lastRow = callback.lastRow
            val nextRow = ByteArrays.rowFollowingRow(lastRow)

            logger.debug(s"Scan continuing from row: ${ByteArrays.printable(callback.lastRow)}.  Next row is ${ByteArrays.printable(nextRow)}")

            // Reset the callback's status
            callback.lastRow = null

            // JNH: Should request/internalRequest above be changed around?
            // Need to rebuild the 'request' and then the 'callable' from that.
            scan.setStartRow(nextRow)
            //println(s"Calling htable.coproService (second time(s)): ${ByteArrays.printable(nextRow)} to ${ByteArrays.printable(scan.getStopRow)}")
            htable.coprocessorService(service, nextRow, scan.getStopRow, callable, callback)
            logger.debug(s"Closed: ${closed.get}. Callback $callback Callback.lastRow: ${ByteArrays.printable(callback.lastRow)}")
          }
        } catch {
          case e @ (_ :InterruptedException | _ :InterruptedIOException) =>
            logger.warn("Interrupted executing coprocessor query:", e)
        } finally {
          resultQueue.add(terminator)
        }
      }
    })

    override def hasNext: Boolean = {
      if (staged != null) { true } else {
        try {
          staged = resultQueue.take()
        } catch {
          case _ :InterruptedException =>
            // propagate the interruption through to the rpc call
            coprocessor.cancel(true)
            return false
        }
        if (terminator eq staged) {
          resultQueue.add(staged)
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
    private var response: GeoMesaCoprocessorResponse = _

    def get(): GeoMesaCoprocessorResponse = response

    override def run(parameter: GeoMesaCoprocessorResponse): Unit = {
      response = parameter
    }
  }

}
