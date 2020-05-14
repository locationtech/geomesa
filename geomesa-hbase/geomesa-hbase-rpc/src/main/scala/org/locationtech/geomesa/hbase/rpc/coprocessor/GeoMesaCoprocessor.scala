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

import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Scan}
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.GeoMesaCoprocessorService
import org.locationtech.geomesa.index.utils.ThreadManagement.Timeout
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.WithClose

/**
 * Client-side coprocessor execution and common functions
 */
object GeoMesaCoprocessor extends LazyLogging {

  val GeoMesaHBaseRequestVersion = 1

  val AggregatorClass = "geomesa.hbase.aggregator.class"

  val FilterOpt  = "filter"
  val ScanOpt    = "scan"
  val TimeoutOpt = "timeout"
  val YieldOpt   = "yield"

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
  private [coprocessor] def serializeOptions(map: Map[String, String]): Array[Byte] = {
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
   * Timeout coprocessor configuration options
   *
   * @param t timeout
   * @return
   */
  def timeout(t: Timeout): (String, String) = TimeoutOpt -> java.lang.Long.toString(t.absolute)

  /**
   * Closeable iterator implementation for invoking coprocessor rpcs
   *
   * @param table hbase table (not closed by this iterator)
   * @param scan scan (note: may be mutated)
   * @param options coprocessor options
   */
  private class RpcIterator(
      connection: Connection,
      table: TableName,
      scan: Scan,
      options: Map[String, String],
      pool: ExecutorService
    ) extends CloseableIterator[ByteString] {

    private var staged: ByteString = _

    private val htable = connection.getTable(table, pool)
    private val resultQueue = new LinkedBlockingQueue[ByteString]()
    private val call = CachedThreadPool.submit(new CoprocessorInvoker())

    override def hasNext: Boolean = {
      if (staged != null) { true } else {
        try {
          staged = resultQueue.take()
        } catch {
          case _ :InterruptedException =>
            // propagate the interruption through to the rpc call
            call.cancel(true)
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

    override def close(): Unit = htable.close()

    class CoprocessorInvoker extends Runnable() {
      override def run(): Unit = {
        try {
          val callback = new GeoMesaHBaseCallBack(scan, options, resultQueue)
          var i = 0
          // if the scan hasn't been killed and we are not done, issue a new request
          while (callback.hasNext) {
            val next = callback.next
            logger.trace(
              s"Calling htable.coprocessorService (call ${i += 1; i}): " +
                  s"${ByteArrays.printable(next.getStartRow)} to ${ByteArrays.printable(next.getStopRow)}")
            htable.coprocessorService(service, next.getStartRow, next.getStopRow, callback.callable, callback)
          }
        } catch {
          case e @ (_ :InterruptedException | _ :InterruptedIOException) =>
            logger.warn("Interrupted executing coprocessor query:", e)
        } finally {
          resultQueue.add(terminator)
        }
      }
    }
  }
}
