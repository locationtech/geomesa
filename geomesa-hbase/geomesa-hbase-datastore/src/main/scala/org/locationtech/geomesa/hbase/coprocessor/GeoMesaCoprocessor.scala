/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor

import java.io.{InterruptedIOException, _}
import java.util.Collections
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import com.google.protobuf.{ByteString, RpcCallback, RpcController, Service}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call
import org.apache.hadoop.hbase.client.{Scan, Table}
import org.apache.hadoop.hbase.coprocessor.{CoprocessorException, CoprocessorService, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.protobuf.{ProtobufUtil, ResponseConverter}
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{Coprocessor, CoprocessorEnvironment}
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor.Aggregator
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseAggregator
import org.locationtech.geomesa.hbase.coprocessor.utils.{GeoMesaHBaseCallBack, GeoMesaHBaseRpcController}
import org.locationtech.geomesa.hbase.proto.GeoMesaProto
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.{GeoMesaCoprocessorRequest, GeoMesaCoprocessorResponse, GeoMesaCoprocessorService}
import org.locationtech.geomesa.index.iterators.AggregatingScan.Result
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose

import scala.util.control.NonFatal

class GeoMesaCoprocessor extends GeoMesaCoprocessorService with Coprocessor with CoprocessorService with LazyLogging {

  import scala.collection.JavaConverters._

  private var env: RegionCoprocessorEnvironment = _

  @throws[IOException]
  override def start(env: CoprocessorEnvironment): Unit = {
    env match {
      case e: RegionCoprocessorEnvironment => this.env = e
      case _ => throw new CoprocessorException("Must be loaded on a table region!")
    }
  }

  @throws[IOException]
  override def stop(coprocessorEnvironment: CoprocessorEnvironment): Unit = {
  }

  override def getService: Service = this

  override def getResult(
      controller: RpcController,
      request: GeoMesaProto.GeoMesaCoprocessorRequest,
      done: RpcCallback[GeoMesaProto.GeoMesaCoprocessorResponse]): Unit = {

    val results = GeoMesaCoprocessorResponse.newBuilder()

    try {
      val options = GeoMesaCoprocessor.deserializeOptions(request.getOptions.toByteArray)
      val timeout = options.get(GeoMesaCoprocessor.TimeoutOpt).map(_.toLong + System.currentTimeMillis())
      if (!controller.isCanceled) {
        val clas = options(GeoMesaCoprocessor.AggregatorClass)
        val aggregator = Class.forName(clas).newInstance().asInstanceOf[Aggregator]
        logger.debug(s"Initializing aggregator $aggregator with options ${options.mkString(", ")}")
        aggregator.init(options)

        val scan = ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(Base64.decode(options(GeoMesaCoprocessor.ScanOpt))))
        scan.setFilter(FilterList.parseFrom(Base64.decode(options(GeoMesaCoprocessor.FilterOpt))))

        // enable visibilities by delegating to the region server configured coprocessors
        env.getRegion.getCoprocessorHost.preScannerOpen(scan)

        // TODO: Explore use of MultiRangeFilter
        WithClose(env.getRegion.getScanner(scan)) { scanner =>
          aggregator.setScanner(scanner)
          var done = false
          while (!done) {
            logger.trace(s"Running batch on aggregator $aggregator")
            val agg = aggregator.aggregate()
            if (agg == null) { done = true } else {
              results.addPayload(ByteString.copyFrom(agg))
              if (controller.isCanceled) {
                logger.warn(s"Stopping aggregator $aggregator due to controller being cancelled")
                done = true
              } else if (timeout.exists(_ < System.currentTimeMillis())) {
                logger.warn(s"Stopping aggregator $aggregator due to timeout of ${options(GeoMesaCoprocessor.TimeoutOpt)}ms")
                done = true
              }
            }
          }
        }
      }
    } catch {
      case _: InterruptedException | _ : InterruptedIOException => // stop processing, but don't return an error to prevent retries
      case e: IOException => ResponseConverter.setControllerException(controller, e)
      case NonFatal(e) => ResponseConverter.setControllerException(controller, new IOException(e))
    }

    logger.debug(
      s"Results total size: ${results.getPayloadList.asScala.map(_.size()).sum}" +
          s"\n\tBatch sizes: ${results.getPayloadList.asScala.map(_.size()).mkString(", ")}")

    done.run(results.build)
  }
}

object GeoMesaCoprocessor extends LazyLogging {

  val AggregatorClass = "geomesa.hbase.aggregator.class"

  private type Aggregator = HBaseAggregator[_ <: Result]

  private val FilterOpt  = "filter"
  private val ScanOpt    = "scan"
  private val TimeoutOpt = "timeout"

  private def deserializeOptions(bytes: Array[Byte]): Map[String, String] = {
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
    * @param table table to execute against
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
  def timeout(millis: Long): (String, String) = TimeoutOpt -> millis.toString

  /**
   * Closeable iterator implementation for invoking coprocessor rpcs
   *
   * @param table hbase table
   * @param scan scan
   * @param options coprocessor options
   */
  class RpcIterator(table: Table, scan: Scan, options: Map[String, String]) extends CloseableIterator[ByteString] {

    private val closed = new AtomicBoolean(false)

    private val request = {
      val opts = options
          .updated(FilterOpt, Base64.encodeBytes(scan.getFilter.toByteArray))
          .updated(ScanOpt, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
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
      try { table.coprocessorService(classOf[GeoMesaCoprocessorService], null, null, callable, callBack) } catch {
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
