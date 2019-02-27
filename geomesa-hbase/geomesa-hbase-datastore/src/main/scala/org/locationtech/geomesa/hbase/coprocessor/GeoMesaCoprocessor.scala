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

import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.{ByteString, RpcCallback, RpcController, Service}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call
import org.apache.hadoop.hbase.client.{Scan, Table}
import org.apache.hadoop.hbase.coprocessor.{CoprocessorException, CoprocessorService, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.protobuf.{ProtobufUtil, ResponseConverter}
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{Coprocessor, CoprocessorEnvironment}
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseAggregator
import org.locationtech.geomesa.hbase.coprocessor.utils.{GeoMesaHBaseCallBack, GeoMesaHBaseRpcController}
import org.locationtech.geomesa.hbase.proto.GeoMesaProto
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.{GeoMesaCoprocessorRequest, GeoMesaCoprocessorResponse, GeoMesaCoprocessorService}
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

class GeoMesaCoprocessor extends GeoMesaCoprocessorService with Coprocessor with CoprocessorService {

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

  def getResult(controller: RpcController,
                request: GeoMesaProto.GeoMesaCoprocessorRequest,
                done: RpcCallback[GeoMesaProto.GeoMesaCoprocessorResponse]): Unit = {
    val response: GeoMesaCoprocessorResponse = try {
      val options: Map[String, String] = deserializeOptions(request.getOptions.toByteArray)
      val aggregator = {
        val classname = options(GeoMesaCoprocessor.AggregatorClass)
        Class.forName(classname).newInstance().asInstanceOf[HBaseAggregator[_]]
      }
      aggregator.init(options)

      val scanList: List[Scan] = getScanFromOptions(options)
      val filterList: FilterList = getFilterListFromOptions(options)

      val results = ArrayBuffer.empty[Array[Byte]]
      scanList.foreach { scan =>
        scan.setFilter(filterList)
        // Enable Visibilities by delegating to the Region Server configured Coprocessors
        env.getRegion.getCoprocessorHost.preScannerOpen(scan)

        // TODO: Explore use of MultiRangeFilter
        val scanner = env.getRegion.getScanner(scan)
        aggregator.setScanner(scanner)
        try {
          while (aggregator.hasNextData) {
            val agg = aggregator.aggregate()
            if (agg != null) {
              results.append(agg)
            }
          }
        } finally {
          scanner.close()
        }
      }
      import scala.collection.JavaConversions._
      GeoMesaCoprocessorResponse.newBuilder.addAllPayload(results.map(ByteString.copyFrom)).build
    } catch {
      case e: IOException => ResponseConverter.setControllerException(controller, e); null
      case NonFatal(e) => ResponseConverter.setControllerException(controller, new IOException(e)); null
    }

    done.run(response)
  }
}

object GeoMesaCoprocessor extends LazyLogging {

  import scala.collection.JavaConverters._

  val AggregatorClass = "geomesa.hbase.aggregator.class"

  private val executor =
    MoreExecutors.getExitingExecutorService(Executors.newCachedThreadPool().asInstanceOf[ThreadPoolExecutor])
  sys.addShutdownHook(executor.shutdownNow())

  /**
    * Executes a geomesa coprocessor
    *
    * @param table table to execute against
    * @param scan scan to execute
    * @param options configuration options
    * @return serialized results
    */
  def execute(table: Table, scan: Scan, options: Map[String, String]): CloseableIterator[ByteString] = {
    val request = {
      val opts = options
          .updated(FILTER_OPT, Base64.encodeBytes(scan.getFilter.toByteArray))
          .updated(SCAN_OPT, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
      GeoMesaCoprocessorRequest.newBuilder().setOptions(ByteString.copyFrom(serializeOptions(opts))).build()
    }

    val calls = Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap[Future[_], java.lang.Boolean]())

    val callable = new Call[GeoMesaCoprocessorService, java.util.List[ByteString]]() {
      override def call(instance: GeoMesaCoprocessorService): java.util.List[ByteString] = {
        val controller: RpcController = new GeoMesaHBaseRpcController()

        val call = new Callable[java.util.List[ByteString]]() {
          override def call(): java.util.List[ByteString] = {
            val rpcCallback = new RpcCallbackImpl()
            // note: synchronous call
            instance.getResult(controller, request, rpcCallback)
            rpcCallback.get()
          }
        }

        val future = executor.submit(call)
        calls.add(future)

        // block on the result
        val response = try { future.get() } catch {
          case e @ (_ : InterruptedException | _ : InterruptedIOException | _: CancellationException) =>
            logger.warn("Cancelling remote coprocessor call")
            controller.startCancel()
            null
        }

        calls.remove(future)

        if (controller.failed()) {
          logger.error(s"Controller failed with error:\n${controller.errorText()}")
          throw new IOException(controller.errorText())
        }

        response
      }
    }

    new CloseableIterator[ByteString] {

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

      override def close(): Unit = calls.asScala.foreach(_.cancel(true))
    }
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
