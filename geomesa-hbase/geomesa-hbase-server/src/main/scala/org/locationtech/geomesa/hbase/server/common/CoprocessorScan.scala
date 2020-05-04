/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.server.common

import java.io.{IOException, InterruptedIOException}
import java.util.Base64

import com.google.protobuf.{ByteString, RpcCallback, RpcController}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.regionserver.RegionScanner
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter
import org.locationtech.geomesa.hbase.proto.GeoMesaProto
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.GeoMesaCoprocessorResponse
import org.locationtech.geomesa.hbase.rpc.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.hbase.server.common.CoprocessorScan.Aggregator
import org.locationtech.geomesa.index.iterators.AggregatingScan
import org.locationtech.geomesa.index.iterators.AggregatingScan.AggregateCallback
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.WithClose

import scala.util.control.NonFatal

/**
 * Common trait for server side coprocessor class
 */
trait CoprocessorScan extends StrictLogging {

  import scala.collection.JavaConverters._

  /**
   * Gets a scanner for the current region
   *
   * @param scan scan
   * @return
   */
  protected def getScanner(scan: Scan): RegionScanner

  /**
   * Execute an RPC request against this coprocessor
   *
   * @param controller controller
   * @param request request
   * @param done callback
   */
  protected def execute(
      controller: RpcController,
      request: GeoMesaProto.GeoMesaCoprocessorRequest,
      done: RpcCallback[GeoMesaProto.GeoMesaCoprocessorResponse]): Unit = {

    val results = GeoMesaCoprocessorResponse.newBuilder()

    if (request.getVersion != CoprocessorScan.AllowableRequestVersion) {
      // We cannot handle this request.
      // Immediately return an empty response indicating the highest response version
      logger.error(
        s"Got a coprocessor request with version ${request.getVersion}, " +
            s"but can only handle ${CoprocessorScan.AllowableRequestVersion}.")
      results.setVersion(CoprocessorScan.AllowableRequestVersion)
      done.run(results.build)
    } else {
      try {
        val options = GeoMesaCoprocessor.deserializeOptions(request.getOptions.toByteArray)
        val timeout = options.get(GeoMesaCoprocessor.TimeoutOpt).map(_.toLong)
        val yieldPartialResults = options.get(GeoMesaCoprocessor.YieldOpt).exists(_.toBoolean)
        if (!controller.isCanceled && timeout.forall(_ > System.currentTimeMillis())) {
          val clas = options(GeoMesaCoprocessor.AggregatorClass)
          val aggregator = Class.forName(clas).newInstance().asInstanceOf[Aggregator]
          logger.debug(s"Initializing aggregator $aggregator.")
          aggregator.init(options)

          val scan = {
            val bytes = Base64.getDecoder.decode(options(GeoMesaCoprocessor.ScanOpt))
            ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(bytes))
          }

          WithClose(getScanner(scan)) { scanner =>
            aggregator.setScanner(scanner)
            aggregator.aggregate(
              new CoprocessorAggregateCallback(controller, aggregator, results, yieldPartialResults, timeout))
          }
        }
      } catch {
        case _: InterruptedException | _: InterruptedIOException => // stop processing, but don't return an error to prevent retries
        case e: IOException => ResponseConverter.setControllerException(controller, e)
        case NonFatal(e) => ResponseConverter.setControllerException(controller, new IOException(e))
      }

      logger.debug(
        s"Results total size: ${results.getPayloadList.asScala.map(_.size()).sum}" +
          s"\n\tBatch sizes: ${results.getPayloadList.asScala.map(_.size()).mkString(", ")}")

      done.run(results.build)
    }
  }

  /**
   * Coprocessor aggregate callback
   *
   * @param controller controller
   * @param aggregator aggregator instance
   * @param results result builder
   * @param timeout timeout
   */
  private class CoprocessorAggregateCallback(
      controller: RpcController,
      aggregator: Aggregator,
      results: GeoMesaCoprocessorResponse.Builder,
      yieldPartialResults: Boolean,
      timeout: Option[Long]
    ) extends AggregateCallback {

    private val start = System.currentTimeMillis()

    logger.trace(
      s"Running first batch on aggregator $aggregator" +
        timeout.map(t => s" with remaining timeout ${t - System.currentTimeMillis()}ms").getOrElse(""))

    override def batch(bytes: Array[Byte]): Boolean = {
      results.addPayload(ByteString.copyFrom(bytes))
      continue()
    }

    override def partial(bytes: => Array[Byte]): Boolean = {
      if (continue()) { true } else {
        // add the partial results and stop scanning
        results.addPayload(ByteString.copyFrom(bytes))
        false
      }
    }

    private def continue(): Boolean = {
      if (controller.isCanceled) {
        logger.warn(s"Stopping aggregator $aggregator due to controller being cancelled")
        false
      } else if (timeout.exists(_ < System.currentTimeMillis())) {
        logger.warn(s"Stopping aggregator $aggregator due to timeout of ${timeout.get}ms")
        false
      } else if (yieldPartialResults) {
        val lastScanned = aggregator.getLastScanned
        logger.trace(
          s"Stopping aggregator $aggregator at row ${ByteArrays.printable(lastScanned)} and " +
              "returning intermediate results")
        // This check covers the HBase Version Aggregator case
        if (lastScanned != null && !lastScanned.isEmpty) {
          results.setLastScanned(ByteString.copyFrom(lastScanned))
        }
        false
      } else {
        logger.trace(
          s"Running next batch on aggregator $aggregator " +
            s"with elapsed time ${System.currentTimeMillis() - start}ms" +
            timeout.map(t => s" and remaining timeout ${t - System.currentTimeMillis()}ms").getOrElse(""))
        true
      }
    }
  }
}

object CoprocessorScan {
  type Aggregator = HBaseAggregator[_ <: AggregatingScan.Result]
  val AllowableRequestVersion = 1
}
