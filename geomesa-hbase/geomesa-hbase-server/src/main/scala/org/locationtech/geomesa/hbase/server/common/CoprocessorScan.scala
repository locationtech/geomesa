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
import org.apache.hadoop.hbase.filter.{Filter, FilterList, MultiRowRangeFilter}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.regionserver.RegionScanner
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter
import org.locationtech.geomesa.hbase.proto.GeoMesaProto
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.GeoMesaCoprocessorResponse
import org.locationtech.geomesa.hbase.rpc.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.hbase.rpc.filter.LastReadFilter
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
    val lastRead = new LastReadFilter

    try {
      val options = GeoMesaCoprocessor.deserializeOptions(request.getOptions.toByteArray)
      val timeout = options.get(GeoMesaCoprocessor.TimeoutOpt).map(_.toLong)
      if (!controller.isCanceled && timeout.forall(_ > System.currentTimeMillis())) {
        val clas = options(GeoMesaCoprocessor.AggregatorClass)
        val aggregator = Class.forName(clas).newInstance().asInstanceOf[Aggregator]
        logger.debug(s"Initializing aggregator $aggregator with options ${options.mkString(", ")}")
        //aggregator.init(options)
        // JNH: Test with the below to see if partialResults are working.
        aggregator.init(options.updated("batch", "2"))

        val scan = ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(Base64.getDecoder.decode(options(GeoMesaCoprocessor.ScanOpt))))

//        println(s"Scan configured with start: ${ByteArrays.printable(scan.getStartRow)} and end: ${new String(scan.getStopRow)}")
//        println(s"Scan configured with filter: ${scan.getFilter}")
//        import scala.collection.JavaConversions._
//        println(s"Scan configured with MRRF: ${printMRRF(scan.getFilter)} ")

//        def printMRRF(filter: Filter): String = {
//
//          filter.asInstanceOf[FilterList].getFilters.get(0).asInstanceOf[MultiRowRangeFilter].getRowRanges.map {
//            rr =>
//              ByteArrays.printable(rr.getStartRow) + " : " + ByteArrays.printable(rr.getStopRow)
//          }.mkString(" , ")
//        }
        //scan.setFilter(new FilterList(lastRead, FilterList.parseFrom(Base64.getDecoder.decode(options(GeoMesaCoprocessor.FilterOpt)))))

        WithClose(getScanner(scan)) { scanner =>
          aggregator.setScanner(scanner)
          // Connect CoprocessorAggCallback to last scanned
          aggregator.aggregate(new CoprocessorAggregateCallback(controller, aggregator, results, timeout))
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
    //println(s"Read ${lastRead.count} and finished on row ${ByteArrays.printable(lastRead.lastRead)}")
    done.run(results.build)
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
                                              timeout: Option[Long]
                                            ) extends AggregateCallback {

    private val start = System.currentTimeMillis()
    private var count = 0

    logger.trace(s"Running first batch on aggregator $aggregator" +
      timeout.map(t => s" with remaining timeout ${t - System.currentTimeMillis()}ms").getOrElse(""))

    override def batch(bytes: Array[Byte]): Boolean = {
      results.addPayload(ByteString.copyFrom(bytes))
      continue()
    }

    override def partial(bytes: => Array[Byte]): Boolean = {
      // JNH: I don't understand the {true} case...
      if (continue()) { true } else {
        // add the partial results and stop scanning
        results.addPayload(ByteString.copyFrom(bytes))
//        println("CALL PARTIAL RESULTS")
        false
      }
    }

    private def continue(): Boolean = {
      count += 1
      if (count >= 10) {  // We've got 10 batches.  Let's return
        logger.warn(s"Stopping aggregator $aggregator due to having 10 batches!")
        results.setLastscanned(ByteString.copyFrom(aggregator.getLastScanned))
        println(s"Stopping aggregator $aggregator due to having 10 batches!")
        false
      } else if (controller.isCanceled) {
        logger.warn(s"Stopping aggregator $aggregator due to controller being cancelled")
        false
      } else if (timeout.exists(_ < System.currentTimeMillis())) {
        // In the 'partialResultsTimeout case, we would want to return
        //       results.setLastscanned(ByteString.copyFrom(aggregator.getLastScanned))
        logger.warn(s"Stopping aggregator $aggregator due to timeout of ${timeout.get}ms")
        false
      } else {
        logger.trace(s"Running next batch on aggregator $aggregator " +
          s"with elapsed time ${System.currentTimeMillis() - start}ms" +
          timeout.map(t => s" and remaining timeout ${t - System.currentTimeMillis()}ms").getOrElse(""))
        true
      }
    }
  }
}

object CoprocessorScan {
  type Aggregator = HBaseAggregator[_ <: AggregatingScan.Result]
}
