/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor

import java.io._

import com.google.common.primitives.Bytes
import com.google.protobuf.{ByteString, RpcCallback, RpcController, Service}
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call
import org.apache.hadoop.hbase.client.{Scan, Table}
import org.apache.hadoop.hbase.coprocessor.{CoprocessorException, CoprocessorService, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback
import org.apache.hadoop.hbase.protobuf.ResponseConverter
import org.apache.hadoop.hbase.{Coprocessor, CoprocessorEnvironment}
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseAggregator
import org.locationtech.geomesa.hbase.coprocessor.utils.{GeoMesaHBaseCallBack, GeoMesaHBaseRpcController}
import org.locationtech.geomesa.hbase.proto.GeoMesaProto
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.{GeoMesaCoprocessorRequest, GeoMesaCoprocessorResponse, GeoMesaCoprocessorService}

import scala.collection.mutable.ArrayBuffer

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
    val options: Map[String, String] = deserializeOptions(request.getOptions.toByteArray)
    val aggregator = {
      val classname = options(GeoMesaCoprocessor.AggregatorClass)
      Class.forName(classname).newInstance().asInstanceOf[HBaseAggregator[_]]
    }
    aggregator.init(options)

    val scanList: List[Scan] = getScanFromOptions(options)
    val filterList: FilterList = getFilterListFromOptions(options)

    val response: GeoMesaCoprocessorResponse = try {
      val results = ArrayBuffer.empty[Array[Byte]]
      scanList.foreach { scan =>
        scan.setFilter(filterList)
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
      GeoMesaCoprocessorResponse.newBuilder.setSf(ByteString.copyFrom(Bytes.concat(results: _*))).build
    } catch {
      case ioe: IOException =>
        ResponseConverter.setControllerException(controller, ioe)
        null
      case cnfe: ClassNotFoundException =>
        throw cnfe
      case dse: DeserializationException =>
        throw dse
    }

    done.run(response)
  }
}

object GeoMesaCoprocessor {

  val AggregatorClass = "geomesa.hbase.aggregator.class"

  /**
    * Executes a geomesa coprocessor
    *
    * @param table table to execute against
    * @param options configuration options
    * @return serialized results
    */
  def execute(table: Table, options: Array[Byte]): List[ByteString] = {
    val requestArg = GeoMesaCoprocessorRequest.newBuilder().setOptions(ByteString.copyFrom(options)).build()

    val callable = new Call[GeoMesaCoprocessorService, ByteString]() {
      override def call(instance: GeoMesaCoprocessorService): ByteString = {
        val controller: RpcController = new GeoMesaHBaseRpcController()
        val rpcCallback = new BlockingRpcCallback[GeoMesaCoprocessorResponse]()
        instance.getResult(controller, requestArg, rpcCallback)
        val response: GeoMesaCoprocessorResponse = rpcCallback.get
        if (controller.failed()) {
          throw new IOException(controller.errorText())
        }
        response.getSf
      }
    }

    val callBack: GeoMesaHBaseCallBack = new GeoMesaHBaseCallBack()

    table.coprocessorService(classOf[GeoMesaCoprocessorService], null, null, callable, callBack)
    callBack.getResult
  }
}
