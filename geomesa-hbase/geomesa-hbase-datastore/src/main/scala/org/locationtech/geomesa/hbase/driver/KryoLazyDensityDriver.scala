/** *********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
* ************************************************************************/

package org.locationtech.geomesa.hbase.driver

import java.io.IOException
import java.util.{ArrayList, List}

import com.google.protobuf.{ByteString, RpcCallback, RpcController}
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.client.coprocessor.Batch._
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback
import org.locationtech.geomesa.hbase.proto.KryoLazyDensityProto._

import scala.collection.JavaConversions._

/**
  * An RpcController implementation for use here in this endpoint.
  */
class KryoLazyDensityRpcController extends RpcController {

  var errorText: String = _

  private var cancelled: Boolean = false

  var failed: Boolean = false

  override def isCanceled(): Boolean = this.cancelled

  override def reset(): Unit = {
    this.errorText = null
    this.cancelled = false
    this.failed = false
  }

  override def setFailed(errorText: String): Unit = {
    this.failed = true
    this.errorText = errorText
  }

  override def startCancel(): Unit = {
    this.cancelled = true
  }

  override def notifyOnCancel(rpcCallback: RpcCallback[AnyRef]): Unit = {
    throw new UnsupportedOperationException()
  }
}

class KryoLazyDensityDriver {

  /**
    * It gives the combined result of pairs received from different region servers value of a column for a given column family for the
    * given range. In case qualifier is null, a min of all values for the given
    * family is returned.
    *
    * @param table
    * @return HashMap result;
    * @throws Throwable
    */
  def kryoLazyDensityFilter(table: Table, filter: Array[Byte]): List[ByteString] = {
    val requestArg: DensityRequest = DensityRequest.newBuilder().setByteFilter(ByteString.copyFrom(filter)).build()

    class KryoLazyDensityFilterCallBack extends Callback[ByteString] {

      private var finalResult: List[ByteString] = new ArrayList()

      def getResult(): List[ByteString] = {
        val list: List[ByteString] = new ArrayList[ByteString]()
        for (s <- finalResult) {
          list.add(s)
        }
        list
      }

      override def update(region: Array[Byte], row: Array[Byte], result: ByteString): Unit = {
        synchronized {
          finalResult.add(result)
        }
      }
    }

    val kryoLazyDensityFilterCallBack: KryoLazyDensityFilterCallBack = new KryoLazyDensityFilterCallBack()
    table.coprocessorService(classOf[KryoLazyDensityService], null, null, new Call[KryoLazyDensityService, ByteString]() {
      override def call(instance: KryoLazyDensityService): ByteString = {
        val controller: RpcController = new KryoLazyDensityRpcController()
        val rpcCallback: BlockingRpcCallback[DensityResponse] =
          new BlockingRpcCallback[DensityResponse]()
        instance.getDensity(controller, requestArg, rpcCallback)
        val response: DensityResponse = rpcCallback.get
        if (controller.failed()) {
          throw new IOException(controller.errorText())
        }
        response.getSf
      }
    },
      kryoLazyDensityFilterCallBack
    )
    kryoLazyDensityFilterCallBack.getResult
  }

}
