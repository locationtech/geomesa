/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.utils

import com.google.protobuf.{RpcCallback, RpcController}

/**
  * An RpcController implementation for use here in this endpoint.
  */
class GeoMesaHBaseRpcController extends RpcController {

  private[utils] var errorText: String = _

  private var cancelled: Boolean = false

  private[utils] var failed: Boolean = false

  override def isCanceled: Boolean = this.cancelled

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
