/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.coprocessor

import com.google.protobuf.{RpcCallback, RpcController}

/**
  * An RpcController implementation for use here in this endpoint.
  */
class GeoMesaHBaseRpcController extends RpcController {

  private var _errorText: String = _
  private var _cancelled: Boolean = false
  private var _failed: Boolean = false

  override def isCanceled: Boolean = _cancelled
  override def failed(): Boolean = _failed
  override def errorText(): String = _errorText

  override def reset(): Unit = {
    _errorText = null
    _cancelled = false
    _failed = false
  }

  override def setFailed(errorText: String): Unit = {
    _failed = true
    _errorText = errorText
  }

  override def startCancel(): Unit = {
    _cancelled = true
  }

  override def notifyOnCancel(rpcCallback: RpcCallback[AnyRef]): Unit =
    throw new UnsupportedOperationException()
}
