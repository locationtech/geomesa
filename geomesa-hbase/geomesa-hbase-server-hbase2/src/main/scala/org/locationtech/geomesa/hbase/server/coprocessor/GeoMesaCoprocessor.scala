/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.server.coprocessor

import java.util.Collections

import com.google.protobuf.{RpcCallback, RpcController, Service}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.coprocessor.{CoprocessorException, RegionCoprocessor, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.regionserver.{HRegion, RegionScanner}
import org.apache.hadoop.hbase.{Coprocessor, CoprocessorEnvironment}
import org.locationtech.geomesa.hbase.proto.GeoMesaProto
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.GeoMesaCoprocessorService
import org.locationtech.geomesa.hbase.server.common.CoprocessorScan

/**
 * Server-side coprocessor implementation for HBase 2.2
 */
class GeoMesaCoprocessor extends GeoMesaCoprocessorService with RegionCoprocessor with CoprocessorScan {

  private var env: RegionCoprocessorEnvironment = _

  override def getServices: java.lang.Iterable[Service] = Collections.singleton(this)

  override def start(env: CoprocessorEnvironment[_ <: Coprocessor]): Unit = {
    env match {
      case e: RegionCoprocessorEnvironment => this.env = e
      case _ => throw new CoprocessorException(s"Expected a region environment, got: $env")
    }
  }

  override def getResult(
      controller: RpcController,
      request: GeoMesaProto.GeoMesaCoprocessorRequest,
      done: RpcCallback[GeoMesaProto.GeoMesaCoprocessorResponse]): Unit = {
    execute(controller, request, done)
  }

  override protected def getScanner(scan: Scan): RegionScanner = {
    // enable visibilities by delegating to the region server configured coprocessors
    env.getRegion.asInstanceOf[HRegion].getCoprocessorHost.preScannerOpen(scan)
    env.getRegion.getScanner(scan)
  }
}
