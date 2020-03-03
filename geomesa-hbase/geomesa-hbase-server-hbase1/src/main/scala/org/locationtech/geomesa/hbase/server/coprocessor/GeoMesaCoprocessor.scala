/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.server.coprocessor

import java.io._

import com.google.protobuf.{RpcCallback, RpcController, Service}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.coprocessor.{CoprocessorException, CoprocessorService, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.regionserver.RegionScanner
import org.apache.hadoop.hbase.{Coprocessor, CoprocessorEnvironment}
import org.locationtech.geomesa.hbase.proto.GeoMesaProto
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.GeoMesaCoprocessorService
import org.locationtech.geomesa.hbase.server.common.CoprocessorScan

/**
 * Server-side coprocessor implementation for HBase 1.4
 */
class GeoMesaCoprocessor extends GeoMesaCoprocessorService
    with Coprocessor with CoprocessorService with CoprocessorScan {

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
    execute(controller, request, done)
  }

  override protected def getScanner(scan: Scan): RegionScanner = {
    // enable visibilities by delegating to the region server configured coprocessors
    env.getRegion.getCoprocessorHost.preScannerOpen(scan)
    env.getRegion.getScanner(scan)
  }
}
