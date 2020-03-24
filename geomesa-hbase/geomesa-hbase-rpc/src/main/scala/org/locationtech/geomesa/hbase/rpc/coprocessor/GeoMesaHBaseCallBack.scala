/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.rpc.coprocessor

import java.util.concurrent.LinkedBlockingQueue

import com.google.protobuf.ByteString
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.GeoMesaCoprocessorResponse
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.jts.geom.Envelope

class GeoMesaHBaseCallBack extends Callback[GeoMesaCoprocessorResponse] {
  val env = ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  var isDone = false
  var lastRow: Array[Byte] = _

  val result = new LinkedBlockingQueue[ByteString]()

  override def update(region: Array[Byte], row: Array[Byte], response: GeoMesaCoprocessorResponse): Unit = {
    val result =  Option(response).map(_.getPayloadList).orNull
    val lastscanned: ByteString = Option(response).map(_.getLastscanned).orNull
    //println(s"Lastscanned: ${response.getLastscanned}.  Condition: ${lastscanned != null && !lastscanned.isEmpty}  Lastscanned is not null: ${lastscanned != null}. lastscanned.isEmpty: ${lastscanned.isEmpty}")

    if (lastscanned != null && !lastscanned.isEmpty) {
      lastRow = lastscanned.toByteArray
      //println(s"Last Scanned is not null and is not Empty with length ${lastRow.length}: ${ByteArrays.printable(lastscanned.toByteArray)}")
      isDone = false
    } else {
      isDone = true
      ////println("Last scanned is null")
    }

    if (result != null) {
      //println(s"In update for region ${region} row: ${ByteArrays.printable(row)}")
      import scala.collection.JavaConversions._
      result.foreach { r =>
        val sf = new ScalaSimpleFeature(DensityScan.DensitySft, "", Array(GeometryUtils.zeroPoint))
        sf.getUserData.put(DensityScan.DensityValueKey, r.toByteArray)
        println("GeoMesaHBaseCallBack: Adding records to result")
        DensityScan.decodeResult(env, 500, 500)(sf).foreach { i =>  println(s"\tIn callback: $i") }
      }
      this.result.addAll(result)
    }
  }
}
