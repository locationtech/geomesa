/** *********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
* ************************************************************************/

package org.locationtech.geomesa.hbase.coprocessor

import com.google.common.base.Throwables
import com.google.protobuf.ByteString
import com.google.protobuf.RpcCallback
import com.google.protobuf.RpcController
import com.google.protobuf.Service
import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.{Cell, Coprocessor, CoprocessorEnvironment}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.coprocessor.CoprocessorException
import org.apache.hadoop.hbase.coprocessor.CoprocessorService
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.protobuf.ResponseConverter
import org.apache.hadoop.hbase.regionserver.InternalScanner
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.hbase.filters.KryoLazyDensityFilter
import org.locationtech.geomesa.hbase.proto.KryoLazyDensityProto
import org.locationtech.geomesa.hbase.proto.KryoLazyDensityProto._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util

import org.locationtech.geomesa.process.utils.KryoLazyDensityUtils

import scala.collection.JavaConversions._

class KryoLazyDensityCoprocessor extends KryoLazyDensityService with Coprocessor with CoprocessorService {
  private var env : RegionCoprocessorEnvironment = null
  final private val logger = LoggerFactory.getLogger(classOf[KryoLazyDensityCoprocessor])

  @throws[IOException]
  def start(env: CoprocessorEnvironment) {
    if (env.isInstanceOf[RegionCoprocessorEnvironment])
      this.env = env.asInstanceOf[RegionCoprocessorEnvironment]
    else
      throw new CoprocessorException("Must be loaded on a table region!")
  }

  @throws[IOException]
  def stop(coprocessorEnvironment: CoprocessorEnvironment) {
  }

  def getService: Service = this

  def getDensity(controller: RpcController, request: KryoLazyDensityProto.DensityRequest, done: RpcCallback[KryoLazyDensityProto.DensityResponse]) {
    val outputsft = SimpleFeatureTypes.createType("result", "mapkey:string,weight:java.lang.Double")
    val output_serializer = new KryoFeatureSerializer(outputsft, SerializationOptions.withoutId)
    val byteFilterArray = request.getByteFilter.toByteArray
    var response : DensityResponse = null
    var scanner : InternalScanner = null
    try {
      val filter = KryoLazyDensityFilter.parseFrom(byteFilterArray).asInstanceOf[KryoLazyDensityFilter]
      val scan = new Scan
      scan.setFilter(filter)
      scanner = env.getRegion.getScanner(scan)
      val results = new java.util.ArrayList[Cell]
      var hasMore = false
      val resultMap : util.HashMap[Tuple2[Integer, Integer], java.lang.Double] = new util.HashMap[Tuple2[Integer, Integer], java.lang.Double]
      do {
        hasMore = scanner.next(results)
        for (cell <- results) {
          val sf = output_serializer.deserialize(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          val str = sf.getAttribute("mapkey").asInstanceOf[String]
          val key = filter.deserializeParameters(str).asInstanceOf[Tuple2[Integer, Integer]]
          val value = sf.getAttribute("weight").asInstanceOf[Double]
          resultMap.put(key, resultMap.getOrDefault(key, 0.0) + value)
        }
        results.clear()
      } while (hasMore)
      val result: Array[Byte] = KryoLazyDensityUtils.encodeResult(resultMap)
      response = DensityResponse.newBuilder.setSf(ByteString.copyFrom(result)).build
    } catch {
      case ioe: IOException => {
        ResponseConverter.setControllerException(controller, ioe)
      }
      case cnfe: ClassNotFoundException => {
        logger.error(Throwables.getStackTraceAsString(cnfe))
      }
      case dse: DeserializationException => {
        logger.error(Throwables.getStackTraceAsString(dse))
      }
    } finally IOUtils.closeQuietly(scanner)
    done.run(response)
  }
}