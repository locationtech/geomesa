/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor

import java.io._

import com.google.protobuf.{ByteString, RpcCallback, RpcController, Service}
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call
import org.apache.hadoop.hbase.client.{Scan, Table}
import org.apache.hadoop.hbase.coprocessor.{CoprocessorException, CoprocessorService, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.{FilterList, Filter => HFilter}
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback
import org.apache.hadoop.hbase.protobuf.{ProtobufUtil, ResponseConverter}
import org.apache.hadoop.hbase.{Cell, Coprocessor, CoprocessorEnvironment}
import org.geotools.data.Base64
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.hbase.coprocessor.utils.{GeoMesaHBaseCallBack, GeoMesaHBaseRpcController}
import org.locationtech.geomesa.hbase.proto.KryoLazyDensityProto
import org.locationtech.geomesa.hbase.proto.KryoLazyDensityProto._
import org.locationtech.geomesa.index.utils.KryoLazyDensityUtils
import org.locationtech.geomesa.index.utils.KryoLazyDensityUtils._
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable

class KryoLazyDensityCoprocessor extends KryoLazyDensityService with Coprocessor with CoprocessorService {

  private var env: RegionCoprocessorEnvironment = _

  @throws[IOException]
  def start(env: CoprocessorEnvironment): Unit = {
    env match {
      case e: RegionCoprocessorEnvironment => this.env = e
      case _ => throw new CoprocessorException("Must be loaded on a table region!")
    }
  }

  @throws[IOException]
  def stop(coprocessorEnvironment: CoprocessorEnvironment): Unit = {
  }

  def getService: Service = this

  def getDensity(controller: RpcController,
                 request: KryoLazyDensityProto.DensityRequest,
                 done: RpcCallback[KryoLazyDensityProto.DensityResponse]): Unit = {

    val options: Map[String, String] = deserializeOptions(request.getOptions.toByteArray)
    val sft = SimpleFeatureTypes.createType("input", options(SFT_OPT))
    val densityUtils = new KryoLazyDensityUtils {}
    val densityResult: DensityResult = densityUtils.initialize(options, sft)
    val scanList: List[Scan] = getScanFromOptions(options)
    val filterList: FilterList = getFilterListFromOptions(options)
    val serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId)

    def aggregateResult(sf: SimpleFeature, result: DensityResult): Unit = densityUtils.writeGeom(sf, result)

    val response: DensityResponse = try {

      scanList.foreach(scan => {
        scan.setFilter(filterList)
        // TODO: Explore use of MultiRangeFilter
        val scanner = env.getRegion.getScanner(scan)

        try {
          val results = new java.util.ArrayList[Cell]

          def processResults() = {
            for (cell <- results) {
              val sf = serializer.deserialize(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
              aggregateResult(sf, densityResult)
            }
            results.clear()
          }

          @tailrec
          def readScanner(): Unit = {
            if(scanner.next(results)) {
              processResults()
              readScanner()
            } else {
              processResults()
            }
          }

          readScanner()
        } catch {
          case t: Throwable => throw t
        } finally CloseQuietly(scanner)
      })

      val result: Array[Byte] = KryoLazyDensityUtils.encodeResult(densityResult)
      DensityResponse.newBuilder.setSf(ByteString.copyFrom(result)).build
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

object KryoLazyDensityCoprocessor {



  /**
    * Creates an iterator config for the kryo density iterator
    */
  def configure(sft: SimpleFeatureType,
                scan: Scan,
                filterList: FilterList,
                hints: Hints): Array[Byte] = {

    import org.apache.hadoop.hbase.util.Base64
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val is = mutable.Map.empty[String, String]

    val envelope = hints.getDensityEnvelope.get
    val (width, height) = hints.getDensityBounds.get

    is.put(ENVELOPE_OPT, s"${envelope.getMinX},${envelope.getMaxX},${envelope.getMinY},${envelope.getMaxY}")
    is.put(GRID_OPT, s"$width,$height")
    hints.getDensityWeight.foreach(is.put(WEIGHT_OPT, _))
    is.put(SFT_OPT, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
    is.put(FILTER_OPT, Base64.encodeBytes(filterList.toByteArray))
    is.put(SCAN_OPT, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    serializeOptions(is.toMap)
  }

  def bytesToFeatures(bytes : Array[Byte]): SimpleFeature = {
    val sf = new ScalaSimpleFeature("", DENSITY_SFT)
    sf.setAttribute(0, GeometryUtils.zeroPoint)
    sf.getUserData.put(DENSITY_VALUE, bytes)
    sf
  }

  /**
    * It gives the combined result of pairs received from different region servers value of a column for a given column family for the
    * given range. In case qualifier is null, a min of all values for the given
    * family is returned.
    *
    * @param table
    * @return HashMap result;
    * @throws Throwable
    */
  def execute(table: Table, options: Array[Byte]): List[ByteString] = {
    val requestArg: DensityRequest = DensityRequest.newBuilder().setOptions(ByteString.copyFrom(options)).build()

    val callBack: GeoMesaHBaseCallBack = new GeoMesaHBaseCallBack()

    table.coprocessorService(classOf[KryoLazyDensityService], null, null, new Call[KryoLazyDensityService, ByteString]() {
      override def call(instance: KryoLazyDensityService): ByteString = {
        val controller: RpcController = new GeoMesaHBaseRpcController()
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
      callBack
    )
    callBack.getResult
  }
}
