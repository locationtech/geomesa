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
import com.vividsolutions.jts.geom.Envelope
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.coprocessor.{CoprocessorException, CoprocessorService, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.filter.{FilterList, Filter => HFilter}
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.protobuf.{ProtobufUtil, ResponseConverter}
import org.apache.hadoop.hbase.regionserver.{InternalScanner, RegionScanner}
import org.apache.hadoop.hbase.{Cell, Coprocessor, CoprocessorEnvironment}
import org.geotools.data.Base64
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
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

  import KryoLazyDensityCoprocessor._

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

    def aggregateResult(sf: SimpleFeature, result: DensityResult): Unit = densityUtils.writeGeom(sf, result)

    val response: DensityResponse = try {

      val scanList: List[Scan] = if (options.containsKey(SCAN_OPT)) {
        val decoded = org.apache.hadoop.hbase.util.Base64.decode(options(SCAN_OPT))
        val clientScan = ClientProtos.Scan.parseFrom(decoded)
        List(ProtobufUtil.toScan(clientScan))
      } else {
        List(new Scan())
      }

      val filterList: FilterList = if (options.containsKey(FILTER_OPT)) {
        FilterList.parseFrom(Base64.decode(options(FILTER_OPT)))
      } else {
        new FilterList()
      }

      val serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId)

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

  private val SFT_OPT    = "sft"
  private val FILTER_OPT = "filter"
  private val SCAN_OPT   = "scan"

  /**
    * Creates an iterator config for the kryo density iterator
    */
  def configure(sft: SimpleFeatureType,
                scan: Scan,
                filterList: FilterList,
                hints: Hints): Map[String, String] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    val envelope = hints.getDensityEnvelope.get
    val (width, height) = hints.getDensityBounds.get
    val weight = hints.getDensityWeight
    configure(sft, scan, filterList, envelope, width, height, weight)
  }


  protected def configure(sft: SimpleFeatureType,
                          scan: Scan,
                          filterList: FilterList,
                          envelope: Envelope,
                          gridWidth: Int,
                          gridHeight: Int,
                          weightAttribute: Option[String]): Map[String, String] = {
    val is = mutable.Map.empty[String, String]

    is.put(ENVELOPE_OPT, s"${envelope.getMinX},${envelope.getMaxX},${envelope.getMinY},${envelope.getMaxY}")
    is.put(GRID_OPT, s"$gridWidth,$gridHeight")
    weightAttribute.foreach(is.put(WEIGHT_OPT, _))
    is.put(SFT_OPT, SimpleFeatureTypes.encodeType(sft, includeUserData = true))
    is.put(FILTER_OPT, Base64.encodeBytes(filterList.toByteArray))

    import org.apache.hadoop.hbase.util.Base64
    is.put(SCAN_OPT, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    is.toMap
  }

  @throws[IOException]
  def serializeOptions(map: Map[String, String]): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(map)
    oos.flush()
    val output = baos.toByteArray
    oos.close()
    baos.close()
    output
  }

  def deserializeOptions(bytes: Array[Byte]) = {
    val byteIn = new ByteArrayInputStream(bytes)
    val in = new ObjectInputStream(byteIn)
    val map = in.readObject.asInstanceOf[Map[String, String]]
    map
  }

  def bytesToFeatures(bytes : Array[Byte]): SimpleFeature = {
    val sf = new ScalaSimpleFeature("", DENSITY_SFT)
    sf.setAttribute(0, GeometryUtils.zeroPoint)
    sf.getUserData.put(DENSITY_VALUE, bytes)
    sf
  }
}
