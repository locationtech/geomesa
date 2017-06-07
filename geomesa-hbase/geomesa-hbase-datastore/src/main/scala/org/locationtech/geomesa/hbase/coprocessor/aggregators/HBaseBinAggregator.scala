/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.hbase.coprocessor.aggregators
import java.util.Date

import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
import org.locationtech.geomesa.index.utils.bin.{BinAggregatingUtils, ByteBufferResult}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import HBaseBinAggregator._

import scala.collection.mutable

class HBaseBinAggregator extends GeoMesaHBaseAggregator with BinAggregatingUtils {
  var internalResult: ByteBufferResult = _
  var sft: SimpleFeatureType = _

  override def init(options: Map[String, String]): Unit = {
    sft = SimpleFeatureTypes.createType("", options(SFT_OPT))
    internalResult = initBinAggregation(options)
  }

  override def aggregate(sf: SimpleFeature): Unit = writeBin(sf, internalResult)

  override def encodeResult(): Array[Byte] = encodeResult(internalResult)

  // TODD: Refactor - copied code
  def encodeResult(result: ByteBufferResult): Array[Byte] = encodeBinAggregationResult(internalResult)

  override def isPoints: Boolean = sft.isPoints
  override def isLines: Boolean = sft.isLines
}

object HBaseBinAggregator {
  // TODO Factor out!
  val SFT_OPT      = "sft"
  private val zeroPoint = WKTUtils.read("POINT(0 0)")
  private val BATCH_SIZE_OPT = "batch"
  private val SORT_OPT       = "sort"

  private val TRACK_OPT      = "track"
  private val GEOM_OPT       = "geom"
  private val DATE_OPT       = "dtg"
  private val LABEL_OPT      = "label"

  private val DATE_ARRAY_OPT = "dtg-array"

  def bytesToFeatures(bytes : Array[Byte]): SimpleFeature = {
    val sf = new ScalaSimpleFeature("", BinaryOutputEncoder.BinEncodedSft)
    sf.setAttribute(1, zeroPoint)
    sf.setAttribute(BIN_ATTRIBUTE_INDEX, bytes)
    sf
  }

  def configure(sft: SimpleFeatureType,
                hints: Hints): Map[String, String] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._

    val is = mutable.Map.empty[String, String]
    is.put(GeoMesaHBaseAggregator.AGGREGATOR_CLASS, classOf[HBaseBinAggregator].getName)

    val trackId = hints.getBinTrackIdField
    val geom = hints.getBinGeomField.getOrElse(sft.getGeomField)
    val dtg = hints.getBinDtgField.orElse(sft.getDtgField)
    val label = hints.getBinLabelField
    val batchSize = hints.getBinBatchSize
    val sort = hints.isBinSorting
    //val sampling = hints.getSampling

    is.put(BATCH_SIZE_OPT, batchSize.toString)
    is.put(TRACK_OPT, sft.indexOf(trackId).toString)
    is.put(GEOM_OPT, sft.indexOf(geom).toString)
    val dtgIndex = dtg.map(sft.indexOf).getOrElse(-1)
    is.put(DATE_OPT, dtgIndex.toString)
    if (sft.isLines && dtgIndex != -1 && sft.getDescriptor(dtgIndex).isList &&
      classOf[Date].isAssignableFrom(sft.getDescriptor(dtgIndex).getListType())) {
      is.put(DATE_ARRAY_OPT, "true")
    }

    //    sampling.foreach(SamplingIterator.configure(is, sft, _))
    label.foreach(l => is.put(LABEL_OPT, sft.indexOf(l).toString))
    is.put(SORT_OPT, sort.toString)
    is.put(SFT_OPT, SimpleFeatureTypes.encodeType(sft, includeUserData = true))

    is.toMap
  }
}