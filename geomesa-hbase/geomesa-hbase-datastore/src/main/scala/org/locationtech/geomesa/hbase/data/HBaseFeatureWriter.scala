/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.hbase.data

import java.nio.charset.StandardCharsets
import java.util.Date

import com.google.common.primitives.{Bytes, Longs, Shorts}
import org.apache.hadoop.hbase.client.{Put, Table}
import org.geotools.data.FeatureWriter
import org.joda.time.DateTime
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


class HBaseFeatureWriter(sft: SimpleFeatureType, table: Table) extends FeatureWriter[SimpleFeatureType, SimpleFeature] {

  import org.locationtech.geomesa.utils.geotools.Conversions._

  import scala.collection.JavaConversions._

  private val SFC = new Z3SFC
  private var curFeature: SimpleFeature = null
  private val dtgIndex =
    sft.getAttributeDescriptors
      .zipWithIndex
      .find { case (ad, idx) => classOf[java.util.Date].equals(ad.getType.getBinding) }
      .map  { case (_, idx)  => idx }
      .getOrElse(throw new RuntimeException("No date attribute"))

  private val encoder = new KryoFeatureSerializer(sft)

  override def next(): SimpleFeature = {
    curFeature = new ScalaSimpleFeature("", sft)
    curFeature
  }

  override def remove(): Unit = ???

  override def hasNext: Boolean = true

  override def write(): Unit = {
    // write
    val geom = curFeature.point
    val x = geom.getX
    val y = geom.getY
    val dtg = new DateTime(curFeature.getAttribute(dtgIndex).asInstanceOf[Date])
    val weeks = epochWeeks(dtg)
    val prefix = Shorts.toByteArray(weeks.getWeeks.toShort)

    val secondsInWeek = secondsInCurrentWeek(dtg, weeks)
    val z3 = SFC.index(x, y, secondsInWeek)
    val z3idx = Longs.toByteArray(z3.z)

    val idBytes = curFeature.getID.getBytes(StandardCharsets.UTF_8)
    val row = Bytes.concat(prefix, z3idx)
    val idCQ = Bytes.concat(z3idx, idBytes)
    val serializedFeature = encoder.serialize(curFeature)
    val put =
      new Put(row)
        .addImmutable(HBaseDataStore.DATA_FAMILY_NAME, idCQ, serializedFeature)

    table.put(put)
    curFeature = null
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = {}
}