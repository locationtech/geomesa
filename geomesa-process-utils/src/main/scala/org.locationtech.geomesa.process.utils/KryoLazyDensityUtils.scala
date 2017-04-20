/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.process.utils

import com.vividsolutions.jts.geom.Envelope
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap}
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import scala.collection.mutable

object KryoLazyDensityUtils {
  val DENSITY_SFT: SimpleFeatureType = SimpleFeatureTypes.createType("density", "result:String,*geom:Point:srid=4326")
  val density_serializer: KryoFeatureSerializer  = new KryoFeatureSerializer(DENSITY_SFT, SerializationOptions.withoutId)
  /**
    * Encodes a sparse matrix into a byte array
    */
  def encodeResult(input: java.util.Map[(java.lang.Integer, java.lang.Integer), java.lang.Double]): Array[Byte] = {
    val result = input.asScala.toMap
    val output = KryoFeatureSerializer.getOutput(null)
    result.toList.groupBy(_._1._1).foreach { case (row, cols) =>
      output.writeInt(row, true)
      val x = cols.size
      output.writeInt(cols.size, true)
      cols.foreach { case (xy, weight) =>
        output.writeInt(xy._2, true)
        output.writeDouble(weight)
      }
    }
    output.toBytes
  }

  /**
    * Encodes a sparse matrix into a byte array
    */
  def encodeResult(result: mutable.Map[(Int, Int), Double]): Array[Byte] = {
    val output = KryoFeatureSerializer.getOutput(null)
    result.toList.groupBy(_._1._1).foreach { case (row, cols) =>
      output.writeInt(row, true)
      output.writeInt(cols.size, true)
      cols.foreach { case (xy, weight) =>
        output.writeInt(xy._2, true)
        output.writeDouble(weight)
      }
    }
    output.toBytes
  }

  def bytesToFeatures(bytes : Array[Byte]): SimpleFeature = {
    val sf = new ScalaSimpleFeature("", DENSITY_SFT)
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    sf.values(0) = bytes
    sf
  }

  type DensityResult = mutable.Map[(Int, Int), Double]
  type GridIterator = (SimpleFeature) => Iterator[(Double, Double, Double)]

  /**
    * Returns a mapping of simple features (returned from a density query) to weighted points in the
    * form of (x, y, weight)
    */
  def decodeResult(envelope: Envelope, gridWidth: Int, gridHeight: Int): GridIterator =
    decodeResult(new GridSnap(envelope, gridWidth, gridHeight))

  /**
    * Decodes a result feature into an iterator of (x, y, weight)
    */
  def decodeResult(gridSnap: GridSnap)(sf: SimpleFeature): Iterator[(Double, Double, Double)] = {
    val result = sf.getAttribute(0).asInstanceOf[Array[Byte]]
    val input = KryoFeatureSerializer.getInput(result, 0, result.length)
    new Iterator[(Double, Double, Double)]() {
      private var x = 0.0
      private var colCount = 0
      override def hasNext = input.position < input.limit
      override def next() = {
        if (colCount == 0) {
          x = gridSnap.x(input.readInt(true))
          colCount = input.readInt(true)
        }
        val y = gridSnap.y(input.readInt(true))
        val weight = input.readDouble()
        colCount -= 1
        (x, y, weight)
      }
    }
  }
}
