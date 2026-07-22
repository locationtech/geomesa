/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package schemes

import org.apache.iceberg.{PartitionSpec, StructLike}
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.filter.function.XZ2Function
import org.locationtech.geomesa.fs.storage.core.schema.{ColumnName, ZValueField}
import org.locationtech.geomesa.fs.storage.core.schemes.SpatialScheme.SpatialPartitionSchemeFactory
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.jts.geom.Geometry

/**
 * XZ2 spatial scheme.
 *
 * This scheme uses a fixed high-resolution curve for indexing, then truncates the value to create partition groups,
 * in order to align with the iceberg 'truncate' transforms.
 *
 * Index values are (max) 7-digit hex-encoded longs, as we're only using 25 bits for the high-resolution curve,
 * we can fit it in 7 digits.
 *
 * @param attribute name of the attribute being partitioned
 * @param index index in the sft of the attribute being partitioned
 * @param bits number of bits of resolution used for partitioning
 */
case class XZ2Scheme(attribute: String, index: Int, bits: Int) extends SpatialScheme {

  import FilterHelper.ff

  require(bits % 4 == 0, s"Bit precision must be a multiple of 4, but received $bits")

  private val xz2 = XZ2SFC

  // partition level derived from bits parameter
  // each level adds 2 bits (4 quadrants)
  private val partitionLevel = (bits / 2).toShort
  // number of hex digits used to represent our z value - bits = (xz2.g - partitionLevel) * 2, then divide by 4 to get hex
  override protected val digits: Int = xz2.hexDigits - ((xz2.g - partitionLevel) / 2)

  override val name: String = s"${XZ2Scheme.name}:attribute=$attribute:bits=$bits"

  override val column: String = ZValueField.xz2FieldName(ColumnName.encode(attribute))

  override def getPartition(feature: SimpleFeature): PartitionKey = {
    val envelope = feature.getAttribute(index).asInstanceOf[Geometry].getEnvelopeInternal
    PartitionKey(name, xz2.hexEncode(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY).take(digits))
  }

  override def getCoveringFilter(partition: PartitionKey): Filter = {
    // TODO maybe we can improve this with *some* kind of bbox?
    val hexPrefix = partition.value
    val lower = hexPrefix.padTo(xz2.hexDigits, '0')
    val upper = hexPrefix.padTo(xz2.hexDigits, 'f')
    ff.between(ff.function(XZ2Function.FunctionName.getName), ff.literal(lower), ff.literal(upper))
  }

  override def spec(b: PartitionSpec.Builder): PartitionSpec.Builder =
    b.truncate(ZValueField.xz2FieldName(ColumnName.encode(attribute)), digits)

  override def getPartition(partition: StructLike, i: Int): PartitionKey = PartitionKey(name, partition.get(i, classOf[String]))

  override protected def hexRanges(bounds: Seq[(Double, Double, Double, Double)]): Seq[(String, String)] = {
    val max = QueryProperties.ScanRangesTarget.toInt
    xz2.ranges(bounds, max).map(r => (xz2.hexEncode(r.lower), xz2.hexEncode(r.upper)))
  }
}

object XZ2Scheme extends SpatialPartitionSchemeFactory[Geometry]("xz2") {
  override def buildPartitionScheme(bits: Int, geom: String, geomIndex: Int): PartitionScheme =
    XZ2Scheme(geom, geomIndex, bits)
}
