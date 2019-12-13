/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.id.legacy

import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.id.legacy.IdIndexV3.IdIndexKeySpaceV3
import org.locationtech.geomesa.index.index.id.{IdIndex, IdIndexKeySpace}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

// supports table sharing
class IdIndexV3 protected (ds: GeoMesaDataStore[_], sft: SimpleFeatureType, version: Int, mode: IndexMode)
    extends IdIndex(ds, sft, version, mode) {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def this(ds: GeoMesaDataStore[_], sft: SimpleFeatureType, mode: IndexMode) =
    this(ds, sft, 3, mode)

  override protected val tableNameKey: String = "table.id.v3"

  override val keySpace: IdIndexKeySpace = new IdIndexKeySpaceV3(sft, sft.getTableSharingBytes)
}

object IdIndexV3 {

  private val sharding = Array.empty[Byte]

  class IdIndexKeySpaceV3(sft: SimpleFeatureType, override val sharing: Array[Byte]) extends IdIndexKeySpace(sft) {

    private val rangePrefixes = if (sharing.isEmpty) { Seq.empty } else { Seq(sharing) }

    override val indexKeyByteLength: Right[(Array[Byte], Int, Int) => Int, Int] = Right(sharing.length)

    override def toIndexKey(writable: WritableFeature,
                            tier: Array[Byte],
                            id: Array[Byte],
                            lenient: Boolean): RowKeyValue[Array[Byte]] = {
      // create the byte array - allocate a single array up front to contain everything
      // ignore tier, not used here
      // note: the row already contains the feature ID, so we don't need to add anything else
      val bytes = if (sharing.isEmpty) { id } else {
        val array = Array.ofDim[Byte](1 + id.length)
        array(0) = sharing.head // sharing is only a single byte
        System.arraycopy(id, 0, array, 1, id.length)
        array
      }

      SingleRowKeyValue(bytes, sharing, IdIndexV3.sharding, id, tier, id, writable.values)
    }

    override def getRangeBytes(ranges: Iterator[ScanRange[Array[Byte]]], tier: Boolean): Iterator[ByteRange] = {
      if (rangePrefixes.isEmpty) {
        ranges.map {
          case SingleRowRange(row) => SingleRowByteRange(row)
          case r => throw new IllegalArgumentException(s"Unexpected range type $r")
        }
      } else {
        ranges.flatMap {
          case SingleRowRange(row) => rangePrefixes.map(p => SingleRowByteRange(ByteArrays.concat(p, row)))
          case r => throw new IllegalArgumentException(s"Unexpected range type $r")
        }
      }
    }
  }
}
