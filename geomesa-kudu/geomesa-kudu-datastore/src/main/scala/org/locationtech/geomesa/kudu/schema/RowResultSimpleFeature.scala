/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.schema

import org.apache.kudu.client.RowResult
import org.locationtech.geomesa.features.AbstractSimpleFeature.AbstractMutableSimpleFeature
import org.locationtech.geomesa.utils.collection.AtomicBitSet
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.identity.FeatureId

/**
  * Mutable simple feature backed by a row result. Lazily evaluates attributes when requested, and caches
  * for repeated access.
  *
  * @param sft simple feature type
  * @param readers column readers
  */
class RowResultSimpleFeature(sft: SimpleFeatureType,
                             fid: KuduColumnAdapter[String],
                             readers: IndexedSeq[KuduColumnAdapter[_ <: AnyRef]])
    extends AbstractMutableSimpleFeature(sft, "", null) {

  private var row: RowResult = _

  // tracks the attributes that have been read, plus the feature id in the last element
  private val bits = AtomicBitSet(sft.getAttributeCount + 1)
  private val attributes = Array.ofDim[AnyRef](sft.getAttributeCount)

  /**
    * Set the underlying data. Any prior changes are discarded.
    *
    * @param row row
    */
  def setRowResult(row: RowResult): Unit = {
    this.row = row
    bits.clear()
    getUserData.clear()
  }

  override def getIdentifier: FeatureId = {
    if (bits.add(sft.getAttributeCount)) {
      setId(fid.readFromRow(row))
    }
    super.getIdentifier
  }

  override def getID: String = {
    if (bits.add(sft.getAttributeCount)) {
      setId(fid.readFromRow(row))
    }
    super.getID
  }

  override def setAttributeNoConvert(index: Int, value: AnyRef): Unit = {
    bits.add(index)
    attributes(index) = value
  }

  override def getAttribute(index: Int): AnyRef = {
    if (bits.add(index)) {
      val value = readers(index).readFromRow(row)
      attributes(index) = value
      value
    } else {
      attributes(index)
    }
  }
}
