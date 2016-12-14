/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.id

import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType

case object RecordIndex extends AccumuloFeatureIndexType with RecordWritableIndex with RecordQueryableIndex {

  def getRowKey(rowIdPrefix: String, id: String): String = rowIdPrefix + id

  override val name: String = "records"

  override val version: Int = 2

  override val serializedWithId: Boolean = false

  override def supports(sft: SimpleFeatureType): Boolean = true

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val rowIdPrefix = sft.getTableSharingPrefix
    (wf: AccumuloFeature) => {
      val mutation = new Mutation(getRowKey(rowIdPrefix, wf.feature.getID))
      wf.fullValues.foreach(value => mutation.put(value.cf, value.cq, value.vis, value.value))
      Seq(mutation)
    }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val rowIdPrefix = sft.getTableSharingPrefix
    (wf: AccumuloFeature) => {
      val mutation = new Mutation(getRowKey(rowIdPrefix, wf.feature.getID))
      wf.fullValues.foreach(value => mutation.putDelete(value.cf, value.cq, value.vis))
      Seq(mutation)
    }
  }

}

case object RecordIndexV1 extends AccumuloFeatureIndexType with RecordWritableIndex with RecordQueryableIndex {

  private val SFT_CF = new Text("SFT")

  override val name: String = "records"

  override val version: Int = 1

  override val serializedWithId: Boolean = true

  override def supports(sft: SimpleFeatureType): Boolean = true

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val rowIdPrefix = sft.getTableSharingPrefix
    (wf: AccumuloFeature) => {
      val mutation = new Mutation(RecordIndex.getRowKey(rowIdPrefix, wf.feature.getID))
      wf.fullValuesWithId.foreach(value => mutation.put(SFT_CF, EMPTY_COLQ, value.vis, value.value))
      Seq(mutation)
    }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val rowIdPrefix = sft.getTableSharingPrefix
    (wf: AccumuloFeature) => {
      val mutation = new Mutation(RecordIndex.getRowKey(rowIdPrefix, wf.feature.getID))
      wf.fullValuesWithId.foreach(value => mutation.putDelete(SFT_CF, EMPTY_COLQ, value.vis))
      Seq(mutation)
    }
  }
}