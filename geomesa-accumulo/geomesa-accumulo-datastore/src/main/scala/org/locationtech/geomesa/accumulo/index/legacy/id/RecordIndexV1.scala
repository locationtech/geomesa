/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.id

import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature, EMPTY_COLQ}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.index.conf.splitter.TableSplitter
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.SimpleFeatureType

case object RecordIndexV1 extends AccumuloFeatureIndex with RecordWritableIndex with RecordQueryableIndex {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val SFT_CF = new Text("SFT")

  override val name: String = "records"

  override val version: Int = 1

  override val serializedWithId: Boolean = true

  override val hasPrecomputedBins: Boolean = false

  override def supports(sft: SimpleFeatureType): Boolean = true

  override def getSplits(sft: SimpleFeatureType, partition: Option[String]): Seq[Array[Byte]] = {
    def nonEmpty(bytes: Seq[Array[Byte]]): Seq[Array[Byte]] = if (bytes.nonEmpty) { bytes } else { Seq(Array.empty) }

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val sharing = sft.getTableSharingBytes

    val splits = nonEmpty(TableSplitter.getSplits(sft, name, partition))

    for (split <- splits) yield {
      ByteArrays.concat(sharing, split)
    }
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val rowIdPrefix = sft.getTableSharingBytes
    (wf: AccumuloFeature) => {
      val mutation = new Mutation(ByteArrays.concat(rowIdPrefix, wf.idBytes))
      wf.fullValuesWithId.foreach(value => mutation.put(SFT_CF, EMPTY_COLQ, value.vis, value.value))
      Seq(mutation)
    }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val rowIdPrefix = sft.getTableSharingBytes
    (wf: AccumuloFeature) => {
      val mutation = new Mutation(ByteArrays.concat(rowIdPrefix, wf.idBytes))
      wf.fullValuesWithId.foreach(value => mutation.putDelete(SFT_CF, EMPTY_COLQ, value.vis))
      Seq(mutation)
    }
  }
}
