/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

import org.apache.accumulo.core.data.Mutation
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter._
import org.locationtech.geomesa.accumulo.data._
import org.opengis.feature.simple.SimpleFeatureType

// TODO: Implement as traits and cache results to gain flexibility and speed-up.
// https://geomesa.atlassian.net/browse/GEOMESA-344
object RecordTable extends GeoMesaTable {

  override def supports(sft: SimpleFeatureType) = true

  override val suffix: String = "records"

  override def writer(sft: SimpleFeatureType): Option[FeatureToMutations] = {
    val rowIdPrefix = org.locationtech.geomesa.accumulo.index.getTableSharingPrefix(sft)
    val fn = (toWrite: FeatureToWrite) => {
      val m = new Mutation(getRowKey(rowIdPrefix, toWrite.feature.getID))
      m.put(SFT_CF, EMPTY_COLQ, toWrite.columnVisibility, toWrite.dataValue)
      Seq(m)
    }
    Some(fn)
  }

  override def remover(sft: SimpleFeatureType): Option[FeatureToMutations] = {
    val rowIdPrefix = org.locationtech.geomesa.accumulo.index.getTableSharingPrefix(sft)
    val fn = (toWrite: FeatureToWrite) => {
      val m = new Mutation(getRowKey(rowIdPrefix, toWrite.feature.getID))
      m.putDelete(SFT_CF, EMPTY_COLQ, toWrite.columnVisibility)
      Seq(m)
    }
    Some(fn)
  }

  def getRowKey(rowIdPrefix: String, id: String): String = rowIdPrefix + id
}
