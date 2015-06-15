/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.util

import org.locationtech.geomesa.accumulo.data.{AccumuloConnectorCreator, AccumuloDataStore}
import org.locationtech.geomesa.accumulo.index.ExplainerOutputType
import org.opengis.feature.simple.SimpleFeatureType


class ExplainingConnectorCreator(ds: AccumuloDataStore, output: ExplainerOutputType)
    extends AccumuloConnectorCreator {

  override def getBatchScanner(table: String, numThreads: Int) = new ExplainingBatchScanner(output)

  override def getScanner(table: String) = new ExplainingScanner(output)

  override def getSpatioTemporalTable(sft: SimpleFeatureType) = ds.getSpatioTemporalTable(sft)

  override def getZ3Table(sft: SimpleFeatureType): String = ds.getZ3Table(sft)

  override def getAttributeTable(sft: SimpleFeatureType) = ds.getAttributeTable(sft)

  override def getRecordTable(sft: SimpleFeatureType) = ds.getRecordTable(sft)

  override def getSuggestedSpatioTemporalThreads(sft: SimpleFeatureType) =
    ds.getSuggestedSpatioTemporalThreads(sft)

  override def getSuggestedRecordThreads(sft: SimpleFeatureType) = ds.getSuggestedRecordThreads(sft)

  override def getSuggestedAttributeThreads(sft: SimpleFeatureType) = ds.getSuggestedAttributeThreads(sft)

  override def getSuggestedZ3Threads(sft: SimpleFeatureType) = ds.getSuggestedZ3Threads(sft)

  override def getGeomesaVersion(sft: SimpleFeatureType): Int = ds.getGeomesaVersion(sft)
}
