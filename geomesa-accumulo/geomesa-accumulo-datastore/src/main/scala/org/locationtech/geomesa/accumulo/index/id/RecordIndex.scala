/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.id

import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.opengis.feature.simple.SimpleFeatureType

object RecordIndex extends AccumuloFeatureIndex with RecordWritableIndex with RecordQueryableIndex {

  override val name: String = "records"

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.isTableEnabled(name)
  }

  def getRowKey(rowIdPrefix: String, id: String): String = rowIdPrefix + id
}
