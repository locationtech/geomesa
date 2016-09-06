/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z3

import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.opengis.feature.simple.SimpleFeatureType

object XZ3Index extends AccumuloFeatureIndex with XZ3WritableIndex with XZ3QueryableIndex {

  val Precision: Short = 12 // precision of our SFCs

  override val name: String = "xz3"

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.getSchemaVersion > 9 && sft.nonPoints && sft.getDtgField.isDefined && sft.isTableEnabled(name)
  }
}
