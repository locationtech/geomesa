/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.attribute

import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.opengis.feature.simple.SimpleFeatureType

trait AttributeSplittable {
  def configureSplits(sft: SimpleFeatureType, ds: AccumuloDataStore): Unit
}
