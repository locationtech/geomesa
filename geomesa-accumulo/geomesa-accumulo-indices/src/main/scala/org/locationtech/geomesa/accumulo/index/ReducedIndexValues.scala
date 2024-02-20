/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.locationtech.geomesa.index.api.KeyValue

/**
 * Marker trait for reduced values used in a join index
 */
trait ReducedIndexValues {
  def indexValues: Seq[KeyValue]
}
