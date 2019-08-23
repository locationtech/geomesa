/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

import org.locationtech.geomesa.utils.collection.IntBitSet

package object kryo {

  /**
    * Calculates the size (in bytes) required to store metadata for each feature, which currently consists
    * of offsets for each attribute value and user data, and a bit mask for holding nulls
    *
    * Size required is:
    *   `bytesPerNumber` bytes per attribute for relative offset
    *   + `bytesPerNumber` bytes for user data offset
    *   + 4 bytes per bit mask for nulls
    *
    * @param count number of attributes
    * @param bytesPerNumber number of bytes used to store each offset
    * @return
    */
  def metadataSize(count: Int, bytesPerNumber: Int = 2): Int =
    bytesPerNumber * (count + 1) + (IntBitSet.size(count) * 4)
}
