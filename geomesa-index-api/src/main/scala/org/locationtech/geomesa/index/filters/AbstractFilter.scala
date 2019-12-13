/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import org.locationtech.geomesa.index.iterators.ConfiguredScan

trait AbstractFilter extends ConfiguredScan {

  /**
    * Accept a row. In general, not all fields need to be populated/examined.
    *
    * @param row row
    * @param rowOffset row offset
    * @param rowLength row length
    * @param value value
    * @param valueOffset value offset
    * @param valueLength value length
    * @param timestamp timestamp
    * @return
    */
  def accept(row: Array[Byte],
             rowOffset: Int,
             rowLength: Int,
             value: Array[Byte],
             valueOffset: Int,
             valueLength: Int,
             timestamp: Long): Boolean
}
