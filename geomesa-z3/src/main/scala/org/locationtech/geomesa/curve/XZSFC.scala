/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.curve

object XZSFC {

  val LogPointFive = math.log(0.5)

  /**
    * Ensures that the value is within the required bounds
    *
    * @param value value to bound
    * @param min minimum accepted value
    * @param max maximum accepted value
    * @return
    */
  def bounded(value: Double, min: Double, max: Double): Double =
    if (value < min) min else if (value > max) max else value
}
