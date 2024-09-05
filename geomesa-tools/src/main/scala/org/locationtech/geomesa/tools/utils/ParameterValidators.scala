/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import com.beust.jcommander.{IParameterValidator, ParameterException}

object ParameterValidators {

  /**
   * The default jcommander PositiveInteger validator accepts zero, which isn't positive...
   */
  class PositiveInteger extends IParameterValidator {
    @throws[ParameterException]
    override def validate(name: String, value: String): Unit = {
      val n = value.toInt
      if (n < 1) {
        throw new ParameterException("Parameter " + name + " should be positive (found " + value + ")")
      }
    }
  }
}
