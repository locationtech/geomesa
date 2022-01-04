/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

package object conf {

  object FilterCompatibility extends Enumeration {
    type FilterCompatibility = Value
    val `1.3`: Value = Value("1.3")
    val `2.3`: Value = Value("2.3")
  }
}
