/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.util.logging

import com.typesafe.scalalogging.slf4j.{Logger => Remapped}

import org.slf4j.{ Logger => Underlying }

object Logger {
  def apply(underlying: Underlying): Remapped = Remapped(underlying)
}
