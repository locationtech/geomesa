/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import org.locationtech.geomesa.utils.geotools.Transform
import org.locationtech.geomesa.utils.geotools.Transform._

object RichTransform {

  implicit class RichTransform(val transform: Transform) extends AnyVal {
    def properties: Seq[String] = {
      transform match {
        case t: PropertyTransform => Seq(t.name)
        case t: RenameTransform => Seq(t.original)
        case t: ExpressionTransform => FilterHelper.propertyNames(t.expression, null)
      }
    }
  }
}
