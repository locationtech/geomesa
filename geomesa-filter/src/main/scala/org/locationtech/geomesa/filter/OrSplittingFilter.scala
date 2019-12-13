/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import org.geotools.filter.visitor.DefaultFilterVisitor
import org.opengis.filter._

import scala.collection.JavaConversions._

class OrSplittingFilter extends DefaultFilterVisitor {

  // This function really returns a Seq[Filter].
  override def visit(filter: Or, data: scala.Any): AnyRef = {
    filter.getChildren.flatMap { subfilter =>
      this.visit(subfilter, data)
    }
  }

  def visit(filter: Filter, data: scala.Any): Seq[Filter] = {
    filter match {
      case o: Or => visit(o, data).asInstanceOf[Seq[Filter]]
      case _     => Seq(filter)
    }
  }
}
