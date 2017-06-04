/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geotools.process.vector

import org.opengis.filter.Filter

object BBOXExpandingVisitor {

  /**
    * Provides access to package protected BBOXExpandingFilterVisitor
    *
    * @param filter filter
    * @param by amount to expand by
    * @return filter
    */
  def expand(filter: Filter, by: Double): Filter =
    filter.accept(new BBOXExpandingFilterVisitor(by, by, by, by), null).asInstanceOf[Filter]
}
