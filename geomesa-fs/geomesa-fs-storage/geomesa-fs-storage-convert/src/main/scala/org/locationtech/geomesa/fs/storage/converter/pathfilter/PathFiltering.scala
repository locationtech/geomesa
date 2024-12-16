/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.pathfilter

import org.apache.hadoop.fs.PathFilter
import org.geotools.api.filter.Filter

trait PathFiltering {
  def apply(filter: Filter): PathFilter
}
