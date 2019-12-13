/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.attribute

import org.locationtech.geomesa.filter.{Bounds, FilterValues}

case class AttributeIndexValues[T](attribute: String, i: Int, values: FilterValues[Bounds[T]], binding: Class[T])
