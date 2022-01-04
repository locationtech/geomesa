/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

object ConcurrentHashSet {

  def empty[T](): java.util.Set[T] = Collections.newSetFromMap(new ConcurrentHashMap[T, java.lang.Boolean]())

  def apply[T](initialCapacity: Int): java.util.Set[T] =
    Collections.newSetFromMap(new ConcurrentHashMap[T, java.lang.Boolean](initialCapacity))
}
