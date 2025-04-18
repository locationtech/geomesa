/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

import org.locationtech.jts.index.strtree.STRtree

class WrappedSTRtree[T](nodeCapacity:Int = 10) extends WrapperIndex[T,STRtree](
  indexBuider = () => new STRtree(nodeCapacity)
) with Serializable {

  override def size(): Int = index.size()

}