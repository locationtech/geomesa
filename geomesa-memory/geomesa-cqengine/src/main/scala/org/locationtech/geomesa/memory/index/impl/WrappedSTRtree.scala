/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.memory.index.impl

import org.locationtech.jts.index.strtree.STRtree

class WrappedSTRtree[T](nodeCapacity:Int = 10) extends WrapperIndex[T,STRtree](() => new STRtree(nodeCapacity))
    with Serializable {
  override def size(): Int = index.size()
}
