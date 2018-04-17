/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.InputStream
import java.nio.ByteBuffer

class ByteBufferInputStream(buffer: ByteBuffer) extends InputStream {

  override def read(): Int = {
    if (!buffer.hasRemaining) { -1 } else {
      buffer.get() & 0xFF
    }
  }

  override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
    if (!buffer.hasRemaining) { -1 } else {
      val read = math.min(length, buffer.remaining)
      buffer.get(bytes, offset, read)
      read
    }
  }
}
