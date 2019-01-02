/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.utils.stats

import java.io.{InputStream, FilterInputStream, IOException}

/**
 * Counting input stream from guava, extended to provide reset of count
 */
class CountingInputStream(in: InputStream) extends FilterInputStream(in) {

  private var count: Long = 0L
  private var mark: Long = -1

  /** Returns the number of bytes read. */
  def getCount: Long = count
  def resetCount(): Unit = count = 0L

  override def read: Int = {
    val result: Int = in.read
    if (result != -1) {
      count += 1
    }
    result
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val result: Int = in.read(b, off, len)
    if (result != -1) {
      count += result
    }
    result
  }

  override def skip(n: Long): Long = {
    val result: Long = in.skip(n)
    count += result
    result
  }

  override def mark(readlimit: Int): Unit = {
    in.mark(readlimit)
    mark = count
  }

  override def reset(): Unit = {
    if (!in.markSupported) {
      throw new IOException("Mark not supported")
    }
    if (mark == -1) {
      throw new IOException("Mark not set")
    }
    in.reset()
    count = mark
  }
}
