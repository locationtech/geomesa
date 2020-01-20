/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.util.concurrent.atomic.AtomicInteger

import javax.annotation.concurrent.ThreadSafe
import org.apache.commons.io.FilenameUtils

/**
  * Creates unique file names generated from a base name, by appending a sequence number
  * before the file extension.
  *
  * For example, given the file name 'foo.txt', will return 'foo_000.txt', 'foo_001.txt', etc.
  * If the iterator exceeds the specified number of digits, it will start to append additional
  * digits to ensure uniqueness, e.g. 'foo_999.txt', 'foo_1000.txt', 'foo_1001.txt', etc.
  *
  * @param path file name path
  * @param digits number of digits used to format the sequence number
  */
@ThreadSafe
class IncrementingFileName(path: String, digits: Int = 3) extends Iterator[String] {

  private val i = new AtomicInteger(0)
  private val format = s"_%0${digits}d"
  private val (prefix, suffix) = PathUtils.getBaseNameAndExtension(path)

  override def hasNext: Boolean = true

  override def next(): String = s"$prefix${format.format(i.getAndIncrement())}$suffix"
}
