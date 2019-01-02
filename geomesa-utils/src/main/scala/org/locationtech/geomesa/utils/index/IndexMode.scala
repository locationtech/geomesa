/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.index

/**
  * BitSet for indicating read/write mode of an index
  */
object IndexMode {

  private val ReadBit  = 1 // 01
  private val WriteBit = 2 // 10

  val Any       = new IndexMode(0)
  val Read      = new IndexMode(ReadBit)
  val Write     = new IndexMode(WriteBit)
  val ReadWrite = new IndexMode(ReadBit | WriteBit)

  class IndexMode(val flag: Int) extends AnyRef {
    def read: Boolean =  (flag & ReadBit)  != 0
    def write: Boolean = (flag & WriteBit) != 0
    def supports(m: IndexMode): Boolean = (this.read || !m.read) && (this.write || !m.write)
  }

  def apply(flag: Int): IndexMode = new IndexMode(flag)
}
