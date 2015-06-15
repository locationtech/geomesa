/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.apache.accumulo.core.data.Key
import org.apache.hadoop.io.Text

sealed trait TextExtractor {
  def extract(k: Key): String
  def extract(t: Text, offset: Int, bits: Int): String = t.toString.drop(offset).take(bits)
}

abstract class AbstractExtractor(offset: Int, bits: Int, f: Key => Text) extends TextExtractor {
  def extract(k: Key): String = extract(f(k), offset, bits)
}

case class RowExtractor(offset: Int, bits: Int)
    extends AbstractExtractor(offset, bits, (k: Key) => k.getRow)

case class ColumnFamilyExtractor(offset: Int, bits: Int)
    extends AbstractExtractor(offset, bits, (k: Key) => k.getColumnFamily)

case class ColumnQualifierExtractor(offset: Int, bits: Int)
    extends AbstractExtractor(offset, bits, (k: Key) => k.getColumnQualifier)

