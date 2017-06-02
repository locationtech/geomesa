/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators.legacy

import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, WrappingIterator}

/**
 * This Iterator should wrap the AttributeIndexIterator. It will skip duplicate attributes to optimize
 * unique queries.
 */
class UniqueAttributeIterator extends WrappingIterator with LazyLogging {

  var seekColFamilies: util.Collection[ByteSequence] = null
  var seekInclusive: Boolean = false
  var seekRange: Range = null
  var outOfRange = false

  override def next() = {
    // skip to the next row key - this will skip over any duplicate attribute values
    val following = new Range(Range.followingPrefix(getTopKey.getRow), true, null, false)
    val range = Option(seekRange.clip(following))
    range match {
      case Some(r) => super.seek(r, seekColFamilies, seekInclusive)
      case None    => outOfRange = true
    }
  }

  override def hasTop() :Boolean = !outOfRange && super.hasTop()

  override def seek(range: Range, columnFamilies: util.Collection[ByteSequence], inclusive: Boolean) {
    // keep track of the current range we're handling
    outOfRange = false
    seekRange = range
    seekColFamilies = columnFamilies
    seekInclusive = inclusive
    super.seek(range, columnFamilies, inclusive)
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("UniqueAttributeIterator does not support deepCopy.")
}


