/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.conversions

import scala.reflect.ClassTag

object ScalaImplicits {

  implicit class RichIterator[T](val iter: Iterator[T]) extends AnyVal {
    def head: T = iter.next()
    def headOption: Option[T] = iter.find(_ != null)
    def mapWithIndex[B](f: (T, Int) => B): Iterator[B] = {
      var i = -1
      iter.map { v => i += 1; f(v, i) }
    }
  }

  implicit class RichArray[T](val array: Array[T]) extends AnyVal {
    def foreachIndex[U](f: (T, Int) => U): Unit = {
      var i = 0
      while (i < array.length) {
        f(array(i), i)
        i += 1
      }
    }

    def mapWithIndex[B](f: (T, Int) => B)(implicit ct: ClassTag[B]): Array[B] = {
      var i = -1
      array.map { v => i += 1; f(v, i) }
    }
  }
}
