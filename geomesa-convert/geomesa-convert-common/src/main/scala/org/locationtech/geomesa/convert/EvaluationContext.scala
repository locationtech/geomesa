/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

trait EvaluationContext {
  def get(i: Int): Any
  def set(i: Int, v: Any): Unit
  def indexOf(n: String): Int
  def clear(): Unit
  def counter: Counter
}

object EvaluationContext {
  def empty: EvaluationContext = apply(IndexedSeq.empty, Array.empty, new DefaultCounter)
  def apply(names: IndexedSeq[String], values: Array[Any], counter: Counter): EvaluationContext =
    new EvaluationContextImpl(names, values, counter)
}

class EvaluationContextImpl(names: IndexedSeq[String], values: Array[Any], val counter: Counter)
  extends EvaluationContext {
  // check to see what global variables have been set
  // global variables are at the end of the array
  private val globalValuesOffset = values.takeWhile(_ == null).length

  def get(i: Int): Any = values(i)
  def set(i: Int, v: Any): Unit = values(i) = v
  def clear(): Unit = {
    var i: Int = 0
    while (i < globalValuesOffset) { values(i) = null; i += 1 }
  }
  def indexOf(n: String): Int = names.indexOf(n)
}

trait Counter {
  def incSuccess(i: Long = 1): Unit
  def getSuccess: Long

  def incFailure(i: Long = 1): Unit
  def getFailure: Long

  // For things like Avro think of this as a recordCount as well
  def incLineCount(i: Long = 1): Unit
  def getLineCount: Long
  def setLineCount(i: Long)
}

class DefaultCounter extends Counter {
  private var s: Long = 0
  private var f: Long = 0
  private var c: Long = 0

  override def incSuccess(i: Long = 1): Unit = s += i
  override def getSuccess: Long = s

  override def incFailure(i: Long = 1): Unit = f += i
  override def getFailure: Long = f

  override def incLineCount(i: Long = 1): Unit = c += i
  override def getLineCount: Long = c
  override def setLineCount(i: Long): Unit = c = i
}