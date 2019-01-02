/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
  def getCache(k: String): EnrichmentCache
}

object EvaluationContext {

  val InputFilePathKey = "inputFilePath"

  def empty: EvaluationContext = apply(IndexedSeq.empty, Array.empty, new DefaultCounter, Map.empty)

  def apply(names: IndexedSeq[String],
            values: Array[Any],
            counter: Counter,
            caches: Map[String, EnrichmentCache]): EvaluationContext =
    new EvaluationContextImpl(names, values, counter, caches)

  /**
    * Gets a global parameter map containing the input file path
    *
    * @param file input file path
    * @return
    */
  def inputFileParam(file: String): Map[String, AnyRef] = Map(InputFilePathKey -> file)

  /**
    * Evaluation context accessors
    *
    * @param ec context
    */
  implicit class RichEvaluationContext(val ec: EvaluationContext) extends AnyVal {
    def getInputFilePath: Option[String] = ec.indexOf(InputFilePathKey) match {
      case -1 => None
      case i  => Option(ec.get(i)).map(_.toString)
    }
    def setInputFilePath(path: String): Unit = ec.indexOf(InputFilePathKey) match {
      case -1 => throw new IllegalArgumentException(s"$InputFilePathKey is not present in execution context")
      case i  => ec.set(i, path)
    }
  }

}

class EvaluationContextImpl(names: IndexedSeq[String],
                            values: Array[Any],
                            val counter: Counter,
                            caches: Map[String, EnrichmentCache]) extends EvaluationContext {

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

  override def getCache(k: String): EnrichmentCache = caches(k)
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
