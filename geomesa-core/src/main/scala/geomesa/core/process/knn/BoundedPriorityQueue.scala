/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package geomesa.core.process.knn

import com.google.common.collect.MinMaxPriorityQueue

import scala.collection.GenTraversableOnce
import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom

/**
 * A simple implementation of a bounded priority queue, as a wrapper for the Guava MinMaxPriorityQueue
 *
 * Some methods have been added to make this appear similar to the Scala collections PriorityQueue
 *
 */

class BoundedPriorityQueue[T](val maxSize: Int)(implicit ord: Ordering[T])
  extends Iterable[T] {

  // note that ord.reverse is used in the constructor -- MinMaxPriorityQueue has natural ordering (min first)
  // while the Scala collections PriorityQueue uses reverse natural ordering (max first)
  val corePQ = MinMaxPriorityQueue.orderedBy(ord.reverse).maximumSize(maxSize).create[T]()

  override def isEmpty = !(corePQ.size > 0)

  def ++(xs: GenTraversableOnce[T]) = this.clone() ++= xs.seq

  override def clone() = new BoundedPriorityQueue[T](maxSize)(ord) ++= this.iterator

  def iterator = corePQ.iterator.asScala

  def enqueue(elems: T*) = this ++= elems

  def ++=(xs: GenTraversableOnce[T]) = { xs.foreach { this += _ }; this }

  def +=(single: T) = { corePQ.add(single); this }

  def dequeueAll[T1 >: T, That](implicit bf: CanBuildFrom[_, T1, That]): That = {
    val b = bf.apply()
    while (nonEmpty) { b += dequeue() }
    b.result()
  }

  def dequeue() = corePQ.poll()

  override def last = corePQ.peekLast

  override def head = corePQ.peek

  def clear() = corePQ.clear()

  override def toList = this.iterator.toList

  def isFull = !(size < maxSize)

  override def size = corePQ.size

}