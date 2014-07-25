package geomesa.core.process.knn

import scala.collection.mutable

/**
 * Stub for a Bounded Priority Queue
 */
trait BoundedPriorityQueue[T] extends mutable.PriorityQueue[T] {
  def maxSize: Int

  def isFull: Boolean = !(length < maxSize)

  def getLast = take(maxSize).lastOption
}