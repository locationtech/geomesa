/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils.bin

import com.typesafe.scalalogging.LazyLogging

/**
  * Sorts aggregated bin arrays
  */
object BinSorter extends LazyLogging {

  /**
    * If the length of an array to be sorted is less than this
    * constant, insertion sort is used in preference to Quicksort.
    *
    * This length is 'logical' length, so the array is really binSize * length
    */
  private val INSERTION_SORT_THRESHOLD = 3

  private val swapBuffers = new ThreadLocal[Array[Byte]]() {
    override def initialValue(): Array[Byte] = Array.ofDim[Byte](24) // the larger bin size
  }

  private val priorityOrdering = new Ordering[(Array[Byte], Int)]() {
    override def compare(x: (Array[Byte], Int), y: (Array[Byte], Int)): Int =
      BinSorter.compare(y._1, y._2, x._1, x._2) // reverse for priority queue
  }

  /**
    * Compares two bin chunks by date
    */
  def compare(left: Array[Byte], leftOffset: Int, right: Array[Byte], rightOffset: Int): Int =
    compareIntLittleEndian(left, leftOffset + 4, right, rightOffset + 4) // offset + 4 is dtg

  /**
    * Comparison based on the integer encoding used by ByteBuffer
    * original code is in private/protected java.nio packages
    */
  private def compareIntLittleEndian(left: Array[Byte],
                                     leftOffset: Int,
                                     right: Array[Byte],
                                     rightOffset: Int): Int = {
    val l3 = left(leftOffset + 3)
    val r3 = right(rightOffset + 3)
    if (l3 < r3) {
      return -1
    } else if (l3 > r3) {
      return 1
    }
    val l2 = left(leftOffset + 2) & 0xff
    val r2 = right(rightOffset + 2) & 0xff
    if (l2 < r2) {
      return -1
    } else if (l2 > r2) {
      return 1
    }
    val l1 = left(leftOffset + 1) & 0xff
    val r1 = right(rightOffset + 1) & 0xff
    if (l1 < r1) {
      return -1
    } else if (l1 > r1) {
      return 1
    }
    val l0 = left(leftOffset) & 0xff
    val r0 = right(rightOffset) & 0xff
    if (l0 == r0) {
      0
    } else if (l0 < r0) {
      -1
    } else {
      1
    }
  }

  /**
    * Takes a sequence of (already sorted) aggregates and combines them in a final sort. Uses
    * a priority queue to compare the head element across each aggregate.
    */
  def mergeSort(aggregates: Iterator[Array[Byte]], binSize: Int): Iterator[(Array[Byte], Int)] = {
    if (aggregates.isEmpty) {
      return Iterator.empty
    }
    val queue = new scala.collection.mutable.PriorityQueue[(Array[Byte], Int)]()(priorityOrdering)
    val sizes = scala.collection.mutable.ArrayBuffer.empty[Int]
    while (aggregates.hasNext) {
      val next = aggregates.next()
      sizes.append(next.length / binSize)
      queue.enqueue((next, 0))
    }

    logger.debug(s"Got back ${queue.length} aggregates with an average size of ${sizes.sum / sizes.length}" +
      s" chunks and a median size of ${sizes.sorted.apply(sizes.length / 2)} chunks")

    new Iterator[(Array[Byte], Int)] {
      override def hasNext: Boolean = queue.nonEmpty
      override def next(): (Array[Byte], Int) = {
        val (aggregate, offset) = queue.dequeue()
        if (offset < aggregate.length - binSize) {
          queue.enqueue((aggregate, offset + binSize))
        }
        (aggregate, offset)
      }
    }
  }

  /**
    * Performs a merge sort into a new byte array
    */
  def mergeSort(left: Array[Byte], right: Array[Byte], binSize: Int): Array[Byte] = {
    if (left.length == 0) {
      return right
    } else if (right.length == 0) {
      return left
    }
    val result = Array.ofDim[Byte](left.length + right.length)
    var (leftIndex, rightIndex, resultIndex) = (0, 0, 0)

    while (leftIndex < left.length && rightIndex < right.length) {
      if (compare(left, leftIndex, right, rightIndex) > 0) {
        System.arraycopy(right, rightIndex, result, resultIndex, binSize)
        rightIndex += binSize
      } else {
        System.arraycopy(left, leftIndex, result, resultIndex, binSize)
        leftIndex += binSize
      }
      resultIndex += binSize
    }
    while (leftIndex < left.length) {
      System.arraycopy(left, leftIndex, result, resultIndex, binSize)
      leftIndex += binSize
      resultIndex += binSize
    }
    while (rightIndex < right.length) {
      System.arraycopy(right, rightIndex, result, resultIndex, binSize)
      rightIndex += binSize
      resultIndex += binSize
    }
    result
  }

  /**
    * Sorts the specified range of the array by Dual-Pivot Quicksort.
    * Modified version of java's DualPivotQuicksort
    *
    * @param bytes the array to be sorted
    * @param left the index of the first element, inclusive, to be sorted
    * @param right the index of the last element, inclusive, to be sorted
    */
  def quickSort(bytes: Array[Byte], left: Int, right: Int, binSize: Int): Unit =
    quickSort(bytes, left, right, binSize, leftmost = true)

  /**
    * Optimized for non-leftmost insertion sort
    */
  def quickSort(bytes: Array[Byte], left: Int, right: Int, binSize: Int, leftmost: Boolean): Unit = {

    val length = (right + binSize - left) / binSize

    if (length < INSERTION_SORT_THRESHOLD) {
      // Use insertion sort on tiny arrays
      if (leftmost) {
        // Traditional (without sentinel) insertion sort is used in case of the leftmost part
        var i = left + binSize
        while (i <= right) {
          var j = i
          val ai = getThreadLocalChunk(bytes, i, binSize)
          while (j > left && compare(bytes, j - binSize, ai, 0) > 0) {
            System.arraycopy(bytes, j - binSize, bytes, j, binSize)
            j -= binSize
          }
          if (j != i) {
            // we don't need to copy if nothing moved
            System.arraycopy(ai, 0, bytes, j, binSize)
          }
          i += binSize
        }
      } else {
        // optimized insertions sort when we know we have 'sentinel' elements to the left
        /*
         * Every element from adjoining part plays the role
         * of sentinel, therefore this allows us to avoid the
         * left range check on each iteration. Moreover, we use
         * the more optimized algorithm, so called pair insertion
         * sort, which is faster (in the context of Quicksort)
         * than traditional implementation of insertion sort.
         */
        // Skip the longest ascending sequence
        var i = left
        do {
          if (i >= right) {
            return
          }
        } while ({ i += binSize; compare(bytes, i , bytes, i - binSize) >= 0 })

        val a1 = Array.ofDim[Byte](binSize)
        val a2 = Array.ofDim[Byte](binSize)

        var k = i
        while ({ i += binSize; i } <= right) {
          if (compare(bytes, k, bytes, i) < 0) {
            System.arraycopy(bytes, k, a2, 0, binSize)
            System.arraycopy(bytes, i, a1, 0, binSize)
          } else {
            System.arraycopy(bytes, k, a1, 0, binSize)
            System.arraycopy(bytes, i, a2, 0, binSize)
          }
          while ({ k -= binSize; compare(a1, 0, bytes, k) < 0 }) {
            System.arraycopy(bytes, k, bytes, k + 2 * binSize, binSize)
          }
          k += binSize
          System.arraycopy(a1, 0, bytes, k + binSize, binSize)
          while ({ k -= binSize; compare(a2, 0, bytes, k) < 0 }) {
            System.arraycopy(bytes, k, bytes, k + binSize, binSize)
          }
          System.arraycopy(a2, 0, bytes, k + binSize, binSize)

          i += binSize
          k = i
        }

        var j = right
        val last = getThreadLocalChunk(bytes, j, binSize)
        while ({ j -= binSize; compare(last, 0, bytes, j) < 0 }) {
          System.arraycopy(bytes, j, bytes, j + binSize, binSize)
        }
        System.arraycopy(last, 0, bytes, j + binSize, binSize)
      }
      return
    }

    /*
     * Sort five evenly spaced elements around (and including) the
     * center element in the range. These elements will be used for
     * pivot selection as described below. The choice for spacing
     * these elements was empirically determined to work well on
     * a wide variety of inputs.
     */
    val seventh = (length / 7) * binSize

    val e3 = (((left + right) / binSize) / 2) * binSize // The midpoint
    val e2 = e3 - seventh
    val e1 = e2 - seventh
    val e4 = e3 + seventh
    val e5 = e4 + seventh

    def swap(left: Int, right: Int) = {
      val chunk = getThreadLocalChunk(bytes, left, binSize)
      System.arraycopy(bytes, right, bytes, left, binSize)
      System.arraycopy(chunk, 0, bytes, right, binSize)
    }

    // Sort these elements using insertion sort
    if (compare(bytes, e2, bytes, e1) < 0) { swap(e2, e1) }

    if (compare(bytes, e3, bytes, e2) < 0) { swap(e3, e2)
      if (compare(bytes, e2, bytes, e1) < 0) { swap(e2, e1) }
    }
    if (compare(bytes, e4, bytes, e3) < 0) { swap(e4, e3)
      if (compare(bytes, e3, bytes, e2) < 0) { swap(e3, e2)
        if (compare(bytes, e2, bytes, e1) < 0) {swap(e2, e1) }
      }
    }
    if (compare(bytes, e5, bytes, e4) < 0) { swap(e5, e4)
      if (compare(bytes, e4, bytes, e3) < 0) { swap(e4, e3)
        if (compare(bytes, e3, bytes, e2) < 0) { swap(e3, e2)
          if (compare(bytes, e2, bytes, e1) < 0) { swap(e2, e1) }
        }
      }
    }

    // Pointers
    var less  = left  // The index of the first element of center part
    var great = right // The index before the first element of right part

    if (compare(bytes, e1, bytes, e2) != 0 && compare(bytes, e2, bytes, e3) != 0 &&
      compare(bytes, e3, bytes, e4) != 0 && compare(bytes, e4, bytes, e5) != 0 ) {
      /*
       * Use the second and fourth of the five sorted elements as pivots.
       * These values are inexpensive approximations of the first and
       * second terciles of the array. Note that pivot1 <= pivot2.
       */
      val pivot1 = Array.ofDim[Byte](binSize)
      System.arraycopy(bytes, e2, pivot1, 0, binSize)
      val pivot2 = Array.ofDim[Byte](binSize)
      System.arraycopy(bytes, e4, pivot2, 0, binSize)

      /*
       * The first and the last elements to be sorted are moved to the
       * locations formerly occupied by the pivots. When partitioning
       * is complete, the pivots are swapped back into their final
       * positions, and excluded from subsequent sorting.
       */
      System.arraycopy(bytes, left, bytes, e2, binSize)
      System.arraycopy(bytes, right, bytes, e4, binSize)

      // Skip elements, which are less or greater than pivot values.
      while ({ less += binSize; compare(bytes, less, pivot1, 0) < 0 }) {}
      while ({ great -= binSize; compare(bytes, great, pivot2, 0) > 0 }) {}

      /*
       * Partitioning:
       *
       *   left part           center part                   right part
       * +--------------------------------------------------------------+
       * |  < pivot1  |  pivot1 <= && <= pivot2  |    ?    |  > pivot2  |
       * +--------------------------------------------------------------+
       *               ^                          ^       ^
       *               |                          |       |
       *              less                        k     great
       *
       * Invariants:
       *
       *              all in (left, less)   < pivot1
       *    pivot1 <= all in [less, k)     <= pivot2
       *              all in (great, right) > pivot2
       *
       * Pointer k is the first index of ?-part.
       */

      var k = less - binSize
      var loop = true
      while (loop && { k += binSize; k } <= great) {
        val ak = getThreadLocalChunk(bytes, k, binSize)
        if (compare(ak, 0, pivot1, 0) < 0) { // Move a[k] to left part
          System.arraycopy(bytes, less, bytes, k, binSize)
          System.arraycopy(ak, 0, bytes, less, binSize)
          less += binSize
        } else if (compare(ak, 0, pivot2, 0) > 0) { // Move a[k] to right part
          while (loop && compare(bytes, great, pivot2, 0) > 0) {
            if (great == k) {
              loop = false
            }
            great -= binSize
          }
          if (loop) {
            if (compare(bytes, great, pivot1, 0) < 0) { // a[great] <= pivot2
              System.arraycopy(bytes, less, bytes, k, binSize)
              System.arraycopy(bytes, great, bytes, less, binSize)
              less += binSize
            } else { // pivot1 <= a[great] <= pivot2
              System.arraycopy(bytes, great, bytes, k, binSize)
            }
            System.arraycopy(ak, 0, bytes, great, binSize)
            great -= binSize
          }
        }
      }

      // Swap pivots into their final positions
      System.arraycopy(bytes, less - binSize, bytes, left, binSize)
      System.arraycopy(pivot1, 0, bytes, less - binSize, binSize)
      System.arraycopy(bytes, great + binSize, bytes, right, binSize)
      System.arraycopy(pivot2, 0, bytes, great + binSize, binSize)

      // Sort left and right parts recursively, excluding known pivots
      quickSort(bytes, left, less - 2 * binSize, binSize, leftmost)
      quickSort(bytes, great + 2 * binSize, right, binSize, leftmost = false)

      /*
       * If center part is too large (comprises > 4/7 of the array),
       * swap internal pivot values to ends.
       */
      if (less < e1 && e5 < great) {

        // Skip elements, which are equal to pivot values.
        while (compare(bytes, less, pivot1, 0) == 0) { less += binSize }
        while (compare(bytes, great, pivot2, 0) == 0) { great -= binSize }

        /*
         * Partitioning:
         *
         *   left part         center part                  right part
         * +----------------------------------------------------------+
         * | == pivot1 |  pivot1 < && < pivot2  |    ?    | == pivot2 |
         * +----------------------------------------------------------+
         *              ^                        ^       ^
         *              |                        |       |
         *             less                      k     great
         *
         * Invariants:
         *
         *              all in (*,  less) == pivot1
         *     pivot1 < all in [less,  k)  < pivot2
         *              all in (great, *) == pivot2
         *
         * Pointer k is the first index of ?-part.
         */
        var k = less - binSize
        loop = true
        while (loop && { k += binSize; k } <= great) {
          val ak = getThreadLocalChunk(bytes, k, binSize)
          if (compare(ak, 0, pivot1, 0) == 0) { // Move a[k] to left part
            System.arraycopy(bytes, less, bytes, k, binSize)
            System.arraycopy(ak, 0, bytes, less, binSize)
            less += binSize
          } else if (compare(ak, 0, pivot2, 0) == 0) { // Move a[k] to right part
            while (loop && compare(bytes, great, pivot2, 0) == 0) {
              if (great == k) {
                loop = false
              }
              great -= binSize
            }
            if (loop) {
              if (compare(bytes, great, pivot1, 0) == 0) { // a[great] < pivot2
                System.arraycopy(bytes, less, bytes, k, binSize)
                System.arraycopy(bytes, great, bytes, less, binSize)
                less += binSize
              } else { // pivot1 < a[great] < pivot2
                System.arraycopy(bytes, great, bytes, k, binSize)
              }
              System.arraycopy(ak, 0, bytes, great, binSize)
              great -= binSize
            }
          }
        }
      }

      // Sort center part recursively
      quickSort(bytes, less, great, binSize, leftmost = false)
    } else { // Partitioning with one pivot

      /*
       * Use the third of the five sorted elements as pivot.
       * This value is inexpensive approximation of the median.
       */
      val pivot = Array.ofDim[Byte](binSize)
      System.arraycopy(bytes, e3, pivot, 0, binSize)

      /*
       * Partitioning degenerates to the traditional 3-way
       * (or "Dutch National Flag") schema:
       *
       *   left part    center part              right part
       * +-------------------------------------------------+
       * |  < pivot  |   == pivot   |     ?    |  > pivot  |
       * +-------------------------------------------------+
       *              ^              ^        ^
       *              |              |        |
       *             less            k      great
       *
       * Invariants:
       *
       *   all in (left, less)   < pivot
       *   all in [less, k)     == pivot
       *   all in (great, right) > pivot
       *
       * Pointer k is the first index of ?-part.
       */
      var k = less
      var loop = true
      while (loop && k <= great) {
        val comp = compare(bytes, k, pivot, 0)
        if (comp != 0) {
          val ak = getThreadLocalChunk(bytes, k, binSize)
          if (comp < 0) { // Move a[k] to left part
            System.arraycopy(bytes, less, bytes, k, binSize)
            System.arraycopy(ak, 0, bytes, less, binSize)
            less += binSize
          } else { // a[k] > pivot - Move a[k] to right part
            while (loop && compare(bytes, great, pivot, 0) > 0) {
              if (k == great) {
                loop = false
              }
              great -= binSize
            }
            if (loop) {
              if (compare(bytes, great, pivot, 0) < 0) { // a[great] <= pivot
                System.arraycopy(bytes, less, bytes, k, binSize)
                System.arraycopy(bytes, great, bytes, less, binSize)
                less += binSize
              } else { // a[great] == pivot
                System.arraycopy(bytes, great, bytes, k, binSize)
              }
              System.arraycopy(ak, 0, bytes, great, binSize)
              great -= binSize
            }
          }
        }
        k += binSize
      }

      /*
       * Sort left and right parts recursively.
       * All elements from center part are equal
       * and, therefore, already sorted.
       */
      quickSort(bytes, left, less - binSize, binSize, leftmost)
      quickSort(bytes, great + binSize, right, binSize, leftmost = false)
    }
  }

  // take care - uses thread-local state
  private def getThreadLocalChunk(bytes: Array[Byte], offset: Int, binSize: Int): Array[Byte] = {
    val chunk = swapBuffers.get()
    System.arraycopy(bytes, offset, chunk, 0, binSize)
    chunk
  }
}