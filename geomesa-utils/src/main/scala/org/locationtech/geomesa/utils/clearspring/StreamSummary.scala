/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

/**
  * Adapted from:
  *
  * stream-lib
  * Copyright (C) 2011 Clearspring Technologies, Inc.
  * Copyright 2016 AddThis
  *
  * This product includes software developed by AddThis.
  *
  * This product also includes code adapted from:
  *
  * Apache Solr (http://lucene.apache.org/solr/)
  * Copyright 2014 The Apache Software Foundation
  *
  * Apache Mahout (http://mahout.apache.org/)
  * Copyright 2014 The Apache Software Foundation
  *
  */

package org.locationtech.geomesa.utils.clearspring

import java.util.Collections

import com.clearspring.analytics.stream.ITopK
import com.clearspring.analytics.util.{DoublyLinkedList, ListNode2}
import org.locationtech.geomesa.utils.clearspring.StreamSummary.{Bucket, Counter}

/**
  * Based on the `Space-Saving` algorithm and the `Stream-Summary`
  * data structure as described in:
  * `Efficient Computation of Frequent and Top-k Elements in Data Streams`
  * by Metwally, Agrawal, and Abbadi
  *
  * @param capacity maximum size (larger capacities improve accuracy)
  * @tparam T type of data in the stream to be summarized
  */
class StreamSummary[T] private (val capacity: Int,
                                private val counterMap: java.util.HashMap[T, ListNode2[Counter[T]]],
                                private val bucketList: DoublyLinkedList[Bucket[T]]) extends ITopK[T] {

  /**
    * Algorithm: `Space-Saving`
    *
    * @param item stream element (`e`)
    * @return false if item was already in the stream summary, true otherwise
    */
  override def offer(item: T): Boolean = offer(item, 1)

  /**
    * Algorithm: `Space-Saving`
    *
    * @param item stream element (`e`)
    * @return false if item was already in the stream summary, true otherwise
    */
  override def offer(item: T, increment: Int): Boolean = offerReturnAll(item, increment)._1

  def offer(item: T, increment: Long): Boolean = offerReturnAll(item, increment)._1

  /**
    * @param item stream element (`e`)
    * @return item dropped from summary if an item was dropped, null otherwise
    */
  def offerReturnDropped(item: T, increment: Long): T = offerReturnAll(item, increment)._2

  /**
    * @param item stream element (`e`)
    * @return (isNewItem, itemDropped) where isNewItem is the return value of offer() and itemDropped is
    *         null if no item was dropped
    */
  def offerReturnAll(item: T, increment: Long): (Boolean, T) = {
    var counterNode = counterMap.get(item)
    val isNewItem = counterNode == null
    var droppedItem: T = null.asInstanceOf[T]

    if (isNewItem) {
      if (size < capacity) {
        counterNode = bucketList.enqueue(new Bucket(0)).getValue.counterList.add(new Counter[T](bucketList.tail(), item))
      } else {
        val min = bucketList.first()
        counterNode = min.counterList.tail()
        val counter = counterNode.getValue
        droppedItem = counter.item
        counterMap.remove(droppedItem)
        counter.item = item
        counter.error = min.count
      }
      counterMap.put(item, counterNode)
    }

    incrementCounter(counterNode, increment)

    (isNewItem, droppedItem)
  }

  private def incrementCounter(counterNode: ListNode2[Counter[T]], increment: Long): Unit = {
    val counter = counterNode.getValue     // count_i
    val oldNode = counter.bucketNode
    val bucket = oldNode.getValue          // Let Bucket_i be the bucket of count_i
    bucket.counterList.remove(counterNode) // Detach count_i from Bucket_i's child-list
    counter.count += increment

    // Finding the right bucket for count_i
    // Because we allow a single call to increment count more than once, this may not be the adjacent bucket.
    var bucketNodePrev = oldNode
    var bucketNodeNext = bucketNodePrev.getNext
    var break = false
    while (!break && bucketNodeNext != null) {
      val bucketNext = bucketNodeNext.getValue // Let Bucket_i^+ be Bucket_i's neighbor of larger value
      if (counter.count == bucketNext.count) {
        bucketNext.counterList.add(counterNode)    // Attach count_i to Bucket_i^+'s child-list
        break = true
      } else if (counter.count > bucketNext.count) {
        bucketNodePrev = bucketNodeNext
        bucketNodeNext = bucketNodePrev.getNext  // Continue hunting for an appropriate bucket
      } else {
        // A new bucket has to be created
        bucketNodeNext = null
      }
    }

    if (bucketNodeNext == null) {
        val bucketNext = new Bucket[T](counter.count)
        bucketNext.counterList.add(counterNode)
        bucketNodeNext = bucketList.addAfter(bucketNodePrev, bucketNext)
    }
    counter.bucketNode = bucketNodeNext

    // Cleaning up
    if (bucket.counterList.isEmpty) {     // If Bucket_i's child-list is empty
      bucketList.remove(oldNode);         // Detach Bucket_i from the Stream-Summary
    }
  }

  override def peek(k: Int): java.util.List[T] = {
    val topK = new java.util.ArrayList[T](k)

    if (k > 0) {
      var bNode = bucketList.head()
      while (bNode != null) {
        val b = bNode.getValue
        val iter = b.counterList.iterator()
        while (iter.hasNext) {
          topK.add(iter.next.item)
          if (topK.size() == k) {
            return topK
          }
        }
        bNode = bNode.getPrev
      }
    }

    topK
  }

  def topK(k : Int): Iterator[(T, Long)] = new Iterator[(T, Long)] {
    private var i = 0
    private var bNode = bucketList.head()
    private var counters = nextCounters()

    override def hasNext: Boolean = i < k && (counters.hasNext || { counters = nextCounters(); counters.hasNext })

    override def next(): (T, Long) = {
      i += 1
      val c = counters.next
      (c.item, c.count)
    }

    private def nextCounters(): java.util.Iterator[Counter[T]] = {
      if (bNode == null) { Collections.emptyIterator() } else {
        val iter = bNode.getValue.counterList.iterator()
        bNode = bNode.getPrev
        if (iter.hasNext) {
          iter
        } else {
          nextCounters()
        }
      }
    }
  }

  def size: Int = counterMap.size

  def clear(): Unit = {
    counterMap.clear()
    while (!bucketList.isEmpty) {
      bucketList.remove(bucketList.head)
    }
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append('[')
    var bNode = bucketList.head()
    while (bNode != null) {
      val b = bNode.getValue
      sb.append('{')
      sb.append(b.count)
      sb.append(":[")
      val iter = b.counterList.iterator()
      while (iter.hasNext) {
        val c = iter.next
        sb.append('{')
        sb.append(c.item)
        sb.append(':')
        sb.append(c.error)
        sb.append("},")
      }
      if (b.counterList.size() > 0) {
        sb.deleteCharAt(sb.length() - 1)
      }
      sb.append("]},")
      bNode = bNode.getPrev
    }
    if (bucketList.size() > 0) {
      sb.deleteCharAt(sb.length() - 1)
    }
    sb.append(']')
    sb.toString()
  }
}

object StreamSummary {

  /**
    * Create an empty stream summary
    *
    * @param capacity capacity
    * @tparam T type param
    * @return stream summary
    */
  def apply[T](capacity: Int): StreamSummary[T] = {
    val counterMap = new java.util.HashMap[T, ListNode2[Counter[T]]]()
    val bucketList = new DoublyLinkedList[Bucket[T]]()
    new StreamSummary(capacity, counterMap, bucketList)
  }

  /**
    * Create a stream summary containing counts
    *
    * @param capacity capacity
    * @param counters counters - must be sorted high->low
    * @tparam T type param
    * @return stream summary
    */
  def apply[T](capacity: Int, counters: Seq[(T, Long)]): StreamSummary[T] = {
    val bucketList = new DoublyLinkedList[Bucket[T]]()
    val counterMap = new java.util.HashMap[T, ListNode2[Counter[T]]](counters.length)

    var currentBucket: Bucket[T] = null
    var currentBucketNode: ListNode2[Bucket[T]] = null

    counters.foreach { case (item, count) =>
      val c = new Counter[T](null, item, count)
      if (currentBucket == null || c.count != currentBucket.count) {
        currentBucket = new Bucket(c.count)
        currentBucketNode = bucketList.enqueue(currentBucket)
      }
      c.bucketNode = currentBucketNode
      counterMap.put(c.item, currentBucket.counterList.add(c))
    }

    new StreamSummary(capacity, counterMap, bucketList)
  }

  private class Bucket[T](var count: Long) {
    val counterList = new DoublyLinkedList[Counter[T]]
  }

  private class Counter[T](var bucketNode: ListNode2[Bucket[T]], var item: T, var count: Long = 0L, var error: Long = 0L) {
    override def toString: String = s"$item:$count:$error"
  }
}
