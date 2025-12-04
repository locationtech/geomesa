/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.Closeable

@RunWith(classOf[JUnitRunner])
class CloseableIteratorTest extends Specification {

  "CloseableIterator" should {
    "close" in {
      val closed = new CloseCounter()
      val iter = CloseableIterator(Iterator.empty, { closed.close() })
      closed.count mustEqual 0
      iter.close()
      closed.count mustEqual 1
    }
    "close with map" in {
      val closed = new CloseCounter()
      val iter = CloseableIterator(Iterator(0, 1), { closed.close() }).map(i => i + 1)
      closed.count mustEqual 0
      iter.toSeq mustEqual Seq(1, 2)
      closed.count mustEqual 0
      iter.close()
      closed.count mustEqual 1
    }
    "close with filter" in {
      val closed = new CloseCounter()
      val iter = CloseableIterator(Iterator(0, 1), { closed.close() }).filter(i => i % 2 == 0)
      closed.count mustEqual 0
      iter.toSeq mustEqual Seq(0) // note: toSeq does not close the iterator...
      closed.count mustEqual 0
      iter.close()
      closed.count mustEqual 1
    }
    "close with collect" in {
      val closed = new CloseCounter()
      val iter = CloseableIterator(Iterator(0, 1), { closed.close() }).collect {
        case i if i % 2 == 0 => i + 1
      }
      closed.count mustEqual 0
      iter.toSeq mustEqual Seq(1) // note: toSeq does not close the iterator...
      closed.count mustEqual 0
      iter.close()
      closed.count mustEqual 1
    }
    "close with flatmap" in {
      val closed0, closed1, closed2 = new CloseCounter()
      val result = CloseableIterator(Iterator(0, 1), closed0.close()).flatMap { i =>
        if (i == 0) {
          CloseableIterator(Iterator(2, 3), closed1.close())
        } else {
          CloseableIterator(Iterator(4, 5), closed2.close())
        }
      }
      foreach(Seq(closed0, closed1, closed2))(_.count mustEqual 0)
      result.toSeq mustEqual Seq(2, 3, 4, 5)
      foreach(Seq(closed1, closed2))(_.count mustEqual 1)
      closed0.count mustEqual 0
      result.close()
      foreach(Seq(closed0, closed1, closed2))(_.count mustEqual 1)
    }
    "close any open iterators with flatmap" in {
      val closed0, closed1, closed2 = new CloseCounter()
      val result = CloseableIterator(Iterator(0, 1), closed0.close()).flatMap { i =>
        if (i == 0) {
          CloseableIterator(Iterator(2, 3), closed1.close())
        } else {
          CloseableIterator(Iterator(4, 5), closed2.close())
        }
      }
      foreach(Seq(closed0, closed1, closed2))(_.count mustEqual 0)
      result.hasNext must beTrue
      result.next mustEqual 2
      foreach(Seq(closed0, closed1, closed2))(_.count mustEqual 0)
      result.close()
      foreach(Seq(closed0, closed1))(_.count mustEqual 1)
      closed2.count mustEqual 0
    }
    "self close with flatmap" in {
      val closed0, closed1, closed2 = new CloseCounter()
      val result = CloseableIterator(Iterator(0, 1), closed0.close()).flatMap { i =>
        if (i == 0) {
          CloseableIterator(Iterator(2, 3), closed1.close())
        } else {
          CloseableIterator(Iterator(4, 5), closed2.close())
        }
      }
      foreach(Seq(closed0, closed1, closed2))(_.count mustEqual 0)
      result.toList mustEqual Seq(2, 3, 4, 5)
      foreach(Seq(closed0, closed1, closed2))(_.count mustEqual 1)
    }
    "self close with toList" in {
      val closed = new CloseCounter()
      val iter = CloseableIterator(Iterator(1, 2), { closed.close() })
      closed.count mustEqual 0
      // note: toList isn't explicitly overridden, but it ends up calling foreach, which will close the iterator
      iter.toList mustEqual List(1, 2)
      closed.count mustEqual 1
    }
    "self close with foreach" in {
      val closed = new CloseCounter()
      val iter = CloseableIterator(Iterator(1, 2), { closed.close() })
      closed.count mustEqual 0
      iter.foreach(_ => ())
      closed.count mustEqual 1
    }
    "close with concatenate" in {
      val closed0, closed1, closed2 = new CloseCounter()
      val result = CloseableIterator(Iterator(0, 1), closed0.close()) concat
          CloseableIterator(Iterator(2, 3), closed1.close()) concat
          CloseableIterator(Iterator(4, 5), closed2.close())
      result must beAnInstanceOf[CloseableIterator[Int]]
      result.close()
      foreach(Seq(closed0, closed1, closed2))(_.count mustEqual 1)
    }
    "self close with concatenate" in {
      val closed0, closed1, closed2 = new CloseCounter()
      val result = CloseableIterator(Iterator(0, 1), closed0.close()) concat
        CloseableIterator(Iterator(2, 3), closed1.close()) concat
        CloseableIterator(Iterator(4, 5), closed2.close())
      result must beAnInstanceOf[CloseableIterator[Int]]
      result.toList mustEqual Seq(0, 1, 2, 3, 4, 5)
      foreach(Seq(closed0, closed1, closed2))(_.count mustEqual 1)
    }
    "provide an empty iterator that has no next element" in {
        CloseableIterator.empty.hasNext must beFalse
    }
    "provide an empty iterator that will throw an exception on next" in {
      CloseableIterator.empty[String].next() must throwA[NoSuchElementException]
    }
    "provide an empty iterator that can be closed" in {
      CloseableIterator.empty.close() must not(throwA[NullPointerException])
    }
    "not smash the stack in flatMap" in {
      val f: Int => CloseableIterator[Int] =
        n => if (n < 50000) { CloseableIterator.empty } else { CloseableIterator(Iterator(n)) }
      val ci = CloseableIterator((1 to 50000).iterator)
      ci.flatMap(f).length should be equalTo 1
    }
    "apply iterator with closeable" in {
      val closed = new CloseCounter()
      val iter = new Iterator[String] with Closeable {
        override def hasNext: Boolean = false
        override def next(): String = Iterator.empty.next
        override def close(): Unit = closed.close()
      }
      val closeable = CloseableIterator(iter)
      closed.count mustEqual 0
      closeable.hasNext must beFalse
      closed.count mustEqual 0
      closeable.close()
      closed.count mustEqual 1
    }
  }

  class CloseCounter(var count: Int = 0) extends Closeable {
    override def close(): Unit = count += 1
  }
}
