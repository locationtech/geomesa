/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import java.io.Closeable

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CloseableIteratorTest extends Specification {

  "CloseableIterator" should {
    "close" >> {
      var closed = false
      val iter = CloseableIterator(Iterator.empty, { closed = true })
      closed must beFalse
      iter.close()
      closed must beTrue
    }
    "close with map" >> {
      var closed = false
      val iter = CloseableIterator(Iterator(0, 1), { closed = true }).map(i => i + 1)
      closed must beFalse
      iter.toSeq mustEqual Seq(1, 2)
      closed must beFalse
      iter.close()
      closed must beTrue
    }
    "close with filter" >> {
      var closed = false
      val iter = CloseableIterator(Iterator(0, 1), { closed = true }).filter(i => i % 2 == 0)
      closed must beFalse
      iter.toSeq mustEqual Seq(0)
      closed must beFalse
      iter.close()
      closed must beTrue
    }
    "close with collect" >> {
      var closed = false
      val iter = CloseableIterator(Iterator(0, 1), { closed = true }).collect {
        case i if i % 2 == 0 => i + 1
      }
      closed must beFalse
      iter.toSeq mustEqual Seq(1)
      closed must beFalse
      iter.close()
      closed must beTrue
    }
    "close with flatmap" >> {
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
    "close any open iterators with flatmap" >> {
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
    "self close with flatmap" >> {
      val closed0, closed1, closed2 = new CloseCounter()
      val result = SelfClosingIterator(CloseableIterator(Iterator(0, 1), closed0.close())).flatMap { i =>
        if (i == 0) {
          CloseableIterator(Iterator(2, 3), closed1.close())
        } else {
          CloseableIterator(Iterator(4, 5), closed2.close())
        }
      }
      foreach(Seq(closed0, closed1, closed2))(_.count mustEqual 0)
      result.toSeq mustEqual Seq(2, 3, 4, 5)
      foreach(Seq(closed0, closed1, closed2))(_.count mustEqual 1)
    }
    "close with concatenate" >> {
      val closed0, closed1, closed2 = new CloseCounter()
      val result = CloseableIterator(Iterator(0, 1), closed0.close()) ++
          CloseableIterator(Iterator(2, 3), closed1.close()) ++
          CloseableIterator(Iterator(4, 5), closed2.close())
      result.toSeq mustEqual Seq(0, 1, 2, 3, 4, 5)
      result.close()
      foreach(Seq(closed0, closed1, closed2))(_.count mustEqual 1)
    }
    "provide an empty iterator that has no next element" >> {
        CloseableIterator.empty.hasNext must beFalse
    }
    "provide an empty iterator that will throw an exception on next" >> {
      CloseableIterator.empty[String].next() must throwA[NoSuchElementException]
    }
    "provide an empty iterator that can be closed" >> {
      CloseableIterator.empty.close() must not(throwA[NullPointerException])
    }
    "not smash the stack in flatMap" >> {
      val f: Int => CloseableIterator[Int] =
        n => if (n < 50000) { CloseableIterator.empty } else { CloseableIterator(Iterator(n)) }
      val ci = CloseableIterator((1 to 50000).iterator)
      ci.flatMap(f).length should be equalTo 1
    }
    "apply iterator with closeable" >> {
      val closed = new CloseCounter()
      val iter = new Iterator[String] with Closeable {
        override def hasNext: Boolean = false
        override def next(): String = Iterator.empty.next
        override def close(): Unit = closed.close()
      }
      val closeable = SelfClosingIterator(iter)
      closed.count mustEqual 0
      closeable.hasNext must beFalse
      closed.count mustEqual 1
      closeable.close()
      closed.count mustEqual 2
    }
  }

  class CloseCounter(var count: Int = 0) extends Closeable {
    override def close(): Unit = count += 1
  }
}
