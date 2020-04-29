/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.util.NoSuchElementException

import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.MatchResult
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class SortingSimpleFeatureIteratorTest extends Specification with Mockito {

  val sft = SimpleFeatureTypes.createType("ns:test", "age:Int,name:String,foo:Int")

  val a =  ScalaSimpleFeature.create(sft, "1", 7, "A")
  val b =  ScalaSimpleFeature.create(sft, "2", 9, "B")
  val c1 = ScalaSimpleFeature.create(sft, "3", 6, "C")
  val c2 = ScalaSimpleFeature.create(sft, "4", 9, "C")
  val d =  ScalaSimpleFeature.create(sft, "5", 6, "D")

  "SortingSimpleFeatureIterator" should {

    "lazily sort" >> {

      "when hasNext is called first" >> {
        val features = mock[CloseableIterator[SimpleFeature]]
        // note: hasNext gets called twice consecutively
        features.hasNext returns true thenReturns true thenReturns true thenReturns false
        features.next returns b thenReturns a thenThrows new NoSuchElementException

        val test = new SortingSimpleFeatureIterator(features, Seq(("", false)))

        there were no(features).hasNext
        there were no(features).next
        there were no(features).close

        test.hasNext must beTrue

        there were exactly(4)(features).hasNext
        there were two(features).next
        there were no(features).close

        test.close()
        there was one(features).close
      }

      "or when next is called first" >> {
        val features = mock[CloseableIterator[SimpleFeature]]
        // note: hasNext gets called twice consecutively
        features.hasNext returns true thenReturns true thenReturns true thenReturns false
        features.next returns b thenReturns a thenThrows new NoSuchElementException

        val test = new SortingSimpleFeatureIterator(features, Seq(("", false)))

        there were no(features).hasNext
        there were no(features).next
        there were no(features).close

        test.next mustEqual a

        there were exactly(4)(features).hasNext
        there were two(features).next
        there were no(features).close

        test.close()
        there was one(features).close
      }
    }

    "be able to sort by id asc" >> {
      val features = new TestCloseableIterator(Iterator(b, c1, d, a, c2))
      test(features, Seq(("", false)), Seq(a, b, c1, c2, d))
    }

    "be able to sort by id desc" >> {
      val features = new TestCloseableIterator(Iterator(b, c1, d, a, c2))
      test(features, Seq(("", true)), Seq(d, c2, c1, b, a))
    }

    "be able to sort by an attribute asc" >> {
      val features = new TestCloseableIterator(Iterator(b, c2, d, a, c1))
      test(features, Seq(("name", false)), Seq(a, b, c2, c1, d)) // note: sort is stable
    }

    "be able to sort by an attribute desc" >> {
      val features = new TestCloseableIterator(Iterator(b, c2, d, a, c1))
      test(features, Seq(("name", true)), Seq(d, c2, c1, b, a)) // note: sort is stable
    }

    "be able to sort by an attribute and id" >> {
      val features = new TestCloseableIterator(Iterator(b, c2, d, a, c1))
      test(features, Seq(("name", false), ("", false)), Seq(a, b, c1, c2, d))
    }

    "be able to sort by an multiple attributes" >> {
      val features = new TestCloseableIterator(Iterator(a, b, c1, d, c2))
      test(features, Seq(("age", true), ("name", false)), Seq(b, c2, a, c1, d))
    }

    "be able to spill to disk" >> {
      val threshold = Seq(a, b, c1, c2, d).map(_.calculateSizeOf()).min * 2
      val sort = Seq(("", false))
      val ordered = Seq(a, b, c1, c2, d)
      val shuffled = new Random(42).shuffle(ordered)
      QueryProperties.SortMemoryThreshold.threadLocalValue.set(s"${threshold}B")
      try {
        WithClose(new SortingSimpleFeatureIterator(CloseableIterator(shuffled.iterator), sort)) { iter =>
          iter.toList mustEqual ordered
        }
      } finally {
        QueryProperties.SortMemoryThreshold.threadLocalValue.remove()
      }
      ok
    }
  }

  def test(
      features: TestCloseableIterator,
      sortBy: Seq[(String, Boolean)],
      expected: Seq[SimpleFeature]): MatchResult[Any] = {

    val iter = new SortingSimpleFeatureIterator(features, sortBy)
    try {
      foreach(expected) { f =>
        iter.hasNext must beTrue
        iter.next mustEqual f
      }
      iter.hasNext must beFalse
    } finally {
      iter.close()
    }
    features.isClosed must beTrue
  }

  class TestCloseableIterator(features: Iterator[SimpleFeature]) extends CloseableIterator[SimpleFeature] {
    private var closed = false
    override def hasNext: Boolean = features.hasNext
    override def next(): SimpleFeature = features.next
    override def close(): Unit = closed = true
    def isClosed = closed
  }
}
