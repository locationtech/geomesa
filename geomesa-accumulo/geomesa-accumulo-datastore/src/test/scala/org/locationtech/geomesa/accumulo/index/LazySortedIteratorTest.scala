/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.accumulo.index

import java.util.NoSuchElementException

import org.geotools.factory.CommonFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.util.CloseableIterator
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.sort.{SortBy, SortOrder}
import org.specs2.matcher.MatchResult
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LazySortedIteratorTest extends Specification with Mockito {

  val ff = CommonFactoryFinder.getFilterFactory
  
  "LazySortedIterator" should {

    val a =  mockSF(1, "A", 7)
    val b =  mockSF(2, "B", 9)
    val c1 = mockSF(3, "C", 6)
    val c2 = mockSF(4, "C", 9)
    val d =  mockSF(5, "D", 6)

    "lazily sort" >> {

      "when hasNext is called first" >> {
        val features = mock[CloseableIterator[SimpleFeature]]
        features.hasNext returns true thenReturns true thenReturns false
        features.next returns b thenReturns a thenThrows new NoSuchElementException

        val test = new LazySortedIterator(features, Array(SortBy.NATURAL_ORDER))

        there was no(features).hasNext
        there was no(features).next
        there was no(features).close

        test.hasNext must beTrue

        there were three(features).hasNext
        there were two(features).next()
        there were one(features).close()
      }

      "or when next is called first" >> {
        val features = mock[CloseableIterator[SimpleFeature]]
        features.hasNext returns true thenReturns true thenReturns false
        features.next returns b thenReturns a thenThrows new NoSuchElementException

        val test = new LazySortedIterator(features, Array(SortBy.NATURAL_ORDER))

        there was no(features).hasNext
        there was no(features).next
        there was no(features).close

        test.next mustEqual a

        there were three(features).hasNext
        there were two(features).next()
        there were one(features).close()
      }
    }

    "be able to sort by id asc" >> {
      val features = CloseableIterator(Iterator(b, c1, d, a, c2))
      val sortBy = Array(SortBy.NATURAL_ORDER)

      test(features, sortBy, Seq(a, b, c1, c2, d))
    }

    "be able to sort by id desc" >> {
      val features = CloseableIterator(Iterator(b, c1, d, a, c2))
      val sortBy =Array(SortBy.REVERSE_ORDER) 
      
      test(features, sortBy, Seq(d, c2, c1, b, a))
    }

    "be able to sort by an attribute asc" >> {
      val features = CloseableIterator(Iterator(b, c2, d, a, c1))
      val sortBy = Array(ff.sort("name", SortOrder.ASCENDING))

      // sort is stable
      test(features, sortBy, Seq(a, b, c2, c1, d))
    }

    "be able to sort by an attribute desc" >> {
      val features = CloseableIterator(Iterator(b, c2, d, a, c1))
      val sortBy = Array(ff.sort("name", SortOrder.DESCENDING))

      // sort is stable
      test(features, sortBy, Seq(d, c2, c1, b, a))
    }

    "be able to sort by an attribute and id" >> {
      val features = CloseableIterator(Iterator(b, c2, d, a, c1))
      val sortBy = Array(ff.sort("name", SortOrder.ASCENDING), SortBy.NATURAL_ORDER)

      test(features, sortBy, Seq(a, b, c1, c2, d))
    }

    "be able to sort by an multiple attributes" >> {
      val features = CloseableIterator(Iterator(a, b, c1, d, c2))
      val sortBy = Array(ff.sort("age", SortOrder.DESCENDING), ff.sort("name", SortOrder.ASCENDING))

      test(features, sortBy, Seq(b, c2, a, c1, d))
    }
  }

  def mockSF(id: Int, name: String, age: Int): SimpleFeature = {
    val sf = mock[SimpleFeature]
    sf.getID returns id.toString
    sf.getAttribute("name") returns name
    sf.getAttribute("age") returns age.asInstanceOf[AnyRef]
    sf
  }

  def test(features: CloseableIterator[SimpleFeature],
             sortBy: Array[SortBy],
             expected: Seq[SimpleFeature]): MatchResult[Any] = {

    val test = new LazySortedIterator(features, sortBy)

    expected.foreach {f =>
      test.hasNext must beTrue
      test.next mustEqual f
    }
    test.hasNext must beFalse
  }
}
