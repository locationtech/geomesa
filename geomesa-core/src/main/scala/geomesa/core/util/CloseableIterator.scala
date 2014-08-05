package geomesa.core.util

import java.util.Map.Entry
import org.apache.accumulo.core.client.{Scanner, BatchScanner}
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.data.FeatureReader
import org.opengis.feature.Feature
import org.opengis.feature.`type`.FeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.collection.Iterator
import scala.collection.JavaConversions._

// A CloseableIterator is one which involves some kind of close function which should be called at the end of use.
object CloseableIterator {
  // In order to use 'map' and 'flatMap', we provide an implicit promoting wrapper.
  implicit def iteratorToCloseable[A](iter: Iterator[A]) = apply(iter)

  val empty: CloseableIterator[Nothing] = apply(Iterator.empty)

  val noop: () => Unit = () => {}

  // This apply method provides us with a simple interface for creating new CloseableIterators.
  def apply[A](iter: Iterator[A], closeIter: () => Unit = noop) = new CloseableIterator[A] {
    def hasNext = iter.hasNext
    def next()  = iter.next()
    def close() = closeIter()
  }

  // This apply method provides us with a simple interface for creating new CloseableIterators.
  def apply[A <: Feature, B <: FeatureType](iter: FeatureReader[B, A]) = new CloseableIterator[A] {
    def hasNext = iter.hasNext
    def next()  = iter.next()
    def close() = iter.close()
  }
}

import geomesa.core.util.CloseableIterator.empty

trait CloseableIterator[+A] extends Iterator[A] {
  self =>

  def close(): Unit

  override def map[B](f: A => B): CloseableIterator[B] = CloseableIterator(super.map(f), self.close)

  // NB: Since we wish to be able to close the iterator currently in use, we can't call out to super.flatMap.
  def ciFlatMap[B](f: A => CloseableIterator[B]): CloseableIterator[B] = new SelfClosingIterator[B] {
    private var cur: CloseableIterator[B] = if(self.hasNext) f(self.next()) else empty

    // Add in the 'SelfClosing' behavior.
    def hasNext: Boolean = {
      val iterHasNext = innerHasNext
      if(!iterHasNext) close()
      iterHasNext
    }

    private def innerHasNext: Boolean =
      cur.hasNext || self.hasNext && { advanceClosing(); hasNext }

    private def advanceClosing() = {
      if (cur != empty) cur.close()
      cur = f(self.next())
    }

    def next(): B = (if (hasNext) cur else empty).next()

    def close() = { cur.close(); self.close() }
  }
}

// By 'self-closing', we mean that the iterator will automatically call close once it is completely exhausted.
trait SelfClosingIterator[+A] extends CloseableIterator[A]

object SelfClosingIterator {
  def apply[A](iter: Iterator[A], closeIter: () => Unit) = new SelfClosingIterator[A] {
    def hasNext: Boolean = {
      val iterHasNext = iter.hasNext
      if(!iterHasNext) close()
      iterHasNext
    }
    def next(): A = iter.next()
    def close() = closeIter()
  }

  def apply[A](iter: CloseableIterator[A]): SelfClosingIterator[A] = apply(iter, iter.close)

  def apply(s: Scanner): SelfClosingIterator[Entry[Key, Value]] = apply(s.iterator(), s.close)
}

// This object provides a standard way to wrap BatchScanners in a self-closing and closeable iterator.
object SelfClosingBatchScanner {
  def apply(bs: BatchScanner): SelfClosingIterator[Entry[Key, Value]] = SelfClosingIterator(bs.iterator, () => bs.close())
}