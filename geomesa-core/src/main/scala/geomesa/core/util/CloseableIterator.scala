package geomesa.core.util

import java.util.Map.Entry
import org.apache.accumulo.core.client.BatchScanner
import org.apache.accumulo.core.data.{Key, Value}
import scala.collection.Iterator

object CloseableIterator {
  implicit def iteratorToCloseable[A](iter: Iterator[A]) = new RichCloseableIterator(iter)

  val empty: CloseableIterator[Nothing] = new CloseableIterator[Nothing] {
    def hasNext: Boolean = false
    def next(): Nothing = throw new NoSuchElementException("next on empty iterator")
    def close(): Unit = {}
  }
}

import geomesa.core.util.CloseableIterator.empty

trait CloseableIterator[+A] extends Iterator[A] {
  self =>

  def close(): Unit

  override def map[B](f: A => B): CloseableIterator[B] = new CloseableIterator[B] {
    def hasNext = self.hasNext
    def next() = f(self.next())
    def close() = self.close()
  }

  def flatMap[B](f: A => CloseableIterator[B]): CloseableIterator[B] = new CloseableIterator[B] {
    private var cur: CloseableIterator[B] = empty

    def hasNext: Boolean = {
      val iterHasNext = innerHasNext
      if(!innerHasNext) close()
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

class SelfClosingBatchScanner(bs: BatchScanner) extends CloseableIterator[Entry[Key, Value]]  {
  val bsIter = bs.iterator

  def hasNext: Boolean = {
    val iterHasNext = bsIter.hasNext
    if(!iterHasNext) bs.close()
    iterHasNext
  }

  def next(): Entry[Key, Value] = bsIter.next

  def close() = bs.close()
}

class RichCloseableIterator[A](iter: Iterator[A]) extends CloseableIterator[A] {
  override def hasNext: Boolean = iter.hasNext
  override def next(): A = iter.next()
  def close(): Unit = {}
}

