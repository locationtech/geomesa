package geomesa.core.util

import org.apache.accumulo.core.client.BatchScanner
import java.util.Map.Entry
import org.apache.accumulo.core.data.{Value, Key}


class SelfClosingBatchScanner(bs: BatchScanner) {
  val bsIter = bs.iterator

  val iterator = {
    new Iterator[Entry[Key, Value]]{
      def hasNext: Boolean = {
        val iterHasNext = bsIter.hasNext
        if(!iterHasNext) bs.close()
        iterHasNext
      }

      def next(): Entry[Key, Value] = bsIter.next
    }
  }
}