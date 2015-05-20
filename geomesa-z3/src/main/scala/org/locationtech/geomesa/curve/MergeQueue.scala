package org.locationtech.geomesa.curve

class MergeQueue(initialSize: Int = 1) {
  private var array = if(initialSize <= 1) { Array.ofDim[(Long, Long)](1) } else { Array.ofDim[(Long, Long)](initialSize) }
  private var _size = 0

  def size = _size

  private def removeElement(i: Int): Unit = {
    if(i < _size - 1) {
      val result = array.clone
      System.arraycopy(array, i + 1, result, i, _size - i - 1)
      array = result
    }
    _size = _size - 1
  }

  private def insertElement(range: (Long, Long), i: Int): Unit = {
    ensureSize(_size + 1)
    if(i == _size) {
      array(i) = range
    } else {
      val result = array.clone
      System.arraycopy(array, 0, result, 0, i)
      System.arraycopy(array, i, result, i + 1, _size - i)
      result(i) = range
      array = result
    }
    _size += 1
  }


  /** Ensure that the internal array has at least `n` cells. */
  protected def ensureSize(n: Int) {
    // Use a Long to prevent overflows
    val arrayLength: Long = array.length
    if (n > arrayLength - 1) {
      var newSize: Long = arrayLength * 2
      while (n > newSize) {
        newSize = newSize * 2
      }
      // Clamp newSize to Int.MaxValue
      if (newSize > Int.MaxValue) newSize = Int.MaxValue

      val newArray: Array[(Long, Long)] = new Array(newSize.toInt)
      scala.compat.Platform.arraycopy(array, 0, newArray, 0, _size)
      array = newArray
    }
  }

  val ordering = implicitly[Ordering[(Long, Long)]]

  /** Inserts a single range into the priority queue.
    *
    *  @param  range        the element to insert.
    */
  @annotation.tailrec
  final def +=(range: (Long, Long)): Unit = {
    val res = if(_size == 0) { -1 } else { java.util.Arrays.binarySearch(array, 0, _size, range, ordering) }
    if(res < 0) {
      val i = -(res + 1)
      var (thisStart, thisEnd) = range
      var removeLeft = false

      var removeRight = false
      var rightRemainder: Option[(Long, Long)] = None

      // Look at the left range
      if(i != 0) {
        val (prevStart, prevEnd) = array(i - 1)
        if(prevStart == thisStart) {
          removeLeft = true
        }
        if (prevEnd + 1 >= thisStart) {
          removeLeft = true
          thisStart = prevStart
          if(prevEnd > thisEnd) {
            thisEnd = prevEnd
          }
        }
      }

      // Look at the right range
      if(i < _size  && _size > 0) {
        val (nextStart, nextEnd) = array(i)
        if(thisStart == nextStart) {
          removeRight = true
          thisEnd = nextEnd
        } else {
          if(thisEnd + 1 >= nextStart) {
            removeRight = true
            if(nextEnd - 1 >= thisEnd) {
              thisEnd = nextEnd
            } else if (nextEnd < thisEnd - 1) {
              rightRemainder = Some((nextEnd + 1, thisEnd))
              thisEnd = nextEnd
            }
          }
        }
      }

      if(removeRight) {
        if(!removeLeft) {
          array(i) = (thisStart, thisEnd)
        } else {
          array(i-1) = (thisStart, thisEnd)
          removeElement(i)
        }
      } else if(removeLeft) {
        array(i-1) = (thisStart, thisEnd)
      } else {
        insertElement(range, i)
      }

      rightRemainder match {
        case None    =>
        case Some(r) => this += r
      }
    }
  }

  def toSeq: Seq[(Long, Long)] = {
    val result = Array.ofDim[(Long, Long)](size)
    System.arraycopy(array, 0, result, 0, size)
    result
  }
}
