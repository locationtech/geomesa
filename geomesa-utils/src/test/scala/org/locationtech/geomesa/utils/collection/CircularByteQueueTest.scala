/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.collection

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CircularByteQueueTest extends Specification {

  "CircularByteQueue" should {
    "enqueue and dequeue values" >> {
      val q = new CircularByteQueue(4)
      q.size mustEqual 0
      q.capacity mustEqual 4
      (0 until 4).foreach(i => q.enqueue(i.toByte))
      q.size mustEqual 4
      q.capacity mustEqual 0
      q.dequeue(4) mustEqual Array.tabulate[Byte](4)(_.toByte)
      q.size mustEqual 0
      q.capacity mustEqual 4
    }
    "grow when needed" >> {
      val q = new CircularByteQueue(4)
      q.size mustEqual 0
      q.capacity mustEqual 4
      (0 until 16).foreach(i => q.enqueue(i.toByte))
      q.size mustEqual 16 // note: capacity doubles each time it grows
      q.capacity mustEqual 0
      q.dequeue(16) mustEqual Array.tabulate[Byte](16)(_.toByte)
      q.size mustEqual 0
      q.capacity mustEqual 16
    }
    "use circular storage" >> {
      val q = new CircularByteQueue(4)
      q.size mustEqual 0
      q.capacity mustEqual 4
      q.enqueue(Array[Byte](0, 1), 0, 2)
      q.size mustEqual 2
      q.capacity mustEqual 2
      q.dequeue(1) mustEqual Array[Byte](0)
      q.size mustEqual 1
      q.capacity mustEqual 3
      q.enqueue(Array[Byte](2, 3, 4), 0, 3)
      q.size mustEqual 4
      q.capacity mustEqual 0
      q.dequeue(4) mustEqual Array[Byte](1, 2, 3, 4)
      q.size mustEqual 0
      q.capacity mustEqual 4
      q.enqueue(Array[Byte](5, 6, 7), 0, 3)
      q.size mustEqual 3
      q.capacity mustEqual 1
      q.dequeue(2) mustEqual Array[Byte](5, 6)
      q.size mustEqual 1
      q.capacity mustEqual 3
    }
  }
}
