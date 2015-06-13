/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.curve

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MergeQueueTest extends Specification {

  "MergeQueue" should {
    "reduce overlapping values" >> {
      val queue = new MergeQueue()
      queue += (3, 3)
      queue += (4, 4)
      queue.toSeq mustEqual Seq((3, 4))
      queue += (6, 7)
      queue.toSeq mustEqual Seq((3, 4), (6, 7))
      queue += (4, 9)
      queue.toSeq mustEqual Seq((3, 9))
      queue += (0, 1)
      queue.toSeq mustEqual Seq((0, 1), (3, 9))
      queue += (0, 0)
      queue.toSeq mustEqual Seq((0, 1), (3, 9))
    }
  }
}
