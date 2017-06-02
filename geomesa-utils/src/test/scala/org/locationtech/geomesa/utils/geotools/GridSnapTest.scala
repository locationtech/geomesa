/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.utils.geotools

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GridSnapTest extends Specification with LazyLogging {

  "GridSnap" should {
    "create a gridsnap around a given bbox" in {
      val bbox = new Envelope(0.0, 10.0, 0.0, 10.0)
      val gridSnap = new GridSnap(bbox, 100, 100)

      gridSnap must not(beNull)
    }

    "snap to the middle of a grid cell" in {
      val bbox = new Envelope(0.0, 4.0, -4.0, 0.0)
      val gridSnap = new GridSnap(bbox, 4, 4)

      gridSnap.x(0) mustEqual 0.5
      gridSnap.x(1) mustEqual 1.5
      gridSnap.x(2) mustEqual 2.5
      gridSnap.x(3) mustEqual 3.5

      gridSnap.y(0) mustEqual -3.5
      gridSnap.y(1) mustEqual -2.5
      gridSnap.y(2) mustEqual -1.5
      gridSnap.y(3) mustEqual -0.5

      gridSnap.snap(0, -4.0)   mustEqual (0.5, -3.5)
      gridSnap.snap(0.1, -3.9) mustEqual (0.5, -3.5)
      gridSnap.snap(0.9, -3.1) mustEqual (0.5, -3.5)

      gridSnap.snap(1.0, -3.0) mustEqual (1.5, -2.5)
      gridSnap.snap(1.1, -2.9) mustEqual (1.5, -2.5)
      gridSnap.snap(1.9, -2.1) mustEqual (1.5, -2.5)

      gridSnap.snap(3.0, -1.0) mustEqual (3.5, -0.5)
      gridSnap.snap(3.1, -0.9) mustEqual (3.5, -0.5)
      gridSnap.snap(3.9, -0.1) mustEqual (3.5, -0.5)
      gridSnap.snap(4.0, 0.0)  mustEqual (3.5, -0.5)
    }

    "handle min/max" >> {
      val bbox = new Envelope(0.0, 10.0, 0.0, 10.0)
      val gridSnap = new GridSnap(bbox, 100, 10)

      gridSnap.i(0.0) mustEqual 0
      gridSnap.j(0.0) mustEqual 0

      gridSnap.i(10.0) mustEqual 99
      gridSnap.j(10.0) mustEqual 9
    }

    "handle out of bounds points" >> {
      val bbox = new Envelope(0.0, 10.0, 0.0, 10.0)
      val gridSnap = new GridSnap(bbox, 100, 10)

      gridSnap.i(-1.0) mustEqual 0
      gridSnap.j(-1.0) mustEqual 0

      gridSnap.i(11.0) mustEqual 99
      gridSnap.j(11.0) mustEqual 9
    }

    "compute a SimpleFeatureSource Grid over the bbox" in {
      val bbox = new Envelope(0.0, 10.0, 0.0, 10.0)
      val gridSnap = new GridSnap(bbox, 10, 10)

      val grid = gridSnap.generateCoverageGrid

      grid must not(beNull)

      val featureIterator = SelfClosingIterator(grid.getFeatures.features)
      featureIterator must haveLength(100)
    }

    "compute a sequence of points between various sets of coordinates" in {
      val bbox = new Envelope(0.0, 10.0, 0.0, 10.0)
      val gridSnap = new GridSnap(bbox, 10, 10)

      val resultDiagonal = gridSnap.genBresenhamCoordList(0, 0, 9, 9)
      resultDiagonal must haveLength(9)

      val resultVertical = gridSnap.genBresenhamCoordList(0, 0, 0, 9)
      resultVertical must haveLength(9)

      val resultHorizontal = gridSnap.genBresenhamCoordList(0, 0, 9, 0)
      resultHorizontal must haveLength(9)

      val resultSamePoint = gridSnap.genBresenhamCoordList(0, 0, 0, 0)
      resultSamePoint must haveLength(0)

      val resultInverse = gridSnap.genBresenhamCoordList(9, 9, 0, 0)
      resultInverse must haveLength(9)
    }

    "not have floating point errors" >> {
      val bbox = new Envelope(0.0, 10.0, 0.0, 10.0)
      val cols = 100
      val rows = 100
      val gridSnap = new GridSnap(bbox, cols, rows)

      "columns" >> {
        forall(0 until cols) { i =>
          gridSnap.x(gridSnap.i(gridSnap.x(i))) mustEqual gridSnap.x(i)
        }
      }

      "rows" >> {
        forall(0 until rows) { j =>
          gridSnap.y(gridSnap.j(gridSnap.y(j))) mustEqual gridSnap.y(j)
        }
      }

    }
  }

}
