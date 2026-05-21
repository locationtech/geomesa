/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.specs2.mutable.SpecificationWithJUnit

class XZ2ReversalTest extends SpecificationWithJUnit {

  sequential

  "XZ2Reversal" should {

    "correctly reverse simple cases" in {
      val sfc = XZ2SFC(XZSFC.DefaultPrecision)

      // Test a few specific bounding boxes
      val testCases = Seq(
        (-180.0, -90.0, -179.0, -89.0),  // near corner
        (0.0, 0.0, 1.0, 1.0),             // near center
        (100.0, 50.0, 101.0, 51.0),       // arbitrary location
        (-10.0, -10.0, 10.0, 10.0),       // larger box
        (45.0, 45.0, 45.01, 45.01)        // tiny box
      )

      foreach(testCases) { case (xmin, ymin, xmax, ymax) =>
        val code = sfc.index(xmin, ymin, xmax, ymax)
println()
println(f"input:  $xmin%.2f, $ymin%.2f, $xmax%.2f, $ymax%.2f")
        val (rxmin, rymin, rxmax, rymax) = sfc.invert(code)
println()
        // The reversed box should be a valid box within bounds
        rxmin must beGreaterThanOrEqualTo(sfc.xBounds._1)
        rymin must beGreaterThanOrEqualTo(sfc.yBounds._1)
        rxmax must beLessThanOrEqualTo(sfc.xBounds._2)
        rymax must beLessThanOrEqualTo(sfc.yBounds._2)
        rxmin must beLessThan(rxmax)
        rymin must beLessThan(rymax)
      }
    }

    "produce results for random bounding boxes" in {
      val sfc = XZ2SFC
      val random = new scala.util.Random(42)

      var successCount = 0
      val testCount = 100

      for (_ <- 0 until testCount) {
        // Generate random bounding box
        val xmin = -180.0 + random.nextDouble() * 360.0
        val ymin = -90.0 + random.nextDouble() * 180.0

        // Random width/height, ensuring we stay in bounds
        val maxWidth = math.min(180.0 - xmin, xmin + 180.0)
        val maxHeight = math.min(90.0 - ymin, ymin + 90.0)
        val width = random.nextDouble() * maxWidth
        val height = random.nextDouble() * maxHeight

        val xmax = xmin + width
        val ymax = ymin + height

        // Index it
        val code = sfc.index(xmin, ymin, xmax, ymax)
println()
println(f"input:  $xmin%.2f, $ymin%.2f, $xmax%.2f, $ymax%.2f")
        // Try to reverse it
        val (rxmin, rymin, rxmax, rymax) = sfc.invert(code)
println()
        // Verify the reversed box is valid
        if (rxmin >= sfc.xBounds._1 && rymin >= sfc.yBounds._1 &&
            rxmax <= sfc.xBounds._2 && rymax <= sfc.yBounds._2 &&
            rxmin < rxmax && rymin < rymax) {
          successCount += 1
        }
      }

      // All test cases should succeed
      successCount mustEqual testCount
    }

    "work with different resolution levels" in {
      val xBounds = (-180.0, 180.0)
      val yBounds = (-90.0, 90.0)
      val (xmin, ymin, xmax, ymax) = (10.0, 20.0, 11.0, 21.0)

      foreach(Seq[Short](8, 10, 12, 15)) { g =>
        val sfc = new XZ2SFC(g, xBounds, yBounds)
        val code = sfc.index(xmin, ymin, xmax, ymax)
println()
println(f"input:  $xmin%.2f, $ymin%.2f, $xmax%.2f, $ymax%.2f")
        val (rxmin, rymin, rxmax, rymax) = sfc.invert(code)
println()

        // Should be a valid box within bounds
        rxmin must beGreaterThanOrEqualTo(xBounds._1)
        rymin must beGreaterThanOrEqualTo(yBounds._1)
        rxmax must beLessThanOrEqualTo(xBounds._2)
        rymax must beLessThanOrEqualTo(yBounds._2)
        rxmin must beLessThan(rxmax)
        rymin must beLessThan(rymax)
      }
    }

    "handle edge cases at boundaries" in {
      val sfc = XZ2SFC

      val edgeCases = Seq(
        (-180.0, -90.0, -179.9, -89.9),   // bottom-left corner
        (179.9, 89.9, 180.0, 90.0),       // top-right corner
        (-180.0, 0.0, -179.0, 1.0),       // left edge
        (179.0, 0.0, 180.0, 1.0),         // right edge
        (0.0, -90.0, 1.0, -89.0),         // bottom edge
        (0.0, 89.0, 1.0, 90.0)            // top edge
      )

      foreach(edgeCases) { case (xmin, ymin, xmax, ymax) =>
        val code = sfc.index(xmin, ymin, xmax, ymax)
println()
println(f"input:  $xmin%.2f, $ymin%.2f, $xmax%.2f, $ymax%.2f")
        val (rxmin, rymin, rxmax, rymax) = sfc.invert(code)
println()

        // Should be a valid box within bounds
        rxmin must beGreaterThanOrEqualTo(sfc.xBounds._1)
        rymin must beGreaterThanOrEqualTo(sfc.yBounds._1)
        rxmax must beLessThanOrEqualTo(sfc.xBounds._2)
        rymax must beLessThanOrEqualTo(sfc.yBounds._2)
        rxmin must beLessThan(rxmax)
        rymin must beLessThan(rymax)
      }
    }

    "handle boxes of varying sizes" in {
      val sfc = XZ2SFC

      // Test boxes from very small to very large (keeping within y bounds of -90 to 90)
      val sizes = Seq(0.001, 0.01, 0.1, 1.0, 10.0, 50.0)

      foreach(sizes) { size =>
        val (xmin, ymin, xmax, ymax) = (0.0, 0.0, size, math.min(size, 90.0))
println()
println(f"input:  $xmin%.2f, $ymin%.2f, $xmax%.2f, $ymax%.2f")
        val code = sfc.index(xmin, ymin, xmax, ymax)
        val (rxmin, rymin, rxmax, rymax) = sfc.invert(code)
println()
        // Should be a valid box within bounds
        rxmin must beGreaterThanOrEqualTo(sfc.xBounds._1)
        rymin must beGreaterThanOrEqualTo(sfc.yBounds._1)
        rxmax must beLessThanOrEqualTo(sfc.xBounds._2)
        rymax must beLessThanOrEqualTo(sfc.yBounds._2)
        rxmin must beLessThan(rxmax)
        rymin must beLessThan(rymax)
      }
    }

    "demonstrate usage pattern" in {
      // Example of how to use the reversal function
      val sfc = XZ2SFC
      // Index a bounding box
      val (xmin, ymin, xmax, ymax) = (10.5, 20.3, 11.2, 21.1)
      val code = sfc.index(xmin, ymin, xmax, ymax)
      println()
println(f"input:  $xmin%.2f, $ymin%.2f, $xmax%.2f, $ymax%.2f")
      // Reverse the index to get back a bounding box
      // The reversed box represents the grid cell that was encoded
      val (rxmin, rymin, rxmax, rymax) = sfc.invert(code)

      // Verify it's a valid box
      rxmin must beGreaterThanOrEqualTo(sfc.xBounds._1)
      rymin must beGreaterThanOrEqualTo(sfc.yBounds._1)
      rxmax must beLessThanOrEqualTo(sfc.xBounds._2)
      rymax must beLessThanOrEqualTo(sfc.yBounds._2)
      rxmin must beLessThan(xmax)
      rymin must beLessThan(ymax)
    }
  }
}
