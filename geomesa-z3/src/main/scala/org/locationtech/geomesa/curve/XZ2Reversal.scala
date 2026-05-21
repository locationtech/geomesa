/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.curve

import scala.util.{Failure, Success, Try}

/**
 * Utilities for reversing XZ2SFC index calculations
 *
 * This file was mostly generated using claude-sonnet-4-5
 */
object XZ2Reversal {

  /**
    * Reverse an XZ2 index value back to its bounding box.
    *
    * This will try all possible encoding lengths and return the bounding box for the first
    * valid decoding (in testing, there is only ever one valid decoding). The returned box
   * represents the grid cell that was encoded and may be larger than the original box due to quantization.
    *
    * @param sfc the XZ2SFC instance used for encoding
    * @param code the XZ2 index value to reverse
    * @return Some((xmin, ymin, xmax, ymax)) if a valid bounding box is found, None otherwise
    */
  def reverseIndex(sfc: XZ2SFC, code: Long): Option[(Double, Double, Double, Double)] = {
    // try all possible lengths from 1 to g
    var length = 1
    while (length <= sfc.g) {
      reverseIndexWithLength(sfc, code, length).foreach { envelope =>
        // return the first valid result
        return Some(envelope)
      }
      length += 1
    }
    None
  }

  /**
    * Reverse an XZ2 index value assuming a specific encoding length.
    *
    * @param sfc the XZ2SFC instance used for encoding
    * @param code the XZ2 index value to reverse
    * @param length the encoding length to assume
    * @return Some((xmin, ymin, xmax, ymax)) if decoding succeeds, None otherwise
    */
  private def reverseIndexWithLength(sfc: XZ2SFC, code: Long, length: Int): Try[(Double, Double, Double, Double)] = {
    // Decode the sequence code to get normalized (xmin, ymin)
    reverseSequenceCode(code, length, sfc.g).map { case (nxmin, nymin) =>
      // Compute cell size at this length
      val cellSize = math.pow(0.5, length)

      // Compute normalized bounding box - use the full cell size
      val nxmax = math.min(nxmin + cellSize, 1.0)
      val nymax = math.min(nymin + cellSize, 1.0)

      // Denormalize to user space
      val (xLo, xHi) = sfc.xBounds
      val (yLo, yHi) = sfc.yBounds

      val xSize = xHi - xLo
      val ySize = yHi - yLo

      val xmin = nxmin * xSize + xLo
      val ymin = nymin * ySize + yLo
      val xmax = math.min(nxmax * xSize + xLo, xHi)
      val ymax = math.min(nymax * ySize + yLo, yHi)

      (xmin, ymin, xmax, ymax)
    }
  }

  /**
    * Reverse the sequence code calculation to recover the normalized (xmin, ymin) position.
    *
    * @param code the sequence code to reverse
    * @param length the length of the encoding
    * @param g the resolution level of the curve
    * @return (xmin, ymin) in normalized [0,1] space
    */
  private def reverseSequenceCode(code: Long, length: Int, g: Short): Try[(Double, Double)] = {
    var xmin = 0.0
    var ymin = 0.0
    var xmax = 1.0
    var ymax = 1.0

    var cs = code

    for (i <- 0 until length) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      val pow4 = math.pow(4, g - i).toLong

      // Calculate the range of possible values from remaining levels
      var minSum = 0L
      var maxSum = 0L
      for (j <- (i + 1) until length) {
        val pow4j = math.pow(4, g - j).toLong
        minSum += 1L
        maxSum += 1L + 3L * (pow4j - 1L) / 3L  // max contribution from level j (Q3)
      }

      // Determine which quadrant by checking ranges
      // Note: Match the exact formula from the encoding:
      // case (true,  true)  => cs += 1L
      // case (false, true)  => cs += 1L + 1L * (math.pow(4, g - i).toLong - 1L) / 3L
      // case (true,  false) => cs += 1L + 2L * (math.pow(4, g - i).toLong - 1L) / 3L
      // case (false, false) => cs += 1L + 3L * (math.pow(4, g - i).toLong - 1L) / 3L
      val q0Contrib = 1L
      val q1Contrib = 1L + 1L * (pow4 - 1L) / 3L
      val q2Contrib = 1L + 2L * (pow4 - 1L) / 3L
      val q3Contrib = 1L + 3L * (pow4 - 1L) / 3L

      val q0Min = q0Contrib + minSum
      val q0Max = q0Contrib + maxSum
      val q1Min = q1Contrib + minSum
      val q1Max = q1Contrib + maxSum
      val q2Min = q2Contrib + minSum
      val q2Max = q2Contrib + maxSum
      val q3Min = q3Contrib + minSum
      val q3Max = q3Contrib + maxSum

      if (cs >= q0Min && cs <= q0Max) {
        // Q0 (SW): x < xCenter, y < yCenter
        xmax = xCenter
        ymax = yCenter
        cs -= q0Contrib
      } else if (cs >= q1Min && cs <= q1Max) {
        // Q1 (SE): x >= xCenter, y < yCenter
        xmin = xCenter
        ymax = yCenter
        cs -= q1Contrib
      } else if (cs >= q2Min && cs <= q2Max) {
        // Q2 (NW): x < xCenter, y >= yCenter
        xmax = xCenter
        ymin = yCenter
        cs -= q2Contrib
      } else if (cs >= q3Min && cs <= q3Max) {
        // Q3 (NE): x >= xCenter, y >= yCenter
        xmin = xCenter
        ymin = yCenter
        cs -= q3Contrib
      } else {
        // Code doesn't fall into any valid range for this length
        return Failure(
          new IllegalArgumentException(
            s"Invalid code $code for length $length at level $i (cs=$cs, " +
              s"ranges: q0=[$q0Min,$q0Max], q1=[$q1Min,$q1Max], q2=[$q2Min,$q2Max], q3=[$q3Min,$q3Max])")
        )
      }
    }

    // Return the lower-left corner of the cell
    Success((xmin, ymin))
  }
}
