/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa

import java.math.{RoundingMode, MathContext}

import org.calrissian.mango.types.LexiTypeEncoders

/**
 * In these lexiEncode and -Decode functions, a double is encoded or decoded into a lexical
 * representation to be used in the rowID in the Accumulo key.
 *
 * In the lexiEncodeDoubleToString function, the double is truncated via floor() to four significant digits,
 * giving a scientific notation of #.###E0. This is to ensure that there is flexibility in the
 * precision of the bounding box given when querying for chunks. Prior to rounding the resolution,
 * it was found that even that slightest change in bounding box caused the resolution to be calculated
 * differently many digits after the decimal point, leading to a different lexical representation of the
 * resolution. This meant that no chunks would be returned out of Accumulo, since they did not match
 * the resolution calculated upon ingest.
 *
 * Now, there is greater flexibility in specifying a bounding box and calculating a resolution because
 * we save only four digits after the decimal point.
 */
package object raster {

  // Sets the rounding mode to use floor() in order to minimize effects from round-off at higher precisions
  val roundingMode =  RoundingMode.FLOOR

  // Sets the scale for the floor() function and thus determines where the truncation occurs
  val significantDigits = 4

  // Defines the rules for rounding using the above
  val mc = new MathContext(significantDigits, roundingMode)

  /**
   * The double, number, is truncated to a certain number of significant digits and then lexiEncoded into
   * a string representations.
   * @param number, the Double to be lexiEncoded
   */
  def lexiEncodeDoubleToString(number: Double): String = {
    val truncatedRes = BigDecimal(number).round(mc).toDouble
    LexiTypeEncoders.LEXI_TYPES.encode(truncatedRes)
  }

  /**
   * The string representation of a double, str, is decoded to its original Double representation
   * and then truncated to a certain number of significant digits to remain consistent with the lexiEncode function.
   * @param str, the String representation of the Double
   */
  def lexiDecodeStringToDouble(str: String): Double = {
    val number = LexiTypeEncoders.LEXI_TYPES.decode("double", str).asInstanceOf[Double]
    BigDecimal(number).round(mc).toDouble
  }
}
