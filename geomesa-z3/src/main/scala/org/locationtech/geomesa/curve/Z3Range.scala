package org.locationtech.geomesa.curve

/**
 * Represents a cube in index space defined by min and max as two opposing points.
 * All operations refer to index space.
 */
case class Z3Range(min: Z3, max: Z3) {

  require(min.z <= max.z, s"Not: $min < $max")

  def mid: Z3 = Z3((max.z - min.z) / 2)

  def length: Int = (max.z - min.z + 1).toInt

  def contains(bits: Z3): Boolean = bits.z >= min.z && bits.z <= max.z

  def contains(r: Z3Range): Boolean = contains(r.min) && contains(r.max)

  def overlaps(r: Z3Range): Boolean = contains(r.min) || contains(r.max)

  def overlapsInUserSpace(r: Z3Range): Boolean =
    overlaps(min.d0, max.d0, r.min.d0, r.max.d0) &&
        overlaps(min.d1, max.d1, r.min.d1, r.max.d1) &&
        overlaps(min.d2, max.d2, r.min.d2, r.max.d2)

  private def overlaps(a1: Int, a2: Int, b1: Int, b2: Int) = math.max(a1, b1) <= math.min(a2, b2)

  def containsInUserSpace(bits: Z3) = {
    val (x, y, z) = bits.decode
    x >= min.d0 &&
        x <= max.d0 &&
        y >= min.d1 &&
        y <= max.d1 &&
        z >= min.d2 &&
        z <= max.d2
  }

  def containsInUserSpace(r: Z3Range): Boolean = containsInUserSpace(r.min) && containsInUserSpace(r.max)
}
