package org.locationtech.geomesa.curve

/**
 * Represents a cube in defined by min and max as two opposing points
 */
case class Z3Range(min: Z3, max: Z3){
  require(min.z < max.z || min.z == max.z, s"NOT: ${min.z} < ${max.z}")

  def mid = min mid max

  def length = max.z - min.z

  def contains(bits: Z3) = {
    val (x, y, z) = bits.decode
    x >= min.dim(0) &&
      x <= max.dim(0) &&
      y >= min.dim(1) &&
      y <= max.dim(1) &&
      z >= min.dim(2) &&
      z <= max.dim(2)

  }
  def contains(r: Z3Range): Boolean =
    contains(r.min) && contains(r.max)

  def overlaps(r: Z3Range): Boolean = {
    def _overlaps(a1:Int, a2:Int, b1:Int, b2:Int) = math.max(a1,b1) <= math.min(a2,b2)
    _overlaps(min.dim(0), max.dim(0), r.min.dim(0), r.max.dim(0)) &&
      _overlaps(min.dim(1), max.dim(1), r.min.dim(1), r.max.dim(1)) &&
      _overlaps(min.dim(2), max.dim(2), r.min.dim(2), r.max.dim(2))
  }

  def zdivide(xd: Z3): (Z3, Z3) =
    Z3.zdivide(xd, min, max)

  /**
   * Cuts Z-Range in two, can be used to perform augmented binary search
   * @param xd: division point
   * @param inRange: is xd in query range
   */
  def cut(xd: Z3, inRange: Boolean): List[Z3Range] = {
    if (min.z == max.z)
      Nil
    else if (inRange) {
      if (xd.z == min.z)      // degenerate case, two nodes min has already been counted
        Z3Range(max, max) :: Nil
      else if (xd.z == max.z) // degenerate case, two nodes max has already been counted
        Z3Range(min, min) :: Nil
      else
        Z3Range(min, xd-1) :: Z3Range(xd+1, max) :: Nil
    } else {
      val (litmax, bigmin) = Z3.zdivide(xd, min, max)
      Z3Range(min, litmax) :: Z3Range(bigmin, max) :: Nil
    }
  }
}
