package org.locationtech.geomesa.curve

class Z3(val z: Long) extends AnyVal {
  import Z3._

  def < (other: Z3) = z < other.z
  def > (other: Z3) = z > other.z
  def >= (other: Z3) = z >= other.z
  def <= (other: Z3) = z <= other.z
  def + (offset: Long) = new Z3(z + offset)
  def - (offset: Long) = new Z3(z - offset)
  def == (other: Z3) = other.z == z

  def decode: (Int, Int, Int) = {
    ( combine(z), combine(z >> 1), combine(z >> 2) )
  }

  def dim(i: Int) = Z3.combine(z >> i)

  def inRange(rmin: Z3, rmax: Z3): Boolean = {
    val (x, y, z) = decode
    x >= rmin.dim(0) &&
      x <= rmax.dim(0) &&
      y >= rmin.dim(1) &&
      y <= rmax.dim(1) &&
      z >= rmin.dim(2) &&
      z <= rmax.dim(2)
  }

  def mid(p: Z3): Z3 = {
    if (p.z < z)
      new Z3(p.z + (z - p.z)/2)
    else
      new Z3(z + (p.z - z)/2)
  }

  def bitsToString = f"(${z.toBinaryString}%16s)(${dim(0).toBinaryString}%8s,${dim(1).toBinaryString}%8s,${dim(2).toBinaryString}%8s)"
  override def toString = f"$z ${decode}"
}

object Z3 {
  final val MAX_BITS = 21
  final val MAX_MASK = 0x1fffffL;
  final val MAX_DIM = 3

  def apply(zvalue: Long) = new Z3(zvalue)

  /** insert 00 between every bit in value. Only first 21 bits can be considred. */
  def split(value: Long): Long = {
    var x = value & MAX_MASK;
    x = (x | x << 32) & 0x1f00000000ffffL
    x = (x | x << 16) & 0x1f0000ff0000ffL
    x = (x | x << 8)  & 0x100f00f00f00f00fL
    x = (x | x << 4)  & 0x10c30c30c30c30c3L
    (x | x << 2)      & 0x1249249249249249L
  }

  /** combine every third bit to form a value. Maximum value is 21 bits. */
  def combine(z: Long): Int = {
    var x = z & 0x1249249249249249L
    x = (x ^ (x >>  2)) & 0x10c30c30c30c30c3L
    x = (x ^ (x >>  4)) & 0x100f00f00f00f00fL
    x = (x ^ (x >>  8)) & 0x1f0000ff0000ffL
    x = (x ^ (x >> 16)) & 0x1f00000000ffffL
    x = (x ^ (x >> 32)) & MAX_MASK
    x.toInt
  }

  /**
   * So this represents the order of the tuple, but the bits will be encoded in reverse order:
   *   ....z1y1x1z0y0x0
   * This is a little confusing.
   */
  def apply(x: Int, y:  Int, z: Int): Z3 = {
    new Z3(split(x) | split(y) << 1 | split(z) << 2)
  }

  def unapply(z: Z3): Option[(Int, Int, Int)] =
    Some(z.decode)

  def zdivide(p: Z3, rmin: Z3, rmax: Z3): (Z3, Z3) = {
    val (litmax,bigmin) = zdiv(load, MAX_DIM)(p.z, rmin.z, rmax.z)
    (new Z3(litmax), new Z3(bigmin))
  }

  /** Loads either 1000... or 0111... into starting at given bit index of a given dimention */
  def load(target: Long, p: Long, bits: Int, dim: Int): Long = {
    val mask = ~(Z3.split(MAX_MASK >> (MAX_BITS-bits)) << dim)
    val wiped = target & mask
    wiped | (split(p) << dim)
  }

  /** Recurse down the oct-tree and report all z-ranges which are contained in the cube defined by the min and max points */
  def zranges(min: Z3, max: Z3, maxRecurse: Int): Seq[(Long, Long)] = {
    var mq: MergeQueue = new MergeQueue
    val sr = Z3Range(min, max)

    var recCounter = 0
    var reportCounter = 0

    def _zranges(prefix: Long, offset: Int, quad: Long, level: Int): Unit = {
      recCounter += 1

      val min: Long = prefix | (quad << offset) // QR + 000..
      val max: Long = min | (1L << offset) - 1 // QR + 111..
      val qr = Z3Range(new Z3(min), new Z3(max))
      if(level < maxRecurse) {
        if (sr contains qr) {
          // whole range matches, happy day
          mq +=(qr.min.z, qr.max.z)
          reportCounter += 1
        } else if (offset > 0 && (sr overlaps qr)) {
          // some portion of this range are excluded
          _zranges(min, offset - MAX_DIM, 0, level + 1)
          _zranges(min, offset - MAX_DIM, 1, level + 1)
          _zranges(min, offset - MAX_DIM, 2, level + 1)
          _zranges(min, offset - MAX_DIM, 3, level + 1)
          _zranges(min, offset - MAX_DIM, 4, level + 1)
          _zranges(min, offset - MAX_DIM, 5, level + 1)
          _zranges(min, offset - MAX_DIM, 6, level + 1)
          _zranges(min, offset - MAX_DIM, 7, level + 1)
          //let our children punt on each subrange
        }
      } else if(sr overlaps qr) {
        mq += (qr.min.z, qr.max.z)
      }
    }

    var prefix: Long = 0
    var offset = MAX_BITS*MAX_DIM
    _zranges(prefix, offset, 0, 0) // the entire space
    mq.toSeq
  }

  /**
   * Implements the the algorithm defined in: Tropf paper to find:
   * LITMAX: maximum z-index in query range smaller than current point, xd
   * BIGMIN: minimum z-index in query range greater than current point, xd
   *
   * @param load: function that knows how to load bits into appropraite dimention of a z-index
   * @param xd: z-index that is outside of the query range
   * @param rmin: minimum z-index of the query range, inclusive
   * @param rmax: maximum z-index of the query range, inclusive
   * @return (LITMAX, BIGMIN)
   */
  def zdiv(load: (Long, Long, Int, Int) => Long, dims: Int)(xd: Long, rmin: Long, rmax: Long): (Long, Long) = {
    require(rmin < rmax, "min ($rmin) must be less than max $(rmax)")
    var zmin: Long = rmin
    var zmax: Long = rmax
    var bigmin: Long = 0L
    var litmax: Long = 0L

    def bit(x: Long, idx: Int) = {
      ((x & (1L << idx)) >> idx).toInt
    }
    def over(bits: Long)  = (1L << (bits-1))
    def under(bits: Long) = (1L << (bits-1)) - 1

    var i = 64
    while (i > 0) {
      i -= 1

      val bits = i/dims+1
      val dim  = i%dims

      ( bit(xd, i), bit(zmin, i), bit(zmax, i) ) match {
        case (0, 0, 0) =>
        // continue

        case (0, 0, 1) =>
          zmax   = load(zmax, under(bits), bits, dim)
          bigmin = load(zmin, over(bits), bits, dim)

        case (0, 1, 0) =>
        // sys.error(s"Not possible, MIN <= MAX, (0, 1, 0)  at index $i")

        case (0, 1, 1) =>
          bigmin = zmin
          return (litmax, bigmin)

        case (1, 0, 0) =>
          litmax = zmax
          return (litmax, bigmin)

        case (1, 0, 1) =>
          litmax = load(zmax, under(bits), bits, dim)
          zmin = load(zmin, over(bits), bits, dim)

        case (1, 1, 0) =>
        // sys.error(s"Not possible, MIN <= MAX, (1, 1, 0) at index $i")

        case (1, 1, 1) =>
        // continue
      }
    }
    (litmax, bigmin)
  }

}
