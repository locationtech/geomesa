package org.locationtech.geomesa.accumulo.data.tables

import com.google.common.primitives.{Longs, Bytes}
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.curve.{SpaceFillingCurve, NormalizedLat, NormalizedLon, Z2SFC}
import org.locationtech.sfcurve.IndexRange
import org.locationtech.sfcurve.zorder.Z2

// This is ripped off of Z2SFC
object SSIZ2SFC extends SpaceFillingCurve[Z2] {

  // Point level precision
  private val xprec: Long = math.pow(2, 31).toLong - 1
  private val yprec: Long = math.pow(2, 31).toLong - 1

  override val lon: NormalizedLon = NormalizedLon(xprec)
  override val lat  = NormalizedLat(yprec)

  override def index(x: Double, y: Double): Z2 = Z2(lon.normalize(x), lat.normalize(y))

  def indexGeometryToBytes(geom: Geometry): Array[Byte] = {
    val size: Double = sizeOf(geom)

    val tier = findTier(size)
    val centroid = geom.getCentroid
    val index = tier.sfc.index(centroid.getX, centroid.getY)

    Bytes.concat(tier.id, Longs.toByteArray(index.z))
  }

  def findTier(size: Double): Tier = {
    Tier.tiers.find( f => f.minSize > size).get // TODO: .getOrElse BOTTOM TIER
  }

  def sizeOf(geom: Geometry): Double = {
    val env = geom.getEnvelopeInternal
    math.max(env.getHeight, env.getWidth)
  }

  def tieredranges(x: (Double, Double), y: (Double, Double)): Seq[(Array[Byte], IndexRange)] = {
      Tier.tiers.flatMap( t => t.ranges(x, y, 64).map( rs => (t.id, rs)) )
    }

  override def invert(z: Z2): (Double, Double) = {
    val (x, y) = z.decode
    (lon.denormalize(x), lat.denormalize(y))
  }

  override def ranges(x: (Double, Double), y: (Double, Double), precision: Int): Seq[IndexRange] = ???
}

trait Tier {
  def minSize: Double
  def sfc: SpaceFillingCurve[Z2]
  def id: Array[Byte]
}

object Tier {
  // Handle bottom tier better.
  val levels = Seq(31, 19, 15, 9, 1)

  val tiers = levels.map(new Z2Tier(_))
}

class Z2Tier(precisionBits: Int) extends Tier {
  override def id: Array[Byte] = Array(precisionBits.toByte)

  override def sfc: SpaceFillingCurve[Z2] = new PaddedZ2SFC(precisionBits)

  override def minSize: Double = 180 / math.pow(2, precisionBits)

  def ranges(x: (Double, Double), y: (Double, Double), precision: Int) = sfc.ranges(x, y, precision)
}

class PaddedZ2SFC(precisionBits: Int) extends SpaceFillingCurve[Z2] {

  private val xprec: Long = math.pow(2, precisionBits).toLong - 1
  private val yprec: Long = math.pow(2, precisionBits).toLong - 1

  override val lon  = NormalizedLon(xprec)
  override val lat  = NormalizedLat(yprec)

  val paddingSize: Double = 180 / math.pow(2, precisionBits+1)

  override def index(x: Double, y: Double): Z2 = Z2(lon.normalize(x), lat.normalize(y))

  override def ranges(x: (Double, Double), y: (Double, Double), precision: Int): Seq[IndexRange] =
    Z2.zranges(index(x._1 - paddingSize, y._1 - paddingSize), index(x._2 + paddingSize, y._2 + paddingSize), precision)

  override def invert(z: Z2): (Double, Double) = {
    val (x, y) = z.decode
    (lon.denormalize(x), lat.denormalize(y))
  }
}
