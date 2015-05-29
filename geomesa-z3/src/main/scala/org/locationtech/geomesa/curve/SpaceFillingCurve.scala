package org.locationtech.geomesa.curve

import org.joda.time.Weeks

trait SpaceFillingCurve[T] {
  def xprec: Long
  def yprec: Long
  def tprec: Long
  def tmax: Double
  def index(x: Double, y: Double, t: Long): T
  def invert(i: T): (Double, Double, Long)
  def ranges(lx: Double, ly: Double,
             ux: Double, uy: Double,
             lt: Long,   ut: Long,
             maxRecurse: Int): Seq[(Long, Long)]
  def normLon(x: Double) = math.ceil((180.0 + x) / 360.0 * xprec).toInt
  def denormLon(x: Double): Double = (x / xprec) * 360.0 - 180.0
  def normLat(y: Double) = math.ceil((90.0 + y) / 180.0 * yprec).toInt
  def denormLat(y: Double): Double = (y / yprec) * 180.0 - 90.0
  def normT(t: Long) = math.max(0, math.ceil(t / tmax * tprec).toInt)
  def denormT(t: Long) = t * tmax / tprec
}

class Z3SFC extends SpaceFillingCurve[Z3] {

  override val xprec: Long = math.pow(2, 21).toLong - 1
  override val yprec: Long = math.pow(2, 21).toLong - 1
  override val tprec: Long = math.pow(2, 20).toLong - 1
  override val tmax: Double = Weeks.weeks(1).toStandardSeconds.getSeconds.toDouble

  override def index(x: Double, y: Double, t: Long): Z3 = {
    val nx = normLon(x)
    val ny = normLat(y)
    val nt = normT(t)
    Z3(nx, ny, nt)
  }

  override def ranges(lx: Double, ly: Double,
                      ux: Double, uy: Double,
                      lt: Long,   ut: Long,
                      maxRecurse: Int): Seq[(Long, Long)] = {
    val lz = Z3(normLon(lx), normLat(ly), normT(lt))
    val uz = Z3(normLon(ux), normLat(uy), normT(ut))
    Z3.zranges(lz, uz, maxRecurse)
  }

  override def invert(z: Z3): (Double, Double, Long) = {
    val (x,y,t) = z.decode
    (denormLon(x), denormLat(y), denormT(t).toLong)
  }
}
