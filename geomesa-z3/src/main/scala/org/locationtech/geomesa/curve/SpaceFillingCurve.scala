package org.locationtech.geomesa.curve

import org.joda.time.Weeks

trait SpaceFillingCurve[T] {
  def xprec: Long
  def yprec: Long
  def tprec: Long
  def tmax: Long
  def index(x: Double, y: Double, t: Long): T
  def invert(i: T): (Double, Double, Long)
  def ranges(lx: Double, ly: Double,
             ux: Double, uy: Double,
             lt: Long,   ut: Long,
             maxRecurse: Int): Seq[(Long, Long)]
  def normLon(x: Double) = math.ceil((180.0+x)/360.0*xprec).toInt
  def denormLon(x: Int): Double = (x-180.0)/xprec*360.0
  def normLat(y: Double) = math.ceil((90.0+y)/180.0*yprec).toInt
  def denormLat(y: Int): Double = (y-90.0)/yprec*180.0
  def normT(t: Long) = math.ceil(t/tmax * tprec).toInt
  def denormT(t: Long) = t/tprec*tmax
}

class Z3SFC extends SpaceFillingCurve[Z3] {

  override val xprec: Long = 21
  override val yprec: Long = 21
  override val tprec: Long = 21
  override val tmax: Long = Weeks.weeks(1).toStandardSeconds.getSeconds

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
    (denormLon(x), denormLat(y), denormT(t))
  }
}
