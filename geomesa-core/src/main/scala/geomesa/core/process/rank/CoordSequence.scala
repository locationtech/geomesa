package geomesa.core.process.rank

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 6/18/14
 * Time: 10:38 AM
 */
class CoordWithDateTime(val c: Coordinate, val dt: DateTime) {
  def consistentWithMotion(previous: CoordWithDateTime, beforeThat: CoordWithDateTime): Boolean = {
    if (consistentWithMotion(previous)) {
      val pair1 = new CoordWithDateTimePair(previous, this)
      val pair2 = new CoordWithDateTimePair(beforeThat, previous)
      val headingDiff1 = pair1.heading - pair2.heading
      val headingDiff = if (headingDiff1 > 180.0) headingDiff1 - 180.0 else headingDiff1
      val turnRate = headingDiff / (pair1.timeDiff + pair2.timeDiff)
      turnRate < 10.0
    }
    else false
  }

  def consistentWithMotion(previous: CoordWithDateTime): Boolean = {
    val coordPair = new CoordWithDateTimePair(previous, this)
    if (coordPair.timeDiff < RankingDefaults.maxTimeBetweenPings) {
      val speed = coordPair.speed
      (speed > 0.0) && (speed < 1000.0)
    }
    else false
  }

  def consistentWithMotion(otherPoints: List[CoordWithDateTime]): Boolean = {
    otherPoints match {
      case previous :: beforeThat :: rest => consistentWithMotion(previous, beforeThat)
      case previous :: rest => consistentWithMotion(previous)
      case Nil => true
    }
  }
}

case class SpeedStatistics(max: Double, min: Double, avg: Double, stddev: Double)

case class CoordWithDateTimePair(first: CoordWithDateTime, last: CoordWithDateTime) {
  def distance = JTS.orthodromicDistance(first.c, last.c, DefaultGeographicCRS.WGS84)

  def timeDiff = Math.abs((first.dt.getMillis - last.dt.getMillis).toDouble) / 1000.0

  def speed = distance / timeDiff

  def heading = {
    val lonDiff = last.c.x - first.c.x
    val cosLastY = Math.cos(last.c.y)
    val y = Math.sin(lonDiff) * cosLastY
    val x = Math.cos(first.c.y) * Math.sin(last.c.y) - Math.sin(first.c.y) * cosLastY * Math.cos(lonDiff)
    Math.atan2(y, x).toDegrees
  }
}

class CoordSequence(val coords: List[CoordWithDateTimePair]) {
  def distance: Double = coords.foldLeft(0.0) { (dist, pair) => dist + pair.distance }

  def speedStats = {
    val spds = speeds
    if (spds.length == 0) SpeedStatistics(0.0, 0.0, 0.0, 0.0)
    else {
      val avg = spds.sum / spds.length.toDouble
      SpeedStatistics(spds.max, spds.min, avg, MathUtil.stdDev(spds, avg))
    }
  }

  def speeds: List[Double] = coords.map(_.speed)
}

object CoordSequence {
  def fromCoordWithDateTimeList(motionCoords: List[CoordWithDateTime]): CoordSequence = {
    if (motionCoords.size > 1) {
      val coords = motionCoords.sortBy(_.dt.getMillis)
      val first = coords.slice(0, coords.length - 1)
      new CoordSequence(first.zip(coords.tail).map(pr => CoordWithDateTimePair(pr._1, pr._2)))
    }
    else new CoordSequence(List[CoordWithDateTimePair]())
  }
}
