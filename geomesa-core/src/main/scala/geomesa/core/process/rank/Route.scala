package geomesa.core.process.rank

import com.vividsolutions.jts.geom.{Coordinate, PrecisionModel, GeometryFactory, LineString}
import com.vividsolutions.jts.linearref.LocationIndexedLine
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.renderer.label.LineStringCursor

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 6/18/14
 * Time: 10:37 AM
 */

case class MotionScore(numberOfPings: Int,
                       cumulativePathDistance: Double,
                       queryRouteDistance: Double,
                       cumlativeDistanceFromRoute: Double,
                       speedStats: SpeedStatistics,
                       stddevOfHeading: Double,
                       queryRouteStddevOfHeading: Double) {
  def normalizedDistanceFromRoute = if (cumulativePathDistance == 0.0) 0.0 else
    cumlativeDistanceFromRoute / cumulativePathDistance
  def lengthRatio = if (queryRouteDistance == 0.0) 0.0 else cumulativePathDistance / queryRouteDistance
  def headingDeviationRelativeToRoute = if (queryRouteStddevOfHeading > 0.0)
    Math.abs(stddevOfHeading - queryRouteStddevOfHeading) / queryRouteStddevOfHeading else 1.0
  def distanceFromRouteScore = Math.exp(-1.0 * normalizedDistanceFromRoute)
  def constantSpeedScore = if (speedStats.avg > 0.0) Math.exp(-1.0 * speedStats.stddev / speedStats.avg) else 0.0
  def expectedLengthScore = if (lengthRatio < 1.0) lengthRatio else 1.0 / lengthRatio
  def headingDeviationScore = Math.exp(-1.0 * headingDeviationRelativeToRoute)
  def reasonableSpeedScore = 1.0
  def combined = Math.pow(Math.log(numberOfPings) * distanceFromRouteScore * constantSpeedScore *
    expectedLengthScore * headingDeviationScore * reasonableSpeedScore, 1.0 / 6.0)
}

class Route(val route: LineString) {
  case class RouteDistances(coordinateDistance: Double, metricDistance: Double)

  def distance: RouteDistances = {
    val routeCoords = route.getCoordinates.toList
    val first = routeCoords.slice(0, routeCoords.length - 1)
    val routeCoordPairs = first.zip(routeCoords.tail)
    val routeDistances = routeCoordPairs.map { ls =>
      val scc = ls._1
      val ecc = ls._2
      val coordDist = math.sqrt((ecc.x - scc.x) * (ecc.x - scc.x) + (ecc.y - scc.y) * (ecc.y - scc.y))
      val orthoDist = JTS.orthodromicDistance(scc, ecc, DefaultGeographicCRS.WGS84)
      (coordDist, orthoDist)
    }
    RouteDistances(routeDistances.map(_._1).sum, routeDistances.map(_._2).sum)
  }

  def motionScores(coordSeq: CoordSequence,
                   routeDivisions: Double = RankingDefaults.defaultRouteDivisions): MotionScore = {
    val routeDistances = distance
    val routeDistance = routeDistances.metricDistance
    val coordDistance = coordSeq.distance
    val coordDelta = routeDistances.coordinateDistance / routeDivisions
    val coordsToRouteDistance = cumlativeDistanceToCoordSequence(coordSeq, coordDelta)
    val speedStats = coordSeq.speedStats
    new MotionScore(coordSeq.coords.length + 1, coordDistance, routeDistance, coordsToRouteDistance, speedStats, 0.0, 0.0)
  }

  def cumlativeDistanceToCoordSequence(coords: CoordSequence, coordDelta: Double): Double = {
    val geomFactory = new GeometryFactory(new PrecisionModel(), 4326)
    coords.coords.foldLeft(0.0) { (dist, pair) =>
      val first = pair.first
      val second = pair.last
      val ls = geomFactory.createLineString(List(new Coordinate(first.c.x, first.c.y),
        new Coordinate(second.c.x, second.c.y)).toArray)
      val routeIndexed = new LocationIndexedLine(route)
      val lsCursor = new LineStringCursor(ls)
      lsCursor.moveTo(0)
      dist + (0.0 until ls.getLength by coordDelta).map { cd =>
        lsCursor.moveRelative(coordDelta)
        val cp = lsCursor.getCurrentPosition
        val location = routeIndexed.project(cp)
        val routePt = routeIndexed.extractPoint(location)
        JTS.orthodromicDistance(cp, routePt, DefaultGeographicCRS.WGS84)
      }.sum
    }
  }

}
