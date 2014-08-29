package org.locationtech.geomesa.core.process.rank

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

/**
 * Created by nhamblet.
 */
@RunWith(classOf[JUnitRunner])
class RouteTest extends Specification {

  "MotionScore" should {

    val epsilon = 0.0005
    def be_~(d: Double) = beCloseTo(d, epsilon)

    "calculate derived scores sensibly" in {
      val ms = MotionScore(30, 1234.0, 1000.0, 312.0, SpeedStatistics(50.0, 35.0, 42.0, 3.0))

      ms.normalizedDistanceFromRoute must be_~(0.253) // 312/1234
      ms.lengthRatio must be_~(1.234) // 1234/1000
      ms.distanceFromRouteScore must be_~(0.777) // exp(-0.253)
      ms.constantSpeedScore must be_~(0.931) // exp(-3/42)
      ms.expectedLengthScore must be_~(0.810) // 1/1.234
      ms.reasonableSpeedScore must be_~(1.0) // constant
      ms.combined must be_~(1.148)
    }
  }

  "Route" should {

    val epsilon = 0.000005
    def be_~(d: Double) = beCloseTo(d, epsilon)

    val coordSequenceFactory = CoordinateArraySequenceFactory.instance()

    def ls(xy: (Double, Double)*) =
      Route.geomFactory.createLineString(xy.map { case (x, y) => new Coordinate(x, y) }.toArray)

    val cvilleLS = ls((-78.5, 38.0), (-78.4, 38.2), (-78.41, 38.25), (-78.49, 38.23))
    val cvilleRoute = new Route(cvilleLS)

    val idlLS = ls((179.92, 38.0), (-179.98, 38.2), (-179.99, 38.25), (179.93, 38.23))
    val idlRoute = new Route(idlLS)

    // these two routes are "translations" of eachother, with equivalent latitudes, so should have the same distance
    val commonDistance = 36835.42331

    "calculate distances correctly for generic route" in {
      cvilleRoute.distance must be_~(commonDistance)
    }

    "calculate distances correctly for IDL wrapping route" in {
      // distances should be the same as for the cville route
      idlRoute.distance must be_~(commonDistance)
    }

    "have 0 distance to itself" in {
      val constDT = new DateTime(2014, 8, 29, 10, 38, 12)
      val cvilleCS = CoordSequence.fromCoordWithDateTimeList(
        cvilleLS.getCoordinates.map(c => new CoordWithDateTime(new Coordinate(c.x, c.y), constDT)).toList
      )
      cvilleRoute.cumlativeDistanceToCoordSequence(cvilleCS, 100.0) must be_~(0.0)
    }

    "have positive distance to minor variation" in {
      val constDT = new DateTime(2014, 8, 29, 10, 38, 12)
      val cvilleLSmod = ls((-78.5001, 38.0007), (-78.4004, 38.2002), (-78.41005, 38.25004), (-78.49007, 38.23006))
      val cvilleCSmod = CoordSequence.fromCoordWithDateTimeList(
        cvilleLSmod.getCoordinates.map(c => new CoordWithDateTime(new Coordinate(c.x, c.y), constDT)).toList
      )
      cvilleRoute.cumlativeDistanceToCoordSequence(cvilleCSmod, 100.0) must be_~(2901.28187)
    }
  }
}
