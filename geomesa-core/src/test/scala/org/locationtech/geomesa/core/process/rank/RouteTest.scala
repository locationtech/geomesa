package org.locationtech.geomesa.core.process.rank

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory
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
      val ms = MotionScore(30, 1234.0, 1000.0, 312.0, SpeedStatistics(50.0, 35.0, 42.0, 3.0), 0.2, 0.3)

      ms.normalizedDistanceFromRoute must be_~(0.253) // 312/1234
      ms.lengthRatio must be_~(1.234) // 1234/1000
      ms.headingDeviationRelativeToRoute must be_~(0.333) // 0.1 / 0.3
      ms.distanceFromRouteScore must be_~(0.777) // exp(-0.253)
      ms.constantSpeedScore must be_~(0.931) // exp(-3/42)
      ms.expectedLengthScore must be_~(0.810) // 1/1.234
      ms.headingDeviationScore must be_~(0.717) // exp(-0.333) from relative deviation above
      ms.reasonableSpeedScore must be_~(1.0) // constant
      ms.combined must be_~(1.061)
    }
  }

  "Route" should {

    val epsilon = 0.000005
    def be_~(d: Double) = beCloseTo(d, epsilon)

    val coordSequenceFactory = CoordinateArraySequenceFactory.instance()
    val geomFactory = new GeometryFactory(new PrecisionModel(), 4326)

    def lineString(xy: (Double, Double)*) =
      new LineString(coordSequenceFactory.create(xy.map(c => new Coordinate(c._1, c._2)).toArray), geomFactory)

    val cvilleLS = lineString((-78.5, 38.0), (-78.4, 38.2), (-78.41, 38.25), (-78.49, 38.23))
    val cvilleRoute = new Route(cvilleLS)

    val idlLS = lineString((179.92, 38.0), (-179.98, 38.2), (-179.99, 38.25), (179.93, 38.23))
    val idlRoute = new Route(idlLS)

    "calculate distances correctly for generic route" in {
      val dists = cvilleRoute.distance

      dists.coordinateDistance must be_~(0.35706)
      dists.metricDistance must be_~(36835.42331)
    }

    "calculate distances correctly for IDL wrapping route" in {
      val dists = idlRoute.distance

      // distances should be the same as for the cville route
      dists.coordinateDistance must be_~(0.35706)
      dists.metricDistance must be_~(36835.42331)
    }
  }
}
