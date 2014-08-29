package org.locationtech.geomesa.core.process.rank

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
}
