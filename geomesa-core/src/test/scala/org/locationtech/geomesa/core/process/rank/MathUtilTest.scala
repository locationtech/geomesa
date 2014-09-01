package org.locationtech.geomesa.core.process.rank

import com.vividsolutions.jts.geom.Coordinate
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MathUtilTest extends Specification {

  "MathUtil" should {

    val epsilon = 0.0000005
    def be_~(d: Double) = beCloseTo(d, epsilon)

    def asRad(c: Coordinate) = (c.x.toRadians, c.y.toRadians)

    // it's fun, here, that the East/West answers are not multiples of 90
    "calculate NEWS headings correctly" in {
      val refXY = new Coordinate(2.0, 40.0)
      val refR = asRad(refXY)
      def d(dx: Double, dy: Double) = asRad(new Coordinate(refXY.x + dx, refXY.y + dy))

      MathUtil.headingGivenRadians(refR, d(0.0, 1.0)) must be_~(0.0) // head North
      MathUtil.headingGivenRadians(refR, d(1.0, 0.0)) must be_~(89.678601) // head East(ish)
      MathUtil.headingGivenRadians(refR, d(-1.0, 0.0)) must be_~(-89.678601) // head West(ish)
      MathUtil.headingGivenRadians(refR, d(0.0, -1.0)) must be_~(180.0) // head South
    }

    "calculate straightforward headingDifference correctly" in {
      MathUtil.headingDifference(-10.0, 10.0) must be_~(20.0)
    }

    "calculate headingDifference for SW to SE correctly" in {
      val startXY = new Coordinate(0.01, 0.1)
      val midXY = new Coordinate(0.0, 0.0)
      val endXY = new Coordinate(0.01, -0.1)

      val startR = asRad(startXY)
      val midR = asRad(midXY)
      val endR = asRad(endXY)

      // verify we're heading SW for starters
      val startToMid = MathUtil.headingGivenRadians(startR, midR)
      startToMid must beLessThan(-170.0)

      // verify we're heading SE in the second half
      val midToEnd = MathUtil.headingGivenRadians(midR, endR)
      midToEnd must beGreaterThan(170.0)

      // verify that the difference is small and positive
      val diff = MathUtil.headingDifference(startToMid, midToEnd)
      diff must beGreaterThan(0.0)
      diff must beLessThan(20.0)
    }

    "calculate headingDifference for SE to SW correctly" in {
      val startXY = new Coordinate(-0.01, 0.1)
      val midXY = new Coordinate(0.0, 0.0)
      val endXY = new Coordinate(-0.01, -0.1)

      val startR = asRad(startXY)
      val midR = asRad(midXY)
      val endR = asRad(endXY)

      // verify we're heading SW for starters
      val startToMid = MathUtil.headingGivenRadians(startR, midR)
      startToMid must beGreaterThan(170.0)

      // verify we're heading SE in the second half
      val midToEnd = MathUtil.headingGivenRadians(midR, endR)
      midToEnd must beLessThan(-170.0)

      // verify that the difference is small and positive
      val diff = MathUtil.headingDifference(startToMid, midToEnd)
      diff must beGreaterThan(0.0)
      diff must beLessThan(20.0)
    }
  }
}
