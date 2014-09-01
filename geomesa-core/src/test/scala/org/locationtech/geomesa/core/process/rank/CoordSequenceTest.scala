package org.locationtech.geomesa.core.process.rank

import com.vividsolutions.jts.geom.Coordinate
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CoordSequenceTest extends Specification {

  "CoordWithDateTimePair" should {

    val epsilon = 0.0000005
    def be_~(d: Double) = beCloseTo(d, epsilon)

    val first = new CoordWithDateTime(new Coordinate(1.0, 40.0), new DateTime(2014, 8, 28, 6, 2, 30))
    val second = new CoordWithDateTime(new Coordinate(2.0, 41.0), new DateTime(2014, 8, 28, 6, 3, 45))
    val commonPair = CoordWithDateTimePair(first, second)

    "return time difference in seconds" in {
      commonPair.timeDiff mustEqual 75.0
    }

    "return distance orthodromically" in {
      commonPair.distance must be_~(139698.755392)
    }

    "calculate heading correctly" in {
      commonPair.heading must be_~(36.925885)

      val revPair = CoordWithDateTimePair(second, first)
      revPair.heading must be_~(-142.424633)
    }

  }

}
