package org.locationtech.geomesa.feature

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

/**
  * Created by mmatz on 4/2/15.
  */
@RunWith(classOf[JUnitRunner])
class TextEncodingTest extends Specification {

  sequential

  "TextEncoding" should {

    val input = "id=name|point|date"
    val vis = "A&B&(C|D)"

    "correctly add visibility" >> {
      val result = TextEncoding.addVisibility(input, vis)
      result must contain(input)
      result must contain(vis)
    }

    "correctly split visibility" >> {
      val combined = TextEncoding.addVisibility(input, vis)

      val (remainingResult, visResult) = TextEncoding.splitVisibility(combined)
      remainingResult mustEqual input
      visResult mustEqual vis
    }

    "correctly split empty visibility" >> {
      val combined = TextEncoding.addVisibility(input, "")

      val (remainingResult, visResult) = TextEncoding.splitVisibility(combined)
      remainingResult mustEqual input
      visResult mustEqual ""
    }

    "fail to split when no visibility" >> {
      TextEncoding.splitVisibility(input) must throwA[IllegalArgumentException]
    }

    "fail to split when multiple visibilities" >> {
      val badInput = TextEncoding.addVisibility(TextEncoding.addVisibility(input, vis) + "|foo", "B|D|F")
      TextEncoding.splitVisibility(badInput) must throwA[IllegalArgumentException]
    }
  }
}
