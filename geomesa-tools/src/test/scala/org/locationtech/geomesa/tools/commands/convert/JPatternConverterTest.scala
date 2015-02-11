package org.locationtech.geomesa.tools.commands.convert

import com.beust.jcommander.JCommander
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.commands.PatternParams
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JPatternConverterTest extends Specification {

  "JPatternConverter" should {
    "convert strings into patterns" in {
      val params = new PatternParams()
      val jc = new JCommander()
      jc.addConverterFactory(new GeoMesaIStringConverterFactory)
      jc.addObject(params)
      jc.parse(Array("-pt", "foobar\\d+").toArray: _*)
      params.pattern.pattern() mustEqual "foobar\\d+"
      params.pattern.matcher("foobar3").matches mustEqual true
    }

    "allow nulls" in {
      val params = new PatternParams()
      val jc = new JCommander()
      jc.addConverterFactory(new GeoMesaIStringConverterFactory)
      jc.addObject(params)
      jc.parse(Array("").toArray: _*)
      params.pattern must beNull
    }
  }
}
