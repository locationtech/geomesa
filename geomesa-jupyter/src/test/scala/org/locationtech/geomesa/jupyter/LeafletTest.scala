package org.locationtech.geomesa.jupyter

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LeafletTest extends Specification {

  private val gf = JTSFactoryFinder.getGeometryFactory
  def clean(s: String) = s.replaceAll("\n","").replaceAll(" ", "")

  "L should render" >> {
    "a WMS map" >> {
      val rendered = clean(L.WMSLayer("foo").render)
      clean(
        """
          |L.WMS.source('/geoserver/wms?',
          |  {
          |     layers: 'foo',
          |     cql_filter: "INCLUDE",
          |     styles: '',
          |     env: '',
          |     transparent: 'true',
          |     opacity: 0.6,
          |     format: 'image/png',
          |     version: '1.1.1'
          |  }).getLayer('foo').addTo(map);
        """.stripMargin) must be equalTo rendered
    }

    "a polygon" >> {
      import L._
      val rendered = clean(gf.createPolygon(Array((10,10),(10,11),(11,11),(11,10),(10,10)).map { case (x,y) => new Coordinate(x,y)}).render)
      "L.polygon([[10.0,10.0],[11.0,10.0],[11.0,11.0],[10.0,11.0],[10.0,10.0]],{fillOpacity:0}).addTo(map);" must be equalTo rendered
    }
  }


}
