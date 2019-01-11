/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jupyter

import org.locationtech.jts.geom.Coordinate
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
      implicit val renderer = gf.createPolygon(Array((10,10),(10,11),(11,11),(11,10),(10,10)).map { case (x,y) => new Coordinate(x,y)})
      val rendered = clean(JTSPolyLayer(StyleOptions()).render)
      println(rendered)
      s"L.polygon([[10.0,10.0],[11.0,10.0],[11.0,11.0],[10.0,11.0],[10.0,10.0]],${clean(StyleOptions().render)}).addTo(map);" must be equalTo rendered
    }
  }


}
