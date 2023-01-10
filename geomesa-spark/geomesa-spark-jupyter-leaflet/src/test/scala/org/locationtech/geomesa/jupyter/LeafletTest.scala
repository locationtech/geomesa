/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jupyter

import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.jts.geom.Coordinate
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
          |
        """.stripMargin) must be equalTo rendered
    }

    "a polygon" >> {
      import L._
      implicit val renderer = gf.createPolygon(Array((10,10),(10,11),(11,11),(11,10),(10,10)).map { case (x,y) => new Coordinate(x,y)})
      val rendered = clean(JTSPolyLayer(StyleOptions()).render)
      s"L.polygon([[10.0,10.0],[11.0,10.0],[11.0,11.0],[10.0,11.0],[10.0,10.0]],${clean(StyleOptions().render)}).addTo(map);" mustEqual rendered
    }

  }

  "L without path should include CDN links" >> {
    val html = L.buildMap(Seq(), (0,0), 8, None)
    html must contain(L.leafletCssCdn) and contain (L.leafletJsCdn) and contain (L.leafletWmsJsCdn)
  }

  "L with path should not include CDN links" >> {
    val html = L.buildMap(Seq(), (0,0), 8, Some("js"))
    html must not contain(L.leafletCssCdn) and not contain (L.leafletJsCdn) and not contain (L.leafletWmsJsCdn)
  }


}
