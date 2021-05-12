/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php. 
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

<<<<<<< HEAD
=======
import java.util.concurrent.ConcurrentHashMap

>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.geotools.referencing.CRS
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.locationtech.jts.geom._
import org.opengis.referencing.operation.MathTransform

import java.util.concurrent.ConcurrentHashMap

class GeometryFunctionFactory extends TransformerFunctionFactory {

  import GeometryFunctionFactory.{coord, coordM, coordZ, coordZM}

  override def functions: Seq[TransformerFunction] =
    Seq(pointParserFn, pointMParserFn, multiPointParserFn, lineStringParserFn, multiLineStringParserFn,
      polygonParserFn, multiPolygonParserFn, geometryCollectionParserFn, geometryParserFn, projectFromParserFn)

  private val gf = JTSFactoryFinder.getGeometryFactory

  private val pointParserFn = TransformerFunction.pure("point") {
    case Array(g: Point) => g
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
    case Array(x: Number, y: Number) => gf.createPoint(coord(x, y))
    case Array(x: Number, y: Number, z: Number) => gf.createPoint(coordZ(x, y, z))
    case Array(x: Number, y: Number, z: Number, m: Number) => gf.createPoint(coordZM(x, y, z, m))
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[Point]
    case Array(g: Array[Byte]) => WKBUtils.read(g).asInstanceOf[Point]
<<<<<<< HEAD
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
    case Array(x: Number, y: Number) => gf.createPoint(new Coordinate(x.doubleValue, y.doubleValue))
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[Point]
    case Array(g: Array[Byte]) => WKBUtils.read(g).asInstanceOf[Point]
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    case Array(x: Number, y: Number) => gf.createPoint(coord(x, y))
    case Array(x: Number, y: Number, z: Number) => gf.createPoint(coordZ(x, y, z))
    case Array(x: Number, y: Number, z: Number, m: Number) => gf.createPoint(coordZM(x, y, z, m))
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[Point]
    case Array(g: Array[Byte]) => WKBUtils.read(g).asInstanceOf[Point]
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5b8b0204a2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67cac81865 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b6805366a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 91bbd30a5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
=======
    case Array(x: Number, y: Number) => gf.createPoint(new Coordinate(x.doubleValue, y.doubleValue))
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[Point]
    case Array(g: Array[Byte]) => WKBUtils.read(g).asInstanceOf[Point]
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
=======
    case Array(x: Number, y: Number) => gf.createPoint(new Coordinate(x.doubleValue, y.doubleValue))
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[Point]
    case Array(g: Array[Byte]) => WKBUtils.read(g).asInstanceOf[Point]
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 31039e261 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6bd64e630 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
    case Array(x: Number, y: Number) => gf.createPoint(coord(x, y))
    case Array(x: Number, y: Number, z: Number) => gf.createPoint(coordZ(x, y, z))
    case Array(x: Number, y: Number, z: Number, m: Number) => gf.createPoint(coordZM(x, y, z, m))
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[Point]
    case Array(g: Array[Byte]) => WKBUtils.read(g).asInstanceOf[Point]
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 31039e261 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5b8b0204a2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
    case Array(x: Number, y: Number) => gf.createPoint(coord(x, y))
    case Array(x: Number, y: Number, z: Number) => gf.createPoint(coordZ(x, y, z))
    case Array(x: Number, y: Number, z: Number, m: Number) => gf.createPoint(coordZM(x, y, z, m))
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[Point]
    case Array(g: Array[Byte]) => WKBUtils.read(g).asInstanceOf[Point]
=======
>>>>>>> 3c071a6b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 3c071a6b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6bd64e630 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 91bbd30a5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67cac81865 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 3c071a6b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1b6805366a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args if args.nonEmpty && args.lengthCompare(4) <= 0 && args.forall(_ == null) => null
<<<<<<< HEAD
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
>>>>>>> locationtech-main
=======
=======
    case Array(x: Float, y: Double) => gf.createPoint(new Coordinate(x, y))
    case Array(x: Double, y: Float) => gf.createPoint(new Coordinate(x, y))
    case Array(null) | Array(null, null) => null
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case args => throw new IllegalArgumentException(s"Invalid point conversion argument: ${args.mkString(",")}")
  }

  private val pointMParserFn = TransformerFunction.pure("pointM") {
    case Array(x: Number, y: Number, m: Number) => gf.createPoint(coordM(x, y, m))
    case Array(null, null, null) => null
    case args => throw new IllegalArgumentException(s"Invalid pointM conversion argument: ${args.mkString(",")}")
  }

  private val multiPointParserFn = TransformerFunction.pure("multipoint") {
    case Array(g: MultiPoint) => g
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[MultiPoint]
    case Array(g: Array[Byte]) => WKBUtils.read(g).asInstanceOf[MultiPoint]
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> locationtech-main
=======
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5b8b0204a2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 91bbd30a5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67cac81865 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 31039e261 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5b8b0204a2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6bd64e630 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 91bbd30a5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67cac81865 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3c071a6b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1b6805366a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case Array(x: java.util.List[_], y: java.util.List[_]) =>
      val coords = Array.ofDim[Coordinate](x.size)
      var i = 0
      while (i < coords.length) {
        coords(i) = new Coordinate(x.get(i).asInstanceOf[Number].doubleValue, y.get(i).asInstanceOf[Number].doubleValue)
        i += 1
      }
      gf.createMultiPointFromCoords(coords)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 31039e261 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6bd64e630 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3c071a6b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b6805366a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 31039e261 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6bd64e630 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3c071a6b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b6805366a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 31039e261 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5b8b0204a2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6bd64e630 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 91bbd30a5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67cac81865 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 3c071a6b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1b6805366a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
<<<<<<< HEAD
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case Array(null) => null
    case args => throw new IllegalArgumentException(s"Invalid multipoint conversion argument: ${args.mkString(",")}")
  }

  private val lineStringParserFn = TransformerFunction.pure("linestring") {
    case Array(g: LineString) => g
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[LineString]
    case Array(g: Array[Byte]) => WKBUtils.read(g).asInstanceOf[LineString]
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> locationtech-main
=======
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5b8b0204a2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 91bbd30a5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 67cac81865 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 31039e261 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5b8b0204a2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6bd64e630 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 91bbd30a5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67cac81865 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3c071a6b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1b6805366a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case Array(x: java.util.List[_], y: java.util.List[_]) =>
      val coords = Array.ofDim[Coordinate](x.size)
      var i = 0
      while (i < coords.length) {
        coords(i) = new Coordinate(x.get(i).asInstanceOf[Number].doubleValue, y.get(i).asInstanceOf[Number].doubleValue)
        i += 1
      }
      gf.createLineString(coords)
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4073c9b17 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 28e2bfa2d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b4320946b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 31039e261 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6bd64e630 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3c071a6b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1b6805366a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 31039e261 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6bd64e630 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 3c071a6b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1b6805366a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 87bf55340 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 468ab7139 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 31039e261 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 1cc1e5df3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> e847488d8 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 240977229 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 40667d637 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c269144f6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 3dd99aed4 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 45ad5d11f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 3f50ce5c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> fa6116f23 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4524373ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 67b361c85 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> dace2085b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 42d7a4d00 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 4fbe39c62 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 516fe7e9c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a05012d5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5b8b0204a2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> d9ed077cd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 81ee66102b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b54485f5a2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 6bd64e630 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> fdaa0e12fd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> dd53c5e670 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> b1c506839 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> f71fa3c0e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c5e182765 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> f9f6fde40b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9d708b240 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 53a3e72cbf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b603094de2 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 9990cd7dc (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 72976786c9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 6a3ccffd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9d588fa05d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 5f428db97 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 6a2cd10c1c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 76618c8da (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 947761ec9b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0e2352ff0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 62ac58128c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 61208268c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1ea954982b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 83670fc93 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 70391b93ef (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 89bdd3013 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8a6a96234b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> c8a325e13 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> c9328eddce (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> a7ca1d57e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e8299f1551 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 4f0e2712a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 775ef588c2 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9e8a3c11c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2a3da881ff (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 72bafaf54 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 0c734cda2b (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 97ec7d864 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> d94c03a98e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c384956301 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
=======
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> a9f01eaaf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 314e6ef1a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 91bbd30a5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67cac81865 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b8b8fda534 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 6d9a5b626 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 67e4c1354f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d79d99099b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 3c071a6b3 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 1b6805366a (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 027d1bef2e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 947a867d1 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 748532d2db (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> cdb410251 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> e6e1f4bdd (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 8035cc5893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5e763f098 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c5ed86cdfd (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 7e68948ac (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 10910a486 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 4cf81b3bd9 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
<<<<<<< HEAD
>>>>>>> a990ff330 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a38616a270 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 6eb31fb65 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 23c024179 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 250f01786f (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
=======
>>>>>>> b9bdd406e (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 0e68e9f4c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> a910ae135 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 07161eaaa6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a9e851ba1f (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 941104fee (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 13fc760651 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 5ee6153cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 5dfb7bed65 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 07a6a5c29 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 85663f71c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 813488e18c (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> ad3cedc4d (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 9abc630fdf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> d86a5b91d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 25aee54981 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> d6616d893 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> 2c6296264 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
<<<<<<< HEAD
>>>>>>> 7121ca80c5 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
=======
=======
>>>>>>> 8170c2c54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 010e5ce908 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d4c5768cf (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
>>>>>>> fe51719bc6 (GEOMESA-3061 Converters - support bytes in Avro top-level union types (#2762))
    case Array(null) => null
    case args => throw new IllegalArgumentException(s"Invalid linestring conversion argument: ${args.mkString(",")}")
  }

  private val multiLineStringParserFn = TransformerFunction.pure("multilinestring") {
    case Array(g: MultiLineString) => g
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[MultiLineString]
    case Array(g: Array[Byte]) => WKBUtils.read(g)
    case Array(null) => null
    case args => throw new IllegalArgumentException(s"Invalid multilinestring conversion argument: ${args.mkString(",")}")
  }

  private val polygonParserFn = TransformerFunction.pure("polygon") {
    case Array(g: Polygon) => g
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[Polygon]
    case Array(g: Array[Byte]) => WKBUtils.read(g).asInstanceOf[Polygon]
    case Array(null) => null
    case args => throw new IllegalArgumentException(s"Invalid polygon conversion argument: ${args.mkString(",")}")
  }

  private val multiPolygonParserFn = TransformerFunction.pure("multipolygon") {
    case Array(g: MultiPolygon) => g
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[MultiPolygon]
    case Array(g: Array[Byte]) => WKBUtils.read(g).asInstanceOf[MultiPolygon]
    case Array(null) => null
    case args => throw new IllegalArgumentException(s"Invalid multipolygon conversion argument: ${args.mkString(",")}")
  }

  private val geometryCollectionParserFn = TransformerFunction.pure("geometrycollection") {
    case Array(g: GeometryCollection) => g.asInstanceOf[GeometryCollection]
    case Array(g: String) => WKTUtils.read(g).asInstanceOf[GeometryCollection]
    case Array(g: Array[Byte]) => WKBUtils.read(g)
    case Array(null) => null
    case args => throw new IllegalArgumentException(s"Invalid geometrycollection conversion argument: ${args.mkString(",")}")
  }

  private val geometryParserFn = TransformerFunction.pure("geometry") {
    case Array(g: Geometry) => g
    case Array(g: String) => WKTUtils.read(g)
    case Array(g: Array[Byte]) => WKBUtils.read(g)
    case Array(null) => null
    case args => throw new IllegalArgumentException(s"Invalid geometry conversion argument: ${args.mkString(",")}")
  }

  private val projectFromParserFn: TransformerFunction = new NamedTransformerFunction(Seq("projectFrom"), pure = true) {

    import scala.collection.JavaConverters._

    private val cache = new ConcurrentHashMap[String, MathTransform].asScala

    override def apply(args: Array[AnyRef]): AnyRef = {
      import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326

      val geom = args(1).asInstanceOf[Geometry]
      if (geom == null) { null } else {
        val epsg = args(0).asInstanceOf[String]
        val lenient = if (args.length > 2) { java.lang.Boolean.parseBoolean(args(2).toString) } else { true }
        // transforms should be thread safe according to https://sourceforge.net/p/geotools/mailman/message/32123017/
        val transform = cache.getOrElseUpdate(s"$epsg:$lenient",
          CRS.findMathTransform(CRS.decode(epsg), CRS_EPSG_4326, lenient))
        JTS.transform(geom, transform)
      }
    }
  }
}

object GeometryFunctionFactory {

  private def coord(x: Number, y: Number): Coordinate = new CoordinateXY(x.doubleValue, y.doubleValue)

  private def coordZ(x: Number, y: Number, z: Number): Coordinate =
    new Coordinate(x.doubleValue, y.doubleValue, z.doubleValue)

  private def coordM(x: Number, y: Number, m: Number): Coordinate =
    new CoordinateXYM(x.doubleValue, y.doubleValue, m.doubleValue)

  private def coordZM(x: Number, y: Number, z: Number, m: Number): Coordinate =
    new CoordinateXYZM(x.doubleValue, y.doubleValue, z.doubleValue, m.doubleValue)
}
