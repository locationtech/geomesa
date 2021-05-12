/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.shp

<<<<<<< HEAD
import com.codahale.metrics.Counter
import com.typesafe.scalalogging.LazyLogging
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import org.geotools.api.data.{DataStoreFinder, Query}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
=======
=======
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4d245d0d00f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 15b6bf02d15 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f960fee3bbb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 86549c8cb31 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e3538892b8f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d788072e189 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> bca28211b26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import java.io.InputStream
import java.util.Collections
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
import com.codahale.metrics.Counter
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 302df4295a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e258539c2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1db60d5575 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 857dd0a396 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d64b1bd01f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 235643d3d2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d2042eea397 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 2bbe8abc305 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3b338b30f9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d9df30f610b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e703f8474af (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b110409ccde (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f63186e675 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e4e4766a49 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 235643d3d2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
=======
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 3e258539c2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e4e4766a49 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> d64b1bd01f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 107d2814d83 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8401bce4102 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 01f7701037b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 626b1ac8ca (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 367955da6da (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 184b58210c9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 99942875696 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 29d4d5036d7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> adc8eabfb02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0699408297d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0e693a1ea3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> a68681cf5f1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 04b9b7c79a2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9ac51f780b7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 29b01d02901 (Merge branch 'feature/postgis-fixes')
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 33511dd1c3b (Merge branch 'feature/postgis-fixes')
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD

import com.codahale.metrics.Counter
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> abc9ceb3d7d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> a82d3e0f0b9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 308ee781cfa (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 4c39b13fdb9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f4624130386 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> fd4b7d818e1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> f63186e675 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 1e2d9188546 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b14f292888f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dbdff4c3a88 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 626b1ac8c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e4e4766a49 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 07b00fc1a87 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 60df807de4a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7349caccc38 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> bb915316660 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d1d849c5d0e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 235643d3d2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c8dc147f252 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0e693a1ea (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> f854cfe7bf4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> af68691d6fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 10be5d2340b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 709d36fab5e (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
import java.io.InputStream
import java.util.Collections
<<<<<<< HEAD
<<<<<<< HEAD
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
import com.codahale.metrics.Counter
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9be77ec4a5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 667969827b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> acca663a98 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ea600ce0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 667969827b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> acca663a98 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ea600ce0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e4e4766a4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9be77ec4a5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4d245d0d00f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d0b25219f34 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 83b0de52f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9a7539a7217 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d3b57f0af6d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 83b0de52f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ded1d4370 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
=======
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6c850ef227 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> f55112e2dd9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 101b2c45778 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3e81ddad5ab (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 626b1ac8c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e3f60478f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 71312acd822 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 69d0250eb0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3e1e323980f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 83b0de52f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 194497ed923 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 998de7e5d12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8ed2e555e44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3ea600ce0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 12b7785136 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0e693a1ea (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 32bd718240 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d6f24eac2c8 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 12b7785136 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cd902990736 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> aaac9bf1b27 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
>>>>>>> 3b1441ba6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> db99a96dac1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> af3ad095e35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1de0c0109c0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 49d78f3faf5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 392f0ed1292 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 32a3a53f7b2 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8c068b490fe (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 61fab3ea79 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 1b95f558e5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 230d23e4efd (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3c5c29f94e1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1eff02937fd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6ded1d4370 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 280f7eb6b70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cfaead7e54a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 42e4fa76b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 626b1ac8c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e4e4766a4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9be77ec4a5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 945b52f102 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 291a4503c9f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0727aaeebdc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c6ccd26e0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 667969827b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 7a743b8db65 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 156e2c2bb1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2c4ad73f132 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 3ea600ce0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> acca663a98 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 12b7785136 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9e3d035add5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f5b38acde9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 3e6c36184cd (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 15b6bf02d15 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
import java.io.InputStream
import java.util.Collections
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
<<<<<<< HEAD

import com.codahale.metrics.Counter
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4688de07e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f960fee3bbb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dffd024e87 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9fcae8178 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> baee3d5af7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 01394c7a2bc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 52e4edb8ff7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 04ff4c2eabd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
<<<<<<< HEAD
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6c850ef22 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> c0b7ef9ee3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ef6333d869 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 794721b29f1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 10892515265 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 80e53781872 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 626b1ac8c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e3f60478f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b4b8bd326c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0e00e7f5af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> de72b46b988 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9a1c0e7c9da (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 702db0bfee9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> cff5fc6422 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d9af6b6d65 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 49afbf56035 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c4bb86aba42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397dc9e36f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0e693a1ea (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 32bd71824 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> bf2c26ed43 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 1de748a93b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> fce63d06811 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a1218cc4a1c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 8dd024f6791 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import java.io.InputStream
import java.util.Collections
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import com.codahale.metrics.Counter
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 86549c8cb31 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9b6e4f3d2ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> c8108ce7f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9f49cacc441 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 001e9598d37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8108ce7f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 96745b4434 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 649ec743e80 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f999ddcfadf (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5884c46afd6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 626b1ac8c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9cd57b64d7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6e48eeb634c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4b9da52f142 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c2669d51eed (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> c8108ce7f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 8a5d045449c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1316cb79ab5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8670005b874 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0e693a1ea (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> be9ef91d8a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b20b1ecaf17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b89af2347e3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 64a645069b5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import java.io.InputStream
import java.util.Collections
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import com.codahale.metrics.Counter
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e3538892b8f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 490baa117c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 4b59906e258 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 50e677e046 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a72d5e76aa8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 302df4295a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e703f8474af (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3e258539c2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b110409ccde (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66d19a98428 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1db60d5575 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 508b1195899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 626b1ac8ca (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 857dd0a396 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> eeefce2b6b4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2012000cea3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 946c54d73c0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 48d444938bd (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 07fd0942d49 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 41e9ed1c37d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0e693a1ea3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> dffd024e87 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 1a4b5153fcf (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9ac51f780b7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 29b01d02901 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5aaa49b4df0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 15cb7c5b2de (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4c39b13fdb9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4624130386 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> f63186e675 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 7365c58ca32 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 001e2fde484 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 73ef9e5c686 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 626b1ac8c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e4e4766a49 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d64b1bd01f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b1f20d3d402 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7801ae9dc3b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b398d4187e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> fa83950256b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b4bd39326ff (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2787ace19dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 235643d3d2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 52fb8c6c354 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2cc10cb7932 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 709d36fab5e (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
=======
import java.io.InputStream
import java.util.Collections
<<<<<<< HEAD
<<<<<<< HEAD
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import com.codahale.metrics.Counter
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9be77ec4a5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e4e4766a4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9be77ec4a5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d788072e189 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ea67435711d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 83b0de52f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 49471dea7a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> eb1847becc6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0292e04e31 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66d5d413b21 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 83b0de52f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ded1d4370 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6c850ef227 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 7e0e4fb5c3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 2ee4acccaee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d43ceee7c5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> df56e0b4fd4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e39c04d2ed (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3a2237177d5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 626b1ac8c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e3f60478f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 27764e5de8 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 582945a456a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 56470bf31b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e1bc7bc0c7d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 653147a99e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b712cf79eb0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 83b0de52f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 5753704fcc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0d8a845d6eb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a349824633e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3b3b527400 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9b10810a151 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0e693a1ea (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 32bd718240 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> a7118a678b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0f71e17ddc3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b59955740a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 921ed24fe98 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> ae9ccc22c0e (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
>>>>>>> 3b1441ba6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8e6907185ac (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9a8065c960 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c4561e0da47 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2486842735 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c856252e4bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 622d048a77 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 07d5ec61379 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 476df18562 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> de0d3bd4725 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> ab46ac2ae83 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a475643048f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 61fab3ea79 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 1b95f558e5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> c7eab50caf (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> cdd323759f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 809016b858 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 58dd8d20ca1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2e3aa14e0f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8ab33667700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6ded1d4370 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 7685386a5c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 90d1607d00b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0100ea68c90 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 34eaf97eb7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f9a29f55978 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 626b1ac8c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e4e4766a4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9be77ec4a5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 945b52f102 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 2b1173bf03 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> f543164b453 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 34a0684bb3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6ed24c3a4d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 994cb7be424 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 667969827b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> debf6617578 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 03ad1e4fcb3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c11cab6e6c4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 3ea600ce0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> acca663a98 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 12b7785136 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> a5843d8391f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9a1e75a1629 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> b2c5cb17bee (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
=======
import java.io.InputStream
import java.util.Collections
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
=======
>>>>>>> e0403a237f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bca28211b26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9fcae8178 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> baee3d5af7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 967de7cfd6c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e4688de07e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a1bf28a76ed (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e0403a237f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb81be58288 (GEOMESA-3071 Move all converter state into evaluation context)
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.referencing.CRS
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.AbstractConverter
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicField, BasicOptions}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326
import org.locationtech.geomesa.utils.io.{CloseWithLogging, PathUtils}
import org.locationtech.geomesa.utils.text.TextTools

import java.io.InputStream
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util.Collections
import scala.collection.mutable.ArrayBuffer

class ShapefileConverter(sft: SimpleFeatureType, config: BasicConfig, fields: Seq[BasicField], options: BasicOptions)
    extends AbstractConverter[SimpleFeature, BasicConfig, BasicField, BasicOptions](sft, config, fields, options)  {

  import org.locationtech.geomesa.convert.shp.ShapefileFunctionFactory.{InputSchemaKey, InputValuesKey}

  override def createEvaluationContext(
      globalParams: Map[String, Any],
      success: Counter,
      failure: Counter): EvaluationContext = {
    // inject placeholders for shapefile attributes into the evaluation context
    // used for accessing shapefile properties by name in ShapefileFunctionFactory
    val shpParams = Map(InputSchemaKey -> ArrayBuffer.empty[String], InputValuesKey -> ArrayBuffer.empty[AnyRef])
    super.createEvaluationContext(globalParams ++ shpParams, success, failure)
  }

  override def createEvaluationContext(
      globalParams: Map[String, Any],
      success: Counter,
      failure: Counter): EvaluationContext = {
    // inject placeholders for shapefile attributes into the evaluation context
    // used for accessing shapefile properties by name in ShapefileFunctionFactory
    val shpParams = Map(InputSchemaKey -> Array.empty[String], InputValuesKey -> Array.empty[Any])
    super.createEvaluationContext(globalParams ++ shpParams, success, failure)
  }

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[SimpleFeature] = {
    CloseWithLogging(is) // we don't use the input stream, just close it

    val path = ec.getInputFilePath.getOrElse {
      throw new IllegalArgumentException(s"Shapefile converter requires '${EvaluationContext.InputFilePathKey}' " +
          "to be set in the evaluation context")
    }
    val ds = ShapefileConverter.getDataStore(path)
    val schema = ds.getSchema()

    (ec.accessor(InputSchemaKey).apply(), ec.accessor(InputValuesKey).apply()) match {
      case (n: ArrayBuffer[String], v: ArrayBuffer[AnyRef]) =>
        n.clear()
        n ++= Array.tabulate(schema.getAttributeCount)(i => schema.getDescriptor(i).getLocalName)
        v.clear()
        v ++= Array.fill[AnyRef](n.length + 1)(null)
      case _ =>
        logger.warn("Input schema not found in evaluation context, shapefile functions " +
            s"${TextTools.wordList(new ShapefileFunctionFactory().functions.map(_.names.head))} will not be available")
    }

    val q = new Query
    // Only ask to reproject if the Shapefile has a non-4326 CRS
    if (ds.getSchema.getCoordinateReferenceSystem == null) {
      logger.warn(s"Shapefile does not have CRS info")
    } else if (!CRS.equalsIgnoreMetadata(ds.getSchema.getCoordinateReferenceSystem, CRS_EPSG_4326)) {
      q.setCoordinateSystemReproject(CRS_EPSG_4326)
    }

    val reader = CloseableIterator(ds.getFeatureSource.getReader(q)).map { f => ec.line += 1; f }

    CloseableIterator(reader, { CloseWithLogging(reader); ds.dispose() })
  }

  override protected def values(parsed: CloseableIterator[SimpleFeature],
                                ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    (ec.accessor(InputSchemaKey).apply(), ec.accessor(InputValuesKey).apply()) match {
      case (_: ArrayBuffer[String], v: ArrayBuffer[AnyRef]) =>
        parsed.map { feature =>
          var i = 1
          while (i < v.length) {
            v(i) = feature.getAttribute(i - 1)
            i += 1
          }
          v(0) = feature.getID
          v.toArray
        }

      case _ =>
        logger.warn("Input schema not found in evaluation context, shapefile functions " +
            s"${TextTools.wordList(new ShapefileFunctionFactory().functions.map(_.names.head))} will not be available")
        var array: Array[Any] = null
        parsed.map { feature =>
          if (array == null) {
            array = Array.ofDim(feature.getAttributeCount + 1)
          }
          var i = 1
          while (i < array.length) {
            array(i) = feature.getAttribute(i - 1)
            i += 1
          }
          array(0) = feature.getID
          array
        }
    }
  }
}

object ShapefileConverter extends LazyLogging {

  /**
    * Creates a URL, needed for the shapefile data store
    *
    * @param path input path
    * @return
    */
  def getDataStore(path: String): ShapefileDataStore = {
    val params = Collections.singletonMap(ShapefileDataStoreFactory.URLP.key, PathUtils.getUrl(path))
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[ShapefileDataStore]
    tryInferCharsetFromCPG(path) match {
      case Some(charset) => ds.setCharset(charset)
      case None =>
    }
    if (ds == null) {
      throw new IllegalArgumentException(s"Could not read shapefile using path '$path'")
    }
    ds
  }

  // Infer charset to decode strings in DBF file by inspecting the content of the CPG file. 
  private def tryInferCharsetFromCPG(path: String): Option[Charset] = {
    val shpDirPath = Paths.get(path).getParent
    val (baseName, _) = PathUtils.getBaseNameAndExtension(path)
    val cpgPath = shpDirPath.resolve(baseName + ".cpg")
    if (!Files.isRegularFile(cpgPath)) None else {
<<<<<<< HEAD
      val source = scala.io.Source.fromFile(cpgPath.toFile)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
      val source = io.Source.fromFile(cpgPath.toFile)
<<<<<<< HEAD
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
      val source = io.Source.fromFile(cpgPath.toFile)
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
      val source = io.Source.fromFile(cpgPath.toFile)
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
=======
=======
      val source = io.Source.fromFile(cpgPath.toFile)
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
      val source = io.Source.fromFile(cpgPath.toFile)
<<<<<<< HEAD
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 490baa117c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
      try {
        source.getLines.take(1).toList match {
          case Nil => None
          case charsetName :: _ => Some(Charset.forName(charsetName.trim))
        }
      } catch {
<<<<<<< HEAD
        case _: Exception =>
<<<<<<< HEAD
          logger.warn("Can't figure out charset from cpg file, will use default charset")
          None
      } finally {
        source.close()
      }
<<<<<<< HEAD
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
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 490baa117c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
        case e: Exception =>
          logger.warn("Can't figure out charset from cpg file, will use default charset")
          None
      } finally source.close()
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
          logger.warn("Can't figure out charset from cpg file, will use default charset")
          None
      } finally {
        source.close()
      }
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 490baa117c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
    }
  }
}
