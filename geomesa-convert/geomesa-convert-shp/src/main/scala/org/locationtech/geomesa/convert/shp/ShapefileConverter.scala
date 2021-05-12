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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
import java.io.InputStream
import java.util.Collections
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 36ff9b69f0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 36ff9b69f0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> c0205a0050 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> fc00100ff0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ebea6992b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 214fae6c43 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e35ff95571 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9f4dc60aaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81124cda89 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a2eb05093d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 6b2ee508d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6eb7d73613 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b63a4a7b59 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a3506f6924 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5869a55e95 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 302df4295a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3e258539c2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1db60d5575 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 857dd0a396 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d64b1bd01f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 235643d3d2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> aede893190 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d32495cd07 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> edc23fb02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6740db1cf8 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5824ba4b77 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b71ceab97a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3c3507f6d0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e72d6905dd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 89bb58cf10 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 401c676c9c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e36fb99905 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d8920ce086 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a05e46a12 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 1a99b6baaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6843ec6035 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ef47c713ed (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> fd162657d1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ee61d06bf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e18d6e4012 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 72b57b3b08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 39ae2e950c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c9c803fbc4 (d)
=======
>>>>>>> 323b1f8f17 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 302df4295a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6d51efa18f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c0205a0050 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> fc00100ff0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ebea6992b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 857dd0a396 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 214fae6c43 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e35ff95571 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9f4dc60aaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81124cda89 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> a2eb05093d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 6b2ee508d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6eb7d73613 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b63a4a7b59 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a3506f6924 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5869a55e95 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d51efa18f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c0205a0050 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81124cda89 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f63186e675 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a2eb05093d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 6b2ee508d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6eb7d73613 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b63a4a7b59 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e4e4766a49 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a3506f6924 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5869a55e95 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 235643d3d2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> e18d6e4012 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 72b57b3b08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 39ae2e950c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> c9c803fbc4 (d)
=======
>>>>>>> 323b1f8f17 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a3506f6924 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5869a55e95 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 72b57b3b08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
=======
=======
>>>>>>> ee61d06bf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e18d6e4012 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 72b57b3b08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c0205a0050 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 3e258539c2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e4e4766a49 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> d64b1bd01f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> aede893190 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e36fb99905 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d8920ce086 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a05e46a12 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1a99b6baaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6843ec6035 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e4e4766a49 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ef47c713ed (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> fd162657d1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ee61d06bf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c0205a0050 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81124cda89 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> a2eb05093d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 6b2ee508d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6eb7d73613 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b63a4a7b59 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> e36fb99905 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e18d6e4012 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 72b57b3b08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c0205a0050 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9f4dc60aaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9f4dc60aaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 72b57b3b08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e36fb99905 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e18d6e4012 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 72b57b3b08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9f4dc60aaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81124cda89 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> e36fb99905 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d8920ce086 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 72b57b3b08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9f4dc60aaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81124cda89 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 81124cda89 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> d8920ce086 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 72b57b3b08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81124cda89 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> fc00100ff0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e35ff95571 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> fc00100ff0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e35ff95571 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 626b1ac8ca (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> e72d6905dd (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3c3507f6d0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9a05e46a12 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ee61d06bf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e18d6e4012 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> a2eb05093d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 0e693a1ea3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 5824ba4b77 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9f4dc60aaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 36ff9b69f0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81124cda89 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
=======
>>>>>>> a2eb05093d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 6b2ee508d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ee61d06bf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e18d6e4012 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 9a05e46a12 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> fd162657d1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 323b1f8f17 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a2eb05093d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> fd162657d1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 323b1f8f17 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a2eb05093d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> f63186e675 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> fd162657d1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 323b1f8f17 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> fd162657d1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 323b1f8f17 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 235643d3d2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a3506f6924 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 235643d3d2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 5869a55e95 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1a99b6baaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c9c803fbc4 (d)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0e693a1ea (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5869a55e95 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d78cbc0a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afa165aef3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7802b2e058 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 835f1f8b7d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9be5107268 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 53c3f692fd (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9f003510b7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d78cbc0a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> afa165aef3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7802b2e058 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 667969827b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 835f1f8b7d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 667969827b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 835f1f8b7d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
=======
>>>>>>> 835f1f8b7d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
>>>>>>> 835f1f8b7d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> b71ceab97a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e72d6905dd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5824ba4b77 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
>>>>>>> 835f1f8b7d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 999d7f72e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 83b0de52f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0d78cbc0a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9be5107268 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d78cbc0a5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9be5107268 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d78cbc0a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0d78cbc0a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9be5107268 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d78cbc0a5 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7802b2e058 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 53c3f692fd (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9f003510b7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> afa165aef3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7802b2e058 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 32bd718240 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 12b7785136 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> afa165aef3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7802b2e058 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7802b2e058 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 53c3f692fd (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9f003510b7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afa165aef3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7802b2e058 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
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
=======
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
=======
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6ded1d4370 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 667969827b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0d78cbc0a5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> afa165aef3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 3ea600ce0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7802b2e058 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> acca663a98 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 12b7785136 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
import java.io.InputStream
import java.util.Collections
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)

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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> ec43ea66f9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2a828f5c13 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5dc876437 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8b77cdcb23 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 146c58e019 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 49a2b487df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45249a7eda (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7f3de5ffd1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 97b80a2974 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5f12c5b15a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afb948327d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> b9893d2acd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1d2de0dbf7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d0f0423854 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> c69aaa7823 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8cbc51e901 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 947eaa5c00 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5452e830aa (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3b82d3a71e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ec43ea66f9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b5dc876437 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 2a828f5c13 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0cc0c1c4e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> af069a542a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a53589eff3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> af069a542a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5452e830aa (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 947eaa5c00 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5452e830aa (GEOMESA-2679 Infer encoding of shapefile from cpg file)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 56c80634b9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c5dcff4428 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba4579a08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 673a8f6f9a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7f5a93f6dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 36830b04a1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7432ca9de4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b9d60c0839 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 42951bd0a6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9893d2acd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0cc0c1c4e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> d0f0423854 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> c69aaa7823 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8cbc51e901 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 947eaa5c00 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dffd024e87 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
=======
>>>>>>> 45249a7eda (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 7f3de5ffd1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 97b80a2974 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5f12c5b15a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afb948327d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 3b82d3a71e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> af069a542a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> b9d60c0839 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a53589eff3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> af069a542a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 5452e830aa (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 3b82d3a71e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9fcae8178 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> baee3d5af7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a53589eff3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5452e830aa (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3b82d3a71e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3b82d3a71e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> af069a542a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> b9d60c0839 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a53589eff3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> af069a542a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 5452e830aa (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 3b82d3a71e (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5452e830aa (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> af069a542a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
=======
>>>>>>> 42951bd0a6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9d60c0839 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 947eaa5c00 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5452e830aa (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 45249a7eda (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8cbc51e901 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7f3de5ffd1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 97b80a2974 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 7432ca9de4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 49a2b487df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45249a7eda (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 7f3de5ffd1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 97b80a2974 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8cbc51e901 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 7432ca9de4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 49a2b487df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7f3de5ffd1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 97b80a2974 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8cbc51e901 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6c850ef22 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> c0b7ef9ee3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ef6333d869 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8cbc51e901 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 7432ca9de4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 49a2b487df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8cbc51e901 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 7432ca9de4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 49a2b487df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8cbc51e901 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8cbc51e901 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e3f60478f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b4b8bd326c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0e00e7f5af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 947eaa5c00 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5452e830aa (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3b82d3a71e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> cff5fc6422 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d9af6b6d65 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
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
=======
>>>>>>> ec43ea66f9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2a828f5c13 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 32bd71824 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> bf2c26ed43 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 1de748a93b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
import java.io.InputStream
import java.util.Collections
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 585518a05c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08a86d3e33 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> eafb98376b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 44568bdad8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eb5c49104f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e5009bc661 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 35819c646f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 178908119f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dcfc490cde (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9e1eb00741 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 08a86d3e33 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> eb5c49104f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> eafb98376b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08e65a6d0f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ff20be1be (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> aa820b9337 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7f900d0b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b1641c8158 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0e6f4968b8 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af7beded62 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> b47a69821b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> eab43d23c8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 21910497bf (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0123efdd5d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2da1e9b4d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a1c5e93d45 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 1bc7395204 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52d038f6a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 77d3530abc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> cfe928ceb3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9b0bf07c29 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 86ee6359ba (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa3a7f1ad7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 14a9a5aa60 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 38ac203621 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 1c7407c1b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2bd64d2137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2047cf0dd1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e91a004ef2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f60441bf0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e056e395ff (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 08e65a6d0f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1bc7395204 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5ff20be1be (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 52d038f6a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aa820b9337 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 77d3530abc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 7f900d0b0b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cfe928ceb3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b1641c8158 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9b0bf07c29 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 35819c646f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> af7beded62 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 178908119f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 86ee6359ba (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dcfc490cde (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9e1eb00741 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 8f60441bf0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> b47a69821b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1bc7395204 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9e1eb00741 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> c8108ce7f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b47a69821b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9e1eb00741 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1bc7395204 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9e1eb00741 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 08e65a6d0f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> fa3a7f1ad7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f60441bf0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> b47a69821b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1bc7395204 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9e1eb00741 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b47a69821b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1bc7395204 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9e1eb00741 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 86ee6359ba (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8108ce7f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 86ee6359ba (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8108ce7f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 8f60441bf0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 86ee6359ba (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dcfc490cde (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e056e395ff (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dcfc490cde (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> c8108ce7f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8f60441bf0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 86ee6359ba (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c8108ce7f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 178908119f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> af7beded62 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e5009bc661 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 585518a05c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e91a004ef2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0123efdd5d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 178908119f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> af7beded62 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e5009bc661 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> e91a004ef2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0123efdd5d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> af7beded62 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 178908119f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 96745b4434 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 178908119f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> af7beded62 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e5009bc661 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> e91a004ef2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0123efdd5d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 178908119f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> af7beded62 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e5009bc661 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> e91a004ef2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0123efdd5d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> af7beded62 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 178908119f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1c7407c1b5 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 626b1ac8c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9cd57b64d7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 86ee6359ba (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 178908119f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 86ee6359ba (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dcfc490cde (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> dcfc490cde (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9e1eb00741 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> c8108ce7f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> eb5c49104f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 21910497bf (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 08a86d3e33 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> eb5c49104f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> eafb98376b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e5009bc661 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0123efdd5d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> eafb98376b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> e5009bc661 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> be9ef91d8a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
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
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 490baa117c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 50e677e046 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 302df4295a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3e258539c2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 585518a05c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 7f3de5ffd1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 97b80a2974 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 915bd11f55 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 585518a05c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e35ff95571 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7f3de5ffd1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 97b80a2974 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e35ff95571 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 45249a7eda (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 97b80a2974 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 97b80a2974 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b71ceab97a (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45249a7eda (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1db60d5575 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e35ff95571 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 857dd0a396 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 45249a7eda (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 89bb58cf10 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5f12c5b15a (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 401c676c9c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0e693a1ea3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dffd024e87 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
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
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 81124cda89 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> f63186e675 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 047efc8f96 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 52d038f6a1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
>>>>>>> e4e4766a49 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d64b1bd01f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 235643d3d2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
import java.io.InputStream
import java.util.Collections
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0fd022c963 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7abee1f968 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6db68267b9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7680dddcc9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6998848c4a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ca2f767d2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 305611f128 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 790ed5f51e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 4ebf06cf99 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9be77ec4a5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9514c5eb78 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 44799c5b28 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> abee93f02d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e608a73fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ae0443a91 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> acb0053396 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4820ac9224 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6db68267b9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7680dddcc9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 6998848c4a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ca2f767d2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 305611f128 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 790ed5f51e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4ebf06cf99 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6db68267b9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7680dddcc9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6998848c4a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ca2f767d2a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 305611f128 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 790ed5f51e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e4e4766a4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4ebf06cf99 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9be77ec4a5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e608a73fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ae0443a91 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> acb0053396 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6db68267b9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7680dddcc9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 6998848c4a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ca2f767d2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 305611f128 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9be77ec4a5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 790ed5f51e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 4ebf06cf99 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6db68267b9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6e608a73fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6db68267b9 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 6e608a73fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ae0443a91 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6db68267b9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7680dddcc9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 49471dea7a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ae0443a91 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 6e608a73fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ae0443a91 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6db68267b9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7680dddcc9 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 9ae0443a91 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6998848c4a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0292e04e31 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 83b0de52f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6ded1d4370 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 9ae0443a91 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ded1d4370 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 6998848c4a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> abee93f02d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 6998848c4a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> abee93f02d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6998848c4a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6c850ef227 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 7e0e4fb5c3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
=======
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> acb0053396 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ca2f767d2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4ebf06cf99 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> ca2f767d2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4ebf06cf99 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 626b1ac8c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e3f60478f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 27764e5de8 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 56470bf31b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 653147a99e (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
=======
>>>>>>> 4820ac9224 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3b3b527400 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b59955740a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7abee1f968 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
>>>>>>> 3b1441ba6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7abee1f968 (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7abee1f968 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9a8065c960 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2486842735 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 622d048a77 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 476df18562 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
=======
=======
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 809016b858 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2e3aa14e0f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6ded1d4370 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 7685386a5c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
>>>>>>> e4e4766a4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9be77ec4a5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 945b52f102 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 2b1173bf03 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 34a0684bb3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
>>>>>>> 667969827b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 3ea600ce0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> acca663a98 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 12b7785136 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
import java.io.InputStream
import java.util.Collections
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD

import com.codahale.metrics.Counter
<<<<<<< HEAD
=======
>>>>>>> e0403a237f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 84b83872bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6bfe5baf76 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
=======
>>>>>>> 1d2de0dbf7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 84b83872bb (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f2da1e9b4d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a1c5e93d45 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9fcae8178 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> baee3d5af7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e4688de07e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
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
>>>>>>> e0403a237f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6c850ef22 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> c0b7ef9ee3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ef6333d869 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0cc0c1c4e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84b83872bb (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e3f60478f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b4b8bd326c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0e00e7f5af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6bfe5baf76 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> af069a542a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> cff5fc6422 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d9af6b6d65 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> af069a542a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b5dc876437 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 32bd71824 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> bf2c26ed43 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 1de748a93b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b5dc876437 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD

import com.codahale.metrics.Counter
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 08e65a6d0f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 52d038f6a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 14a9a5aa60 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 52d038f6a1 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 5ff20be1be (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 38ac203621 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 96745b4434 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> aa820b9337 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f900d0b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b1641c8158 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 0e6f4968b8 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> af7beded62 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 86ee6359ba (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> c8108ce7f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 44568bdad8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be9ef91d8a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> eb5c49104f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e5009bc661 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5148ecd4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e3296facc4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 993ffbfa6e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 915bd11f55 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> aede893190 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d32495cd07 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> edc23fb02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 626b1ac8ca (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 6740db1cf8 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5824ba4b77 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b71ceab97a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 3c3507f6d0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e72d6905dd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 89bb58cf10 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 401c676c9c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 585518a05c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81529b2a85 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0fd022c963 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e36fb99905 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d8920ce086 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> 1a99b6baaa (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> f63186e675 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9a05e46a12 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 1a99b6baaa (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 6843ec6035 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> ef47c713ed (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> fd162657d1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ee61d06bf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 72b57b3b08 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e18d6e4012 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 72b57b3b08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 39ae2e950c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0e693a1ea (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> c9c803fbc4 (d)
=======
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 323b1f8f17 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
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
>>>>>>> 9231cf5fb4 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 9ca06a04f5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> b2b5c52793 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 93fe123887 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> fb06c73b9e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> e813d9e207 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 4f5ef14c16 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 13e714c18d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cb80b4663c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0e8b90947e (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> ee6fc69fc3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3478019d27 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> e4c925188f (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> cb2f617cd5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 46e68cd7f4 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
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
>>>>>>> a68b9539c7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f1d62e987f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1d7198eed4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2b0c3c14ec (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dce8b0c7f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a290bbb67c (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> e2455cc6ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 5523b4f08f (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 5d623b8c44 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 0742c4bf9c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 4b6c2e4eca (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> c89c2ff072 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 37f22b424e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 835f1f8b7d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3f1a7bf675 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 835f1f8b7d (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 667969827b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 72491c8adc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9be5107268 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d409de049 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 53c3f692fd (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9be5107268 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 3ea600ce0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9f003510b7 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> acca663a98 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 12b7785136 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 53c3f692fd (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9f003510b7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
import java.io.InputStream
import java.util.Collections
=======
>>>>>>> d31acf1161 (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5af7c15be6 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> d31acf1161 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 56c80634b9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c5dcff4428 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6c850ef22 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> c0b7ef9ee3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ef6333d869 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 8ba4579a08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 673a8f6f9a (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 7f5a93f6dc (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 7432ca9de4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e3f60478f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b4b8bd326c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0e00e7f5af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 36830b04a1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 42951bd0a6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7432ca9de4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9d60c0839 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 42951bd0a6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> a53589eff3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> cff5fc6422 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d9af6b6d65 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b9d60c0839 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8b77cdcb23 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a53589eff3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 146c58e019 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8b77cdcb23 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 49a2b487df (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 32bd71824 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> bf2c26ed43 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 1de748a93b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 146c58e019 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 49a2b487df (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
=======
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
>>>>>>> bf3b8e427c (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1cbf436890 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> bf3b8e427c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> fa3a7f1ad7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 14a9a5aa60 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> 1c7407c1b5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 96745b4434 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 38ac203621 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c7407c1b5 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 2bd64d2137 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 9cd57b64d7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 2047cf0dd1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e91a004ef2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e056e395ff (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 8f60441bf0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b47a69821b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eab43d23c8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> be9ef91d8a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 21910497bf (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0123efdd5d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
>>>>>>> 31860203c1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
=======
>>>>>>> 2537391cc6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9677081a1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 490baa117c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 31860203c1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 50e677e046 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2537391cc6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d51efa18f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3e258539c2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> c0205a0050 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
>>>>>>> 91dbd747cb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> fc00100ff0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1db60d5575 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9ebea6992b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 857dd0a396 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 214fae6c43 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e35ff95571 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7f3de5ffd1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45249a7eda (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 97b80a2974 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa38 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 05e9052b26 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 7f3de5ffd1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 97b80a2974 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5f12c5b15a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> afb948327d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9f4dc60aaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
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
>>>>>>> 11089e31dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 36ff9b69f0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 727d0966f1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81124cda89 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> a2eb05093d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 6b2ee508d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b63a4a7b59 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6eb7d73613 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e4e4766a49 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d64b1bd01f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b63a4a7b59 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 399cb4eca6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2f4fd68a3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 999d7f72ee (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 30b2505101 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> de8896ccfc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ad3d211d1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a3506f6924 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3ea600ce08 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 235643d3d2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 5869a55e95 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 946abd08d0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
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
>>>>>>> 6e25e2df5b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> aa3d3985c9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 96b7b6d335 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1b306b5c5a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 3d29bbaeac (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aa3d3985c9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 6e25e2df5b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> aa3d3985c9 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8f97df2ea (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 4d5be61f70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 83b0de52f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 49471dea7a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 108bb7bb94 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0292e04e31 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e9c2969403 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 83b0de52f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6c850ef227 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 7e0e4fb5c3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 57af30a800 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
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
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d43ceee7c5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4101c28680 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e39c04d2ed (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9e6360a1ef (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> e3f60478f6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 27764e5de8 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0f98b70db2 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 53a0ac2d93 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> ebf01d640d (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> dc3e8bf290 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9514c5eb78 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f27d878dd6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 44799c5b28 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3b3b527400 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9514c5eb78 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 32bd718240 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> a7118a678b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 44799c5b28 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b59955740a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> abee93f02d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======

import com.codahale.metrics.Counter
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a6710b33bf (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 6e608a73fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> acb0053396 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2486842735 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9ae0443a91 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4820ac9224 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 622d048a77 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> acb0053396 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 476df18562 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4820ac9224 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7abee1f968 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9f784746bb (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 7abee1f968 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7680dddcc9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 809016b858 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6db68267b9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6998848c4a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2e3aa14e0f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7680dddcc9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> f63186e67 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> ca2f767d2a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2e274942b0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6ded1d4370 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 7685386a5c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6998848c4a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> ca2f767d2a (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 305611f128 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 4ebf06cf99 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e4e4766a4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9be77ec4a5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 945b52f102 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 2b1173bf03 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 790ed5f51e (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 34a0684bb3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4ebf06cf99 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 09c7932a85 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 667969827b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ebe98296ac (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 3c7455c057 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e4c3f19de7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d78cbc0a5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8a5ee02602 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afa165aef3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d78cbc0a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 3ea600ce0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7802b2e058 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> acca663a98 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 12b7785136 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 32415f6b4d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> afa165aef3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7802b2e058 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
=======
import java.io.InputStream
import java.util.Collections
=======
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
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
>>>>>>> 115257ee37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> b9893d2acd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9fcae8178 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> baee3d5af7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
=======
>>>>>>> 1d2de0dbf7 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e4688de07e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b9893d2acd (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> d0f0423854 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e0403a237f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1d2de0dbf7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
=======
>>>>>>> c69aaa7823 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6c850ef22 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> c0b7ef9ee3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ef6333d869 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0cc0c1c4e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d0f0423854 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
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
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2da1e9b4d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c69aaa7823 (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> a1c5e93d45 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84b83872bb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f2da1e9b4d (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
>>>>>>> 8cbc51e901 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e3f60478f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b4b8bd326c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0e00e7f5af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6bfe5baf76 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> a1c5e93d45 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 947eaa5c00 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cbc51e901 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5452e830aa (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 947eaa5c00 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 3b82d3a71e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 83b0de52f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> cff5fc6422 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> d9af6b6d65 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> af069a542a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 5452e830aa (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ec43ea66f9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3b82d3a71e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2a828f5c13 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ec43ea66f9 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 0e693a1ea (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 32bd71824 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> bf2c26ed43 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 1de748a93b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b5dc876437 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 2a828f5c13 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5c45c012a4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> dc03ef5832 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 1bc7395204 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 77d3530abc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ff20be1be (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 52d038f6a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 96745b4434 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> aa820b9337 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 77d3530abc (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f900d0b0b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cfe928ceb3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 35819c646f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b1641c8158 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9b0bf07c29 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 9cd57b64d7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0e6f4968b8 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 35819c646f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> af7beded62 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 178908119f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dcfc490cde (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> c8108ce7f7 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 0850f4f171 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 8d02c596fb (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1d9a0a9e20 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9e1eb00741 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08a86d3e33 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> be9ef91d8a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> eb5c49104f (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> eafb98376b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> e9e87d87e6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
=======
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
>>>>>>> b2a599de5b (GEOMESA-2679 Infer encoding of shapefile from cpg file)

import com.codahale.metrics.Counter
<<<<<<< HEAD
=======
>>>>>>> e4172eb053 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81f137be74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 915e019d19 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5295ee5d82 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2cb1e7b9c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bfd6e79fa4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 8cdf811be4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1c242b79a1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4b0ab66d74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 091d323f5d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> b2a599de5b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 4ba06e9530 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 12b63c14c2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2235f120d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e4172eb053 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 12b63c14c2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 77e09b8aaa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 81f137be74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> db69c99cee (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4ba06e9530 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 40870a68db (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 915e019d19 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> daa600442f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5295ee5d82 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f1baaf0e1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f2cb1e7b9c (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> a200fd4b19 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> bfd6e79fa4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 41b057b6f8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cdf811be4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> db69c99cee (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1c242b79a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 4ba06e9530 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 6e25e2df5b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 12b63c14c2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aa3d3985c9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a5946358db (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 96b7b6d335 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 271caa4e84 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 1b306b5c5a (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7bbce86d17 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3d29bbaeac (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
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
>>>>>>> 537a54b7ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 091d323f5d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 4ba06e9530 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 12b63c14c2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2235f120d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 12b63c14c2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 77e09b8aaa (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> db69c99cee (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4ba06e9530 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 40870a68db (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
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
>>>>>>> daa600442f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import com.typesafe.scalalogging.LazyLogging
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f1baaf0e1 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> a200fd4b19 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 41b057b6f8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> db69c99cee (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 91dbd747c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> ddc2cffa3 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 4ba06e9530 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 12b63c14c2 (GEOMESA-3071 Move all converter state into evaluation context)
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.referencing.CRS
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.AbstractConverter
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicField, BasicOptions}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326
import org.locationtech.geomesa.utils.io.{CloseWithLogging, PathUtils}
import org.locationtech.geomesa.utils.text.TextTools
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b2a599de5b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 4d5be61f70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9ca06a04f5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d31acf1161 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> bf3b8e427c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 31860203c1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 4d5be61f70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
      val source = io.Source.fromFile(cpgPath.toFile)
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4d5be61f70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
      val source = io.Source.fromFile(cpgPath.toFile)
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> bf3b8e427c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
      val source = io.Source.fromFile(cpgPath.toFile)
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
      val source = io.Source.fromFile(cpgPath.toFile)
<<<<<<< HEAD
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> e3296facc4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0fd022c963 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9ca06a04f5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d31acf1161 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> bf3b8e427c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
      val source = io.Source.fromFile(cpgPath.toFile)
<<<<<<< HEAD
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 490baa117c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
>>>>>>> 31860203c1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 36ff9b69f0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 4d5be61f70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> locationtech-main
=======
=======
      val source = io.Source.fromFile(cpgPath.toFile)
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 091d323f5d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> b2a599de5b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
      val source = io.Source.fromFile(cpgPath.toFile)
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 091d323f5d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> b2a599de5b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9ca06a04f5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d31acf1161 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> bf3b8e427c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 31860203c1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4d5be61f70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 490baa117c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> e3296facc4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d31acf1161 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> bf3b8e427c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 31860203c1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4d5be61f70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 091d323f5d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> b2a599de5b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 091d323f5d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 4d5be61f70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> b2a599de5b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
=======
>>>>>>> 0fd022c963 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 9ca06a04f5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> d31acf1161 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> bf3b8e427c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 31860203c1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 36ff9b69f0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 4d5be61f70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 3b1441ba6d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 36ff9b69f0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4d5be61f70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 401e627535 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 490baa117c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0736678d1d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 7eba086e17 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 6033c1b7af (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
<<<<<<< HEAD
=======
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> e3296facc4 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 0fd022c963 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9fcae81780 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9ca06a04f5 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 9a64765dd6 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> d31acf1161 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> bf3b8e427c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 20b0d52e9d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 490baa117c (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 31860203c1 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 36ff9b69f0 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4d5be61f70 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5350b4ba78 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
=======
>>>>>>> f46b17eca9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 897d36ccff (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 091d323f5d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> b2a599de5b (GEOMESA-2679 Infer encoding of shapefile from cpg file)
=======
>>>>>>> 20b0d52e9 (GEOMESA-2679 Infer encoding of shapefile from cpg file)
>>>>>>> 091d323f5d (GEOMESA-2679 Infer encoding of shapefile from cpg file)
    }
  }
}
