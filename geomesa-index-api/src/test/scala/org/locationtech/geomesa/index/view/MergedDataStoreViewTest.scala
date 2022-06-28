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
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> df9a4d047d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7a7c301714 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 032b5aa797 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> d12efed52d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 8ba5e370d4 (GEOMESA-3202 Check for disjoint date queries in merged view store)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

<<<<<<< HEAD
import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
=======
import org.geotools.data.simple.SimpleFeatureReader
import org.geotools.data.{DataStore, FeatureReader, Query, Transaction}
>>>>>>> ed0b243ea9f (GEOMESA-3202 Check for disjoint date queries in merged view store)
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
import org.geotools.data.{DataStore, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.FilterHelper
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.ArgumentMatchers
<<<<<<< HEAD
=======
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
import org.specs2.matcher.Matchers
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> ed0b243ea9f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> d67c4751dd2 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 33511dd1c3b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 29b01d02901 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9f498804883 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> a6ae93cb5f3 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 24d8c84c5aa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> a6ae93cb5f3 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 709d36fab5e (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 10be5d2340b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6a4ff24d14c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> d0668176da7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ae9ccc22c0e (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> aaac9bf1b27 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d43590761fe (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 7a377ef3d0f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ab46ac2ae83 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 32a3a53f7b2 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d5d6633f7a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 6977303d471 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b2c5cb17bee (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 3e6c36184cd (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f76251a7560 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 118e973349c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 862aae30a3b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 8dd024f6791 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 775ed2dd6f1 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 6e959c6dbc1 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 190b2701741 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 64a645069b5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import org.specs2.matcher.Matchers
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 26275bc316a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 396843bbf39 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 29b01d02901 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> a6ae93cb5f3 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 709d36fab5e (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> d0668176da7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> ae9ccc22c0e (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 7a377ef3d0f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> ab46ac2ae83 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6977303d471 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> b2c5cb17bee (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 118e973349c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 862aae30a3b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6e959c6dbc1 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 190b2701741 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import org.specs2.matcher.Matchers
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> bd279a782eb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 34b79b7f66d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 9f498804883 (Merge branch 'feature/postgis-fixes')
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class MergedDataStoreViewTest extends Specification with Mockito {

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
@RunWith(classOf[JUnitRunner])
class MergedDataStoreViewTest extends Specification with Mockito {

  import scala.collection.JavaConverters._

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
  import org.locationtech.geomesa.filter.andFilters

  val sft = SimpleFeatureTypes.createImmutableType("test",
    "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.index.dtg=dtg")

  def emptyReader(): SimpleFeatureReader = new SimpleFeatureReader() {
    override def getFeatureType: SimpleFeatureType = sft
    override def next(): SimpleFeature = Iterator.empty.next
    override def hasNext: Boolean = false
    override def close(): Unit = {}
  }

  def stores(): Seq[(DataStore, Option[Filter])] = Seq.tabulate(3) { i =>
    val store = mock[DataStore]
    val filter = i match {
      case 0 => ECQL.toFilter("dtg < '2022-02-02T00:00:00.000Z'")
      case 1 => ECQL.toFilter("dtg >= '2022-02-02T00:00:00.000Z' AND dtg < '2022-02-03T00:00:00.000Z'")
      case 2 => ECQL.toFilter("dtg >= '2022-02-03T00:00:00.000Z'")
    }
    store.getSchema(sft.getTypeName) returns sft
    store.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns emptyReader()
    store -> Some(filter)
  }

  "MergedDataStoreView" should {
    "pass through INCLUDE filters" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores) { case (store, Some(filter)) =>
        val query = new Query(sft.getTypeName, filter)
        there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
      }
    }

    "pass through queries that don't conflict with the default filter" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')

      val noDates = Seq("IN ('1', '2')", "foo = 'bar'", "age = 21", "bbox(geom,120,45,130,55)")
      foreach(noDates.map(ECQL.toFilter)) { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
        foreach(stores) { case (store, Some(filter)) =>
          val query = new Query(sft.getTypeName, andFilters(Seq(filter, f)))
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
      }
    }

    "filter out queries from stores that aren't applicable - before" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')

      val before = Seq("dtg during 2022-02-01T00:00:00.000Z/2022-02-01T12:00:00.000Z and name = 'alice'")
      foreach(before.map(ECQL.toFilter)) { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
        foreach(stores.take(1)) { case (store, Some(filter)) =>
          val query = new Query(sft.getTypeName, andFilters(Seq(f, filter)))
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
        foreach(stores.drop(1)) { case (store, _) =>
          val query = new Query(sft.getTypeName, Filter.EXCLUDE)
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
      }
    }

    "filter out queries from stores that aren't applicable - after" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')

      val after = Seq("dtg during 2022-02-04T00:00:00.000Z/2022-02-04T12:00:00.000Z and name = 'alice'")
      foreach(after.map(ECQL.toFilter)) { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
        foreach(stores.take(2)) { case (store, _) =>
          val query = new Query(sft.getTypeName, Filter.EXCLUDE)
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
        foreach(stores.drop(2)) { case (store, Some(filter)) =>
          val query = new Query(sft.getTypeName, andFilters(Seq(f, filter)))
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
      }
    }

    "filter out queries from stores that aren't applicable - overlapping" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = false)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')

      val after = Seq("dtg during 2022-02-01T00:00:00.000Z/2022-02-04T12:00:00.000Z and name = 'alice'")
      foreach(after.map(ECQL.toFilter)) { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
        foreach(stores) { case (store, Some(filter)) =>
          val query = new Query(sft.getTypeName, andFilters(Seq(f, filter)))
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
      }
    }

    "close iterators with parallel scans" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = true)

      val readers = ArrayBuffer.empty[CloseableFeatureReader]
      stores.foreach { case (store, _) =>
        store.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns {
          val reader = new CloseableFeatureReader()
          readers += reader
          reader
        }
      }

      WithClose(view.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))(_.hasNext)
      readers must haveLength(stores.length)
      foreach(readers)(_.closed must beTrue)
    }

    "close iterators with parallel push-down scans" in {
      val stores = this.stores()
      val view = new MergedDataStoreView(stores, deduplicate = false, parallel = true)

      val readers = ArrayBuffer.empty[CloseableFeatureReader]
      stores.foreach { case (store, _) =>
        store.getFeatureReader(ArgumentMatchers.any(), ArgumentMatchers.any()) returns {
          val reader = new CloseableFeatureReader(BinaryOutputEncoder.BinEncodedSft)
          readers += reader
          reader
        }
      }

      val query = new Query(sft.getTypeName, Filter.INCLUDE)
      query.getHints.put(QueryHints.BIN_GEOM, "geom")
      query.getHints.put(QueryHints.BIN_DTG, "dtg")
      query.getHints.put(QueryHints.BIN_TRACK, "name")
      WithClose(view.getFeatureReader(query, Transaction.AUTO_COMMIT))(_.hasNext)
      readers must haveLength(stores.length)
      foreach(readers)(_.closed must beTrue)
    }
  }

  class CloseableFeatureReader(val getFeatureType: SimpleFeatureType = sft)
      extends FeatureReader[SimpleFeatureType, SimpleFeature] {
    var closed: Boolean = false
    override def next(): SimpleFeature = null
    override def hasNext: Boolean = false
    override def close(): Unit = closed = true
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
  }
}
