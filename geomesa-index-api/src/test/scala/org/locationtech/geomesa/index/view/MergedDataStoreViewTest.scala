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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> df9a4d047d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 31af7c2e9c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0c2d9c0d75 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 018041199d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d12efed52d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 018041199d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 8ba5e370d4 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 31af7c2e9c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 0c2d9c0d75 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 018041199d (GEOMESA-3202 Check for disjoint date queries in merged view store)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.data.simple.SimpleFeatureReader
<<<<<<< HEAD
import org.geotools.data.{DataStore, FeatureReader, Query, Transaction}
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.mockito.ArgumentMatchers
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
import org.specs2.matcher.Matchers
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
import org.specs2.matcher.Matchers
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
import org.specs2.matcher.Matchers
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
import org.specs2.matcher.Matchers
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
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b91cc883ba (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
      WithClose(view.getFeatureReader(new Query(sft.getTypeName, Filter.INCLUDE), Transaction.AUTO_COMMIT))(_.hasNext)
      foreach(stores) { case (store, Some(filter)) =>
        val query = new Query(sft.getTypeName, filter)
        there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
      }
    }

    "pass through queries that don't conflict with the default filter" in {
      val stores = this.stores()
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)

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
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)

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
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)

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
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
      val view = new MergedDataStoreView(stores, deduplicate = false)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)

      val after = Seq("dtg during 2022-02-01T00:00:00.000Z/2022-02-04T12:00:00.000Z and name = 'alice'")
      foreach(after.map(ECQL.toFilter)) { f =>
        WithClose(view.getFeatureReader(new Query(sft.getTypeName, f), Transaction.AUTO_COMMIT))(_.hasNext)
        foreach(stores) { case (store, Some(filter)) =>
          val query = new Query(sft.getTypeName, andFilters(Seq(f, filter)))
          there was one(store).getFeatureReader(query, Transaction.AUTO_COMMIT)
        }
      }
    }
<<<<<<< HEAD

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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
<<<<<<< HEAD
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
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 7933021402 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e22e621f59 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a83f02ec95 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1271d9cc25 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> eea6a40faa (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e82ce16cce (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9a8d532101 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f2b3dcc64f (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> f02f76e4ee (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9ef7e87fec (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> ffe6d857f8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
  }
}
