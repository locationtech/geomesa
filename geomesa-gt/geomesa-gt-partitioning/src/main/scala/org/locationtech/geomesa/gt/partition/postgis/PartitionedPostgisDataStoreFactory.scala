/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp.BasicDataSource
import org.geotools.data.postgis.{PostGISDialect, PostGISPSDialect, PostgisNGDataStoreFactory}
import org.geotools.jdbc.{JDBCDataStore, SQLDialect}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> location-main
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locationtech-main
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> location-main
=======
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> location-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locationtech-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> location-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> df43e72c50 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> d7efff9563 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a980818468 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 7481483700 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locationtech-main
=======
>>>>>>> d7efff9563 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> a980818468 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d7efff9563 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a980818468 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect
import org.opengis.feature.simple.SimpleFeatureType
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
=======
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
>>>>>>> locatelli-main

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
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
>>>>>>> locationtech-main
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locationtech-main
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
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
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> df43e72c50 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 7481483700 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locationtech-main
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> df43e72c50 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d7efff9563 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> a980818468 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d7efff9563 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> a980818468 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
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

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

class PartitionedPostgisDataStoreFactory extends PostgisNGDataStoreFactory with LazyLogging {
=======
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)

  import PartitionedPostgisDataStoreParams.{DbType, IdleInTransactionTimeout, PreparedStatements}

  override def getDisplayName: String = "PostGIS (partitioned)"

  override def getDescription: String = "PostGIS Database with time-partitioned tables"

  override protected def getDatabaseID: String = DbType.sample.asInstanceOf[String]

  override protected def setupParameters(parameters: java.util.Map[String, AnyRef]): Unit = {
    super.setupParameters(parameters)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    Seq(DbType, IdleInTransactionTimeout)
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> 008807b427 (GEOMESA-3295 Partitioned PostGIS - default to using prepared statements (#2993))
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> locatelli-main
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> locatelli-main
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> locatelli-main
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> locatelli-main
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> locatelli-main
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> locatelli-main
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> locatelli-main
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> locatelli-main
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
    Seq(DbType, IdleInTransactionTimeout)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
    Seq(DbType, IdleInTransactionTimeout)
=======
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
>>>>>>> 008807b427 (GEOMESA-3295 Partitioned PostGIS - default to using prepared statements (#2993))
>>>>>>> d5f3f284a6 (GEOMESA-3295 Partitioned PostGIS - default to using prepared statements (#2993))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        .foreach(p => parameters.put(p.key, p))
  }

  override def createDataSource(params: java.util.Map[String, _]): BasicDataSource = {
    val source = super.createDataSource(params)
    val options =
      Seq(IdleInTransactionTimeout)
          .flatMap(p => p.opt(params).map(t => s"-c ${p.key}=${t.millis}"))
=======
    // override postgis dbkey
    parameters.put(DbType.key, DbType)
  }

  override protected def createDataStoreInternal(store: JDBCDataStore, params: java.util.Map[String, _]): JDBCDataStore = {
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

    logger.debug(s"Connection options: ${options.mkString(" ")}")

    if (options.nonEmpty) {
      source.addConnectionProperty("options", options.mkString(" "))
    }
    source
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
=======
    // override postgis dbkey
    parameters.put(DbType.key, DbType)
  }

>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
  override protected def createDataStoreInternal(store: JDBCDataStore, params: java.util.Map[String, _]): JDBCDataStore = {

=======
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
  override protected def createDataStoreInternal(store: JDBCDataStore, baseParams: java.util.Map[String, _]): JDBCDataStore = {
    val params = new java.util.HashMap[String, Any](baseParams)
    // default to using prepared statements, if not specified
    if (!params.containsKey(PreparedStatements.key)) {
      params.put(PreparedStatements.key, java.lang.Boolean.TRUE)
    }
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 008807b427 (GEOMESA-3295 Partitioned PostGIS - default to using prepared statements (#2993))
=======
=======
>>>>>>> locationtech-main
    // set default schema, if not specified - postgis store doesn't actually use its own default
    if (!params.containsKey(PostgisNGDataStoreFactory.SCHEMA.key)) {
      // need to set it in the store, as the key has already been processed
      store.setDatabaseSchema("public")
      // also set in the params for consistency, although it's not used anywhere
      params.put(PostgisNGDataStoreFactory.SCHEMA.key, "public")
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
>>>>>>> ed6355ceb8 (GEOMESA-3294 Partitioned PostGIS - use default database schema if not specified)
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d5f3f284a6 (GEOMESA-3295 Partitioned PostGIS - default to using prepared statements (#2993))
=======
    // override postgis dbkey
    parameters.put(DbType.key, DbType)
  }

>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
  override protected def createDataStoreInternal(store: JDBCDataStore, params: java.util.Map[String, _]): JDBCDataStore = {

<<<<<<< HEAD
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
  override protected def createDataStoreInternal(store: JDBCDataStore, baseParams: java.util.Map[String, _]): JDBCDataStore = {
    val params = new java.util.HashMap[String, Any](baseParams)
    // default to using prepared statements, if not specified
    if (!params.containsKey(PreparedStatements.key)) {
      params.put(PreparedStatements.key, java.lang.Boolean.TRUE)
    }
<<<<<<< HEAD
>>>>>>> 008807b427 (GEOMESA-3295 Partitioned PostGIS - default to using prepared statements (#2993))
<<<<<<< HEAD
>>>>>>> d5f3f284a6 (GEOMESA-3295 Partitioned PostGIS - default to using prepared statements (#2993))
=======
=======
    // set default schema, if not specified - postgis store doesn't actually use its own default
    if (!params.containsKey(PostgisNGDataStoreFactory.SCHEMA.key)) {
      // need to set it in the store, as the key has already been processed
      store.setDatabaseSchema("public")
      // also set in the params for consistency, although it's not used anywhere
      params.put(PostgisNGDataStoreFactory.SCHEMA.key, "public")
    }
>>>>>>> ed6355ceb8 (GEOMESA-3294 Partitioned PostGIS - use default database schema if not specified)
>>>>>>> 42fc564606 (GEOMESA-3294 Partitioned PostGIS - use default database schema if not specified)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
    val ds = super.createDataStoreInternal(store, params)
    val dialect = new PartitionedPostgisDialect(ds)

    ds.getSQLDialect match {
      case d: PostGISDialect =>
        dialect.setEncodeBBOXFilterAsEnvelope(d.isEncodeBBOXFilterAsEnvelope)
        dialect.setEstimatedExtentsEnabled(d.isEstimatedExtentsEnabled)
        dialect.setFunctionEncodingEnabled(d.isFunctionEncodingEnabled)
        dialect.setLooseBBOXEnabled(d.isLooseBBOXEnabled)
        dialect.setSimplifyEnabled(d.isSimplifyEnabled)
        ds.setSQLDialect(dialect)

      case d: PostGISPSDialect =>
        dialect.setEncodeBBOXFilterAsEnvelope(d.isEncodeBBOXFilterAsEnvelope)
        dialect.setFunctionEncodingEnabled(d.isFunctionEncodingEnabled)
        dialect.setLooseBBOXEnabled(d.isLooseBBOXEnabled)

        // these configs aren't exposed through the PS dialect so re-calculate them from the params
        val est = PostgisNGDataStoreFactory.ESTIMATED_EXTENTS.lookUp(params)
        dialect.setEstimatedExtentsEnabled(est == null || est == java.lang.Boolean.TRUE)
        val simplify = PostgisNGDataStoreFactory.SIMPLIFY.lookUp(params)
        dialect.setSimplifyEnabled(simplify == null || simplify == java.lang.Boolean.TRUE)

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> locationtech-main
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
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
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locationtech-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> locatelli-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> locatelli-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> locatelli-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> locatelli-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> locatelli-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> locatelli-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> locatelli-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> locatelli-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> locatelli-main
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locationtech-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> location-main
=======
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> location-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locationtech-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> df43e72c50 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 7481483700 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> d7efff9563 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> a980818468 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> d7efff9563 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> a980818468 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
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
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        ds.setSQLDialect(new PostGISPSDialect(ds, dialect) {
          // fix bug with PostGISPSDialect dialect not delegating these methods
          override def getDefaultVarcharSize: Int = dialect.getDefaultVarcharSize
          override def encodeTableName(raw: String, sql: StringBuffer): Unit = dialect.encodeTableName(raw, sql)
          override def postCreateFeatureType(
              featureType: SimpleFeatureType,
              metadata: DatabaseMetaData,
              schemaName: String,
              cx: Connection): Unit = {
            dialect.postCreateFeatureType(featureType, metadata, schemaName, cx)
          }
          override def splitFilter(filter: Filter, schema: SimpleFeatureType): Array[Filter] =
            dialect.splitFilter(filter, schema)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 785315f693 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f31d66ef5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d1cc27725 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locationtech-main
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
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f31d66ef5f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> f201f9ac85 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 110ba3e6bc (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> be0acddd66 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3a9e2ecffe (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> c4c6c99862 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> c1eb5f889f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> c1eb5f889f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 022be00fcb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 8d2a2ddbee (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 0ebf5c5b54 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4744cb0bc3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bded5d63b7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 110ba3e6bc (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f31d66ef5f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d1cc27725e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> ae3269b03f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> f31d66ef5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 69cf24f7a0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d1cc27725 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e219fae656 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> ae3269b03f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> f31d66ef5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 69cf24f7a0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d1cc27725 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e219fae656 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 09404b0462 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> f31d66ef5f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e5b494b7d3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> be0acddd66 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> ae3269b03f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> f31d66ef5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 69cf24f7a0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d1cc27725 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e219fae656 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> ae3269b03f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> f31d66ef5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 69cf24f7a0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d1cc27725 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e219fae656 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 09404b0462 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> f31d66ef5f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e5b494b7d3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> be0acddd66 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 022be00fcb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
          override def getDesiredTablesType: Array[String] = dialect.getDesiredTablesType
=======
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 110ba3e6bc (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> e5b494b7d3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> be0acddd66 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c1eb5f889f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> c1eb5f889f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f201f9ac85 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 022be00fcb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4744cb0bc3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bded5d63b7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> e5b494b7d3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 110ba3e6bc (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> c1eb5f889f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 3a9e2ecffe (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> c4c6c99862 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 8d2a2ddbee (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 0ebf5c5b54 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 3a9e2ecffe (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f31d66ef5f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
>>>>>>> 69cf24f7a0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ae3269b03f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> ae3269b03f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3a9e2ecffe (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
>>>>>>> ae3269b03f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> c4c6c99862 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 8d2a2ddbee (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
<<<<<<< HEAD
>>>>>>> ae3269b03f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 0ebf5c5b54 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> location-main
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d1cc27725e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3a9e2ecffe (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
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
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
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
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 8d2a2ddbee (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9124a4cb37 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f31d66ef5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> location-main
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> location-main
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 0a5b74c946 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> d1cc27725 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> location-main
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locationtech-main
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> locatelli-main
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
=======
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> locatelli-main
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e219fae656 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
=======
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> locatelli-main
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> location-main
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> location-main
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 09404b0462 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
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
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
        })
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> df43e72c50 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> f31d66ef5f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e5b494b7d3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> locatelli-main
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d1cc27725e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
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
<<<<<<< HEAD
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> 086ddfbd4a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
          override def getDesiredTablesType: Array[String] = dialect.getDesiredTablesType
=======
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 086ddfbd4a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
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
>>>>>>> c4c6c99862 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
>>>>>>> a980818468 (GEOMESA-3215 Postgis - support List-type attributes)
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
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> locatelli-main
=======
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e219fae656 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> c1eb5f889f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        })
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
        })
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 7481483700 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> f31d66ef5f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> f201f9ac85 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d1cc27725e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> e7dfbc3fc1 (GEOMESA-3215 Postgis - support List-type attributes)
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
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> 0ebf5c5b54 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> e4a3f1a534 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6494375eef (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> bf6bde0830 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e219fae656 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 4744cb0bc3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
=======
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 83c2c09260 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6de7ca735b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 354b5e5ae5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 09404b0462 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> df43e72c50 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 3ab56cb4cf (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> ec585da266 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
>>>>>>> d581fa3e8f (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 97f94a38ca (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> edde3a188e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6768ebe0c2 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
>>>>>>> 264c9fa240 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d1cc27725e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> eb0302a7c5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 34836e5f4e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 217b7d1cb9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 8ed0ae6564 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 38e39baec8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
>>>>>>> b6e56a9e80 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 01c8505b38 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3ce7fe64a3 (GEOMESA-3215 Postgis - support List-type attributes)
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
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> 333d924309 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d391bc18f0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 67ba50b23a (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> a30a0c1a2a (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1083e19ae3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 30a68d04cb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cdf5a8b797 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cc93e0930d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> locationtech-main
=======
=======
>>>>>>> 086ddfbd4a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d7efff9563 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 48fcd67085 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> a980818468 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 29836dbeb7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
>>>>>>> 79bf541ac8 (Merge branch 'feature/postgis-fixes')
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
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
        })
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d7efff9563 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a980818468 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 64eaf6b132 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
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
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

      case d => throw new IllegalArgumentException(s"Expected PostGISDialect but got: ${d.getClass.getName}")
    }

    ds
  }

  // these will get replaced in createDataStoreInternal, above
  override protected def createSQLDialect(dataStore: JDBCDataStore): SQLDialect = new PostGISDialect(dataStore)
  override protected def createSQLDialect(dataStore: JDBCDataStore, params: java.util.Map[String, _]): SQLDialect =
    new PostGISDialect(dataStore)
}
