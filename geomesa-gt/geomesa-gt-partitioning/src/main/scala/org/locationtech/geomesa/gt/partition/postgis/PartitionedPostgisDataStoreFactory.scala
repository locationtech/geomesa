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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> df43e72c50 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
=======
=======
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
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
=======
=======
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
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
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
<<<<<<< HEAD
=======

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
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
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
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))

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
=======
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> df43e72c50 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
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
=======
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 7481483700 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> dcd872c1ac (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======

import java.sql.{Connection, DatabaseMetaData}
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements)
=======
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
    Seq(DbType, IdleInTransactionTimeout)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
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
  override protected def createDataStoreInternal(store: JDBCDataStore, baseParams: java.util.Map[String, _]): JDBCDataStore = {
    val params = new java.util.HashMap[String, Any](baseParams)
    // default to using prepared statements, if not specified
    if (!params.containsKey(PreparedStatements.key)) {
      params.put(PreparedStatements.key, java.lang.Boolean.TRUE)
    }
    // set default schema, if not specified - postgis store doesn't actually use its own default
    if (!params.containsKey(PostgisNGDataStoreFactory.SCHEMA.key)) {
      // need to set it in the store, as the key has already been processed
      store.setDatabaseSchema("public")
      // also set in the params for consistency, although it's not used anywhere
      params.put(PostgisNGDataStoreFactory.SCHEMA.key, "public")
    }
=======
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
    // override postgis dbkey
    parameters.put(DbType.key, DbType)
  }

>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
  override protected def createDataStoreInternal(store: JDBCDataStore, params: java.util.Map[String, _]): JDBCDataStore = {

>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> a207fd51c3 (GEOMESA-3215 Postgis - support List-type attributes)
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
=======
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> f5b2148366 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> df43e72c50 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 406de071e1 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
<<<<<<< HEAD
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
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
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 713060f3a2 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> da609e20da (GEOMESA-3215 Postgis - support List-type attributes)
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
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
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
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
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> f31d66ef5f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> f201f9ac85 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d4a1e3591e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 64b3066584 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> be0acddd66 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3a9e2ecffe (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> b8426b262b (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> c4c6c99862 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> a26c0acbb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> c1eb5f889f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> be6b05230c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 022be00fcb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> f31d66ef5f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> d1cc27725e (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
=======
>>>>>>> e5b494b7d3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> be0acddd66 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> c1eb5f889f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> f201f9ac85 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 022be00fcb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3a9e2ecffe (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> c4c6c99862 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> f31d66ef5f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
<<<<<<< HEAD
>>>>>>> 69cf24f7a0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> ae3269b03f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 3a9e2ecffe (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
<<<<<<< HEAD
>>>>>>> ae3269b03f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> c4c6c99862 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 5e000da485 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
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
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6bd2f89dcd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ad8bba7bb7 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3a9e2ecffe (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f31d66ef5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4983e191e6 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 9fb20ad56c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30ec49c9c0 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 5e000da48 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e694961e05 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> d30ebb4092 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
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
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> d1cc27725 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
        })
<<<<<<< HEAD
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 713060f3a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> 50a84fd0eb (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
>>>>>>> ddf486214c (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e219fae656 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
        })
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 09404b0462 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
>>>>>>> f9fc403058 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 6a394f9c72 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
=======
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> f31d66ef5f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e5b494b7d3 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> 56e5bdc2a5 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
=======
=======
        })
>>>>>>> dcd872c1a (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> 91b820951 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
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
=======
        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))
>>>>>>> c79be4f83 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
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
=======
=======
>>>>>>> f9fc40305 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> e219fae656 (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
>>>>>>> c1eb5f889f (GEOMESA-3212 Postgis - convert constant functions to literals for SQL translation (#2875))
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

      case d => throw new IllegalArgumentException(s"Expected PostGISDialect but got: ${d.getClass.getName}")
    }

    ds
  }

  // these will get replaced in createDataStoreInternal, above
  override protected def createSQLDialect(dataStore: JDBCDataStore): SQLDialect = new PostGISDialect(dataStore)
  override protected def createSQLDialect(dataStore: JDBCDataStore, params: java.util.Map[String, _]): SQLDialect =
    new PostGISDialect(dataStore)
}
