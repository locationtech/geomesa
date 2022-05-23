/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

<<<<<<< HEAD
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature.`type`.{GeometryDescriptor, Name}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.data._
<<<<<<< HEAD
=======
import org.geotools.data.simple.SimpleFeatureWriter
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> ed0b243ea9f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 24d8c84c5aa (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.geotools.data.simple.SimpleFeatureWriter
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 04ca02e264f (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d0668176da7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> e54506ef011 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d862b7df687 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> aede6534f0f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 4a6d96f2b4e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data._
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6a4ff24d14c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 15b6bf02d15 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> f76251a7560 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 775ed2dd6f1 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data._
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
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> a6ae93cb5f3 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 37636fb3b99 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4a6d96f2b4e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 04ca02e264f (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0668176da7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> aede6534f0f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 8f244c4b80a (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
=======
<<<<<<< HEAD
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 118e973349c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6e959c6dbc1 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data._
<<<<<<< HEAD
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
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 8d705f40286 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 75ae649304a (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> e54506ef011 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b71311c31d (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> d862b7df687 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 9f1e983c633 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 629e7e42b13 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f0edfc1258 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data._
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 5068ffbadcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 96eefca5aaf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 37636fb3b99 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 4a6d96f2b4e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> aede6534f0f (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 8f244c4b80a (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> d3cbb5103f3 (GEOMESA-3202 Check for disjoint date queries in merged view store)
import org.geotools.feature.{AttributeTypeBuilder, FeatureTypes, NameImpl}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.{Bounds, FilterHelper, FilterValues}
import org.locationtech.geomesa.utils.geotools.{SchemaBuilder, SimpleFeatureTypes}

import java.io.IOException
import java.time.ZonedDateTime

package object view extends LazyLogging {

  import org.locationtech.geomesa.filter.andFilters
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)

<<<<<<< HEAD
<<<<<<< HEAD
=======

>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
  private val dateBounds: LoadingCache[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]] =
    Caffeine.newBuilder().build(new CacheLoader[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]]() {
      override def load(key: (String, Filter)): Option[FilterValues[Bounds[ZonedDateTime]]] = {
        val (dtg, filter) = key
        Some(FilterHelper.extractIntervals(filter, dtg)).filter(_.nonEmpty)
      }
    })
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)

<<<<<<< HEAD
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
  private val dateBounds: LoadingCache[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]] =
    Caffeine.newBuilder().build(new CacheLoader[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]]() {
      override def load(key: (String, Filter)): Option[FilterValues[Bounds[ZonedDateTime]]] = {
        val (dtg, filter) = key
        Some(FilterHelper.extractIntervals(filter, dtg)).filter(_.nonEmpty)
      }
    })
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)

  private val dateBounds: LoadingCache[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]] =
    Caffeine.newBuilder().build(new CacheLoader[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]]() {
      override def load(key: (String, Filter)): Option[FilterValues[Bounds[ZonedDateTime]]] = {
        val (dtg, filter) = key
        Some(FilterHelper.extractIntervals(filter, dtg)).filter(_.nonEmpty)
      }
    })
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)

  private val dateBounds: LoadingCache[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]] =
    Caffeine.newBuilder().build(new CacheLoader[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]]() {
      override def load(key: (String, Filter)): Option[FilterValues[Bounds[ZonedDateTime]]] = {
        val (dtg, filter) = key
        Some(FilterHelper.extractIntervals(filter, dtg)).filter(_.nonEmpty)
      }
    })
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)

  private val dateBounds: LoadingCache[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]] =
    Caffeine.newBuilder().build(new CacheLoader[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]]() {
      override def load(key: (String, Filter)): Option[FilterValues[Bounds[ZonedDateTime]]] = {
        val (dtg, filter) = key
        Some(FilterHelper.extractIntervals(filter, dtg)).filter(_.nonEmpty)
      }
    })
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
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)

<<<<<<< HEAD
  private val dateBounds: LoadingCache[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]] =
    Caffeine.newBuilder().build(new CacheLoader[(String, Filter), Option[FilterValues[Bounds[ZonedDateTime]]]]() {
      override def load(key: (String, Filter)): Option[FilterValues[Bounds[ZonedDateTime]]] = {
        val (dtg, filter) = key
        Some(FilterHelper.extractIntervals(filter, dtg)).filter(_.nonEmpty)
      }
    })
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)

=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
  /**
    * Helper method to merge a filtered data store query
    *
    * @param query query
    * @param filter data store filter
    * @return
    */
  def mergeFilter(sft: SimpleFeatureType, query: Query, filter: Option[Filter]): Query = {
    mergeFilter(sft, query.getFilter, filter) match {
      case f if f.eq(query.getFilter) => query
      case f =>
        val q = new Query(query)
        q.setFilter(f)
        q
    }
  }

  /**
    * Helper method to merge a filtered data store query
    *
    * @param filter filter
    * @param option data store filter
    * @return
    */
  def mergeFilter(sft: SimpleFeatureType, filter: Filter, option: Option[Filter]): Filter = {
    option match {
      case None => filter
      case Some(f) if filter == Filter.INCLUDE => f
      case Some(f) =>
        // check for disjoint dates between the store filter and the query filter
        val intersected = sft.getDtgField.flatMap { dtg =>
<<<<<<< HEAD
          // note: don't cache this call, as it can contain things like `currentDate()` that will change per invocation
          Some(FilterHelper.extractIntervals(f, dtg)).filter(_.nonEmpty).flatMap { left =>
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
          dateBounds.get((dtg, f)).flatMap { left =>
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
          dateBounds.get((dtg, f)).flatMap { left =>
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
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
          dateBounds.get((dtg, f)).flatMap { left =>
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> bdd2bd6424 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
          dateBounds.get((dtg, f)).flatMap { left =>
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 985fbd05df (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
          dateBounds.get((dtg, f)).flatMap { left =>
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
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
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> a2184c3082 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
          dateBounds.get((dtg, f)).flatMap { left =>
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 5e0c1295b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 39517d146a (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 00b6906403 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f57ea41028 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c5c38a96d0 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
          dateBounds.get((dtg, f)).flatMap { left =>
<<<<<<< HEAD
>>>>>>> 96cd783e70 (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 6f8af866fb (GEOMESA-3202 Check for disjoint date queries in merged view store)
<<<<<<< HEAD
>>>>>>> 83eb282a5c (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
=======
=======
>>>>>>> 96cd783e7 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 051bc58bcf (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 26e758e6b9 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 85e1bb3bb8 (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e7949e9e55 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 99b5e2f0db (GEOMESA-3202 Check for disjoint date queries in merged view store)
=======
>>>>>>> 13656f5052 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 226f1d6822 (GEOMESA-3202 Check for disjoint date queries in merged view store)
>>>>>>> 9b782cefcb (GEOMESA-3202 Check for disjoint date queries in merged view store)
            val right = FilterHelper.extractIntervals(filter, dtg)
            val merged = FilterValues.and[Bounds[ZonedDateTime]](Bounds.intersection)(left, right)
            if (merged.disjoint) {
              logger.debug(s"Suppressing query with filter (${ECQL.toCQL(filter)}) AND (${ECQL.toCQL(f)})")
              Some(Filter.EXCLUDE)
            } else {
              None
            }
          }
        }
        intersected.getOrElse(andFilters(Seq(filter, f)))
    }
  }

  /**
    * Read only data store - does not support creating/updating/deleting schemas or features
    */
  trait ReadOnlyDataStore extends DataStore {

    private def error = throw new NotImplementedError("This data store is read-only")

    override def createSchema(featureType: SimpleFeatureType): Unit = error
    override def updateSchema(typeName: Name, featureType: SimpleFeatureType): Unit = error
    override def updateSchema(typeName: String, featureType: SimpleFeatureType): Unit = error
    override def removeSchema(typeName: Name): Unit = error
    override def removeSchema(typeName: String): Unit = error
    override def getFeatureWriter(typeName: String, transaction: Transaction): SimpleFeatureWriter = error
    override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): SimpleFeatureWriter = error
    override def getFeatureWriterAppend(typeName: String, transaction: Transaction): SimpleFeatureWriter = error
  }

  /**
    * Data store that merges the schemas from multiple delegate stores and presents them as a unified result
    *
    * @param stores delegate stores
    * @param namespace schema namespace
    */
  abstract class MergedDataStoreSchemas(stores: Seq[DataStore], namespace: Option[String])
      extends ReadOnlyDataStore {

    import scala.collection.JavaConverters._

    override def getTypeNames: Array[String] = stores.map(_.getTypeNames).reduceLeft(_ intersect _)

    override def getNames: java.util.List[Name] =
      java.util.Arrays.asList(getTypeNames.map(t => new NameImpl(namespace.orNull, t)): _*)

    override def getSchema(name: Name): SimpleFeatureType = getSchema(name.getLocalPart)

    override def getSchema(typeName: String): SimpleFeatureType = {
      val schemas = stores.map(_.getSchema(typeName))

      if (schemas.contains(null)) {
        return null
      }

      lazy val fail = new IOException("Delegate schemas do not match: " +
          schemas.map(SimpleFeatureTypes.encodeType).mkString(" :: "))

      schemas.reduceLeft[SimpleFeatureType] { case (left, right) =>
        if (left.getAttributeCount != right.getAttributeCount) {
          throw fail
        }
        val builder = new SchemaBuilder()
        val attribute = new AttributeTypeBuilder()

        val leftDescriptors = left.getAttributeDescriptors.iterator()
        val rightDescriptors = right.getAttributeDescriptors.iterator()

        while (leftDescriptors.hasNext) {
          val leftDescriptor = leftDescriptors.next
          val rightDescriptor = rightDescriptors.next
          if (leftDescriptor.getLocalName != rightDescriptor.getLocalName) {
            throw fail
          }
          val leftBinding = leftDescriptor.getType.getBinding
          val rightBinding = rightDescriptor.getType.getBinding
          // determine a common binding if possible, for things like java.sql.TimeStamp vs java.util.Date
          if (leftBinding == rightBinding || leftBinding.isAssignableFrom(rightBinding)) {
            attribute.binding(leftBinding)
          } else if (rightBinding.isAssignableFrom(leftBinding)) {
            attribute.binding(rightBinding)
          } else {
            throw fail
          }

          // add the user data from each descriptor so the delegate stores have it if needed
          leftDescriptor.getUserData.asScala.foreach { case (k, v) => attribute.userData(k, v) }
          rightDescriptor.getUserData.asScala.foreach { case (k, v) => attribute.userData(k, v) }

          Some(leftDescriptor).collect { case g: GeometryDescriptor => attribute.crs(g.getCoordinateReferenceSystem) }

          builder.addAttribute(attribute.buildDescriptor(leftDescriptor.getLocalName))
        }
        builder.build(namespace.orNull, typeName)
      }
    }

    override def dispose(): Unit = stores.foreach(_.dispose())

    override def getInfo: ServiceInfo = {
      val info = new DefaultServiceInfo()
      info.setDescription(s"Features from ${getClass.getSimpleName}")
      info.setSchema(FeatureTypes.DEFAULT_NAMESPACE)
      info
    }

    override def getLockingManager: LockingManager = null
  }
}
