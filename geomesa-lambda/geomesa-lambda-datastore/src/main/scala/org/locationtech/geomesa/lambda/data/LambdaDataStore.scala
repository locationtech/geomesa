/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.geotools.api.data._
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.data._
import org.geotools.feature.FeatureTypes
<<<<<<< HEAD
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader.HasGeoMesaFeatureReader
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
import org.locationtech.geomesa.index.geotools.{GeoMesaDataStore, GeoMesaFeatureReader, GeoMesaFeatureStore}
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats, NoopStats}
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.data.LambdaFeatureWriter.{AppendLambdaFeatureWriter, ModifyLambdaFeatureWriter, RequiredVisibilityWriter}
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore
import org.locationtech.geomesa.lambda.stream.{OffsetManager, TransientStore}
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}

import java.time.Clock
import java.util.{Collections, Properties}
import scala.concurrent.duration.Duration

class LambdaDataStore(val persistence: DataStore, config: LambdaConfig)(implicit clock: Clock = Clock.systemUTC())
<<<<<<< HEAD
    extends DataStore with HasGeoMesaStats with HasGeoMesaFeatureReader with LazyLogging {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
    extends DataStore with HasGeoMesaStats with LazyLogging {
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
    extends DataStore with HasGeoMesaStats with LazyLogging {
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  private val authProvider: Option[AuthorizationsProvider] = persistence match {
    // this is a bit of a hack to work around hbase null vs empty auths
    case ds: GeoMesaDataStore[_] if ds.config.authProvider.getAuthorizations != null => Some(ds.config.authProvider)
    case _ => None
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private [lambda] val transients = Caffeine.newBuilder().build[String, TransientStore](
    new CacheLoader[String, TransientStore] {
      override def load(key: String): TransientStore =
        new KafkaStore(persistence, persistence.getSchema(key), authProvider, config)
    }
  )
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
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
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
  private [lambda] val transients = Caffeine.newBuilder().build(new CacheLoader[String, TransientStore] {
    override def load(key: String): TransientStore =
      new KafkaStore(persistence, persistence.getSchema(key), authProvider, config)
  })
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 1a21a3c300 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e079 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
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
>>>>>>> 30d431265a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dc17df6c0b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b01 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eee5d7ae15 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 374ba65da3 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
<<<<<<< HEAD
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 99fd8486a3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 45dca3e07 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d898c0c52 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98a03c5861 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ee3aea1cdc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b1 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b5c172e61d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0cdf3f2672 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 425a920afa (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6f7e363c75 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f9397984eb (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e29676c9b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7ea07a654a (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 6a3ccffd0c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> eb01a83daf (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 0e2352ff05 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 3cb02b7b0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 9814eb9768 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> eee5d7ae1 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 83670fc938 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 374ba65da (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 8037623548 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 99fd8486a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> a7ca1d57e6 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 5d898c0c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> ca22354dd7 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 98a03c586 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9e8a3c11c3 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> ee3aea1cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 2d758409cd (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> b36757a53 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bf90e5fa00 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> f8f49130b (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 4a47c538bc (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
=======
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b5c172e61 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> e762caca54 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 595c43086 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 0cdf3f267 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
<<<<<<< HEAD
>>>>>>> 44b15e96d9 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 4623d9a687 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 1a21a3c30 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
>>>>>>> 9cdd96d1c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> c3e314fa80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0da1bb22c5 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 92dccb4d7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e763f098e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9b21f252a6 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> 30d431265 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a990ff330c (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> dc17df6c0 (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
>>>>>>> 275e53813f (GEOMESA-3113 Add system property to managing HBase deletes with visibilities (#2792))
=======
=======
>>>>>>> f487c279d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> b23b68a157 (GEOMESA-3092 Support Lambda NiFi processor (#2777))

  override val stats: GeoMesaStats = persistence match {
    case p: HasGeoMesaStats => new LambdaStats(p.stats, transients)
    case _ => NoopStats
  }

  private val runner = new LambdaQueryRunner(this, persistence, transients)

  def persist(typeName: String): Unit = transients.get(typeName).persist()

  override def getTypeNames: Array[String] = persistence.getTypeNames

  override def getNames: java.util.List[Name] = persistence.getNames

  override def createSchema(sft: SimpleFeatureType): Unit = {
    persistence.createSchema(sft)
    // TODO for some reason lambda qs consumers don't rebalance when the topic is created after the consumers...
    // transients.get(sft.getTypeName).createSchema()
    val topic = KafkaStore.topic(config.zkNamespace, sft)
    val props = new Properties()
    config.producerConfig.foreach { case (k, v) => props.put(k, v) }

    WithClose(AdminClient.create(props)) { admin =>
      if (admin.listTopics().names().get.contains(topic)) {
        logger.warn(s"Topic [$topic] already exists - it may contain stale data")
      } else {
        val replication = SystemProperty("geomesa.kafka.replication").option.map(_.toInt).getOrElse(1)
        val newTopic = new NewTopic(topic, config.partitions, replication.toShort)
        admin.createTopics(Collections.singletonList(newTopic)).all().get
      }
    }
  }

  override def getSchema(typeName: Name): SimpleFeatureType = getSchema(typeName.getLocalPart)

  override def getSchema(typeName: String): SimpleFeatureType = {
    val sft = persistence.getSchema(typeName)
    if (sft != null) {
      // load our transient store so it starts caching features, etc
      transients.get(typeName)
    }
    sft
  }

  override def updateSchema(typeName: Name, featureType: SimpleFeatureType): Unit =
    updateSchema(typeName.getLocalPart, featureType)

  override def updateSchema(typeName: String, featureType: SimpleFeatureType): Unit = {
    val transient = transients.get(typeName)
    if (typeName != featureType.getTypeName) {
      // ensure that we've loaded the entire kafka topic
      logger.debug("Update schema: entering quiet period")
      Thread.sleep(SystemProperty("geomesa.lambda.update.quiet.period", "10 seconds").toDuration.get.toMillis)
      WithClose(transient.read().iterator()) { toPersist =>
        if (toPersist.nonEmpty) {
          logger.debug("Update schema: persisting transient features")
          WithClose(persistence.getFeatureWriter(typeName, Transaction.AUTO_COMMIT)) { writer =>
            toPersist.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }
        }
      }
    }
    CloseWithLogging(transient)
    transients.invalidate(typeName)
    persistence.updateSchema(typeName, featureType)
  }

  override def removeSchema(typeName: Name): Unit = removeSchema(typeName.getLocalPart)

  override def removeSchema(typeName: String): Unit = {
    // note: call transient first, as it may rely on the schema being present
    val transient = transients.get(typeName)
    transient.removeSchema()
    CloseWithLogging(transient)
    transients.invalidate(typeName)
    persistence.removeSchema(typeName)
  }

  override def getFeatureSource(typeName: Name): SimpleFeatureSource = getFeatureSource(typeName.getLocalPart)

  override def getFeatureSource(typeName: String): SimpleFeatureSource =
    new GeoMesaFeatureStore(this, getSchema(typeName))

  override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader =
    getFeatureReader(getSchema(query.getTypeName), transaction, query).reader()

  override private[geomesa] def getFeatureReader(
      sft: SimpleFeatureType,
      transaction: Transaction,
      query: Query): GeoMesaFeatureReader = {
    GeoMesaFeatureReader(sft, query, runner, None)
  }

  override def getFeatureWriterAppend(typeName: String, transaction: Transaction): SimpleFeatureWriter = {
    val transient = transients.get(typeName)
    if (transient.sft.isVisibilityRequired) {
      new AppendLambdaFeatureWriter(transient) with RequiredVisibilityWriter
    } else {
      new AppendLambdaFeatureWriter(transient)
    }
  }

  override def getFeatureWriter(typeName: String, transaction: Transaction): SimpleFeatureWriter =
    getFeatureWriter(typeName, Filter.INCLUDE, transaction)

  override def getFeatureWriter(typeName: String,
                                filter: Filter,
                                transaction: Transaction): SimpleFeatureWriter= {
    val query = new Query(typeName, filter)
    val features = SelfClosingIterator(getFeatureReader(query, transaction))
    val transient = transients.get(typeName)
    if (transient.sft.isVisibilityRequired) {
      new ModifyLambdaFeatureWriter(transient, features) with RequiredVisibilityWriter
    } else {
      new ModifyLambdaFeatureWriter(transient, features)
    }
  }

  override def dispose(): Unit = {
    CloseWithLogging(transients.asMap.asScala.values)
    transients.invalidateAll()
    CloseWithLogging(config.offsetManager)
    persistence.dispose()
  }

  override def getInfo: ServiceInfo = {
    val info = new DefaultServiceInfo()
    info.setDescription(s"Features from ${getClass.getSimpleName}")
    info.setSchema(FeatureTypes.DEFAULT_NAMESPACE)
    info
  }

  override def getLockingManager: LockingManager = null
}

object LambdaDataStore {
  case class LambdaConfig(
      zookeepers: String,
      zkNamespace: String,
      producerConfig: Map[String, String],
      consumerConfig: Map[String, String],
      partitions: Int,
      consumers: Int,
      expiry: Duration,
      persist: Boolean,
      offsetManager: OffsetManager
    )
}
