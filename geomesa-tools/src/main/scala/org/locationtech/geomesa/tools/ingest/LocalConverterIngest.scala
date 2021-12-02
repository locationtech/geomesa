/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

<<<<<<< HEAD
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data._
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e2aff21ea9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
import java.io.Flushable
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, DataStoreFinder, DataUtilities, FeatureWriter, Transaction}
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
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e2aff21ea9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.jobs.JobResult.{JobFailure, JobSuccess}
import org.locationtech.geomesa.jobs._
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.ingest.IngestCommand.{IngestCounters, Inputs}
import org.locationtech.geomesa.tools.ingest.LocalConverterIngest.DataStoreWriter
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.fs.FileSystemDelegate.FileHandle
<<<<<<< HEAD
=======
import org.locationtech.geomesa.utils.io.fs.LocalDelegate.StdInHandle
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d2042eea397 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 9e6388168f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 3b338b30f9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 521da9cfe12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d9df30f610b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> b8a89e42d13 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 8401bce4102 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> fa58eedb460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 01f7701037b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> aec1c19ab02 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 184b58210c9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> ad10a9666aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 99942875696 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 6557674373d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> adc8eabfb02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 4f5d2571feb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 0699408297d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 0a11e4aff94 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 04b9b7c79a2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> b9b328881a9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 33511dd1c3b (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> abc9ceb3d7d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 06363b7f239 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 308ee781cfa (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 885cb0ec106 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> fd4b7d818e1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> bd35dc74000 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> b14f292888f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 100159e4bbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> dbdff4c3a88 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> d6db725e68b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 60df807de4a (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 77f1d486422 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 7349caccc38 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 72737cf3be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d1d849c5d0e (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> b74b2dd228f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c8dc147f252 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> e07b3201782 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> af68691d6fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5e809b33a0a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 10be5d2340b (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 4d245d0d00f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 3b0629a40ea (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 9a7539a7217 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 444dbbbb047 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d3b57f0af6d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 3bd3b83a140 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 101b2c45778 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> a3edd30243b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 3e81ddad5ab (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 63d85668ef7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 69d0250eb0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> a8442edc100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 3e1e323980f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 9fea1f55293 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 998de7e5d12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 1f50947e716 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 8ed2e555e44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 64224447912 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> cd902990736 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 87e6c8eea1a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> aaac9bf1b27 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> db99a96dac1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 78705c60114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> af3ad095e35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 1d6b54753ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1de0c0109c0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 0631068db32 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 49d78f3faf5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 6d798167530 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> b4bd85394e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> b1e6ebbda05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 392f0ed1292 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 101abbef24d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 621ccb5dbbb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 32a3a53f7b2 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 8c068b490fe (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> eeea39f5450 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 3c5c29f94e1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 9660f045a97 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1eff02937fd (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 25e4eb5cdfb (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> cfaead7e54a (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> dab2e331129 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42e4fa76b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 746f6647c78 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 0727aaeebdc (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 99d4d0bf11d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1c6ccd26e0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 89cef35c744 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 156e2c2bb1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 0e666a2f4d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 2c4ad73f132 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> b52b7b1d02a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f5b38acde9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 6aada6c06e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 3e6c36184cd (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 15b6bf02d15 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> f960fee3bbb (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 636d7801a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 52e4edb8ff7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 279c2742fe7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 04ff4c2eabd (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5cf35e3e27e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 10892515265 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 096816f96c6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 80e53781872 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> eb378945663 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 9a1c0e7c9da (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 50e09c8209f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 702db0bfee9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5590f4a9b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c4bb86aba42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> bf872733fee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 397dc9e36f2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> abc5f80485c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> a1218cc4a1c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 32c28b4211e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 8dd024f6791 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 86549c8cb31 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 21bb5fe391a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 9f49cacc441 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> bb91d84f834 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 001e9598d37 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 680fc0f3b36 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f999ddcfadf (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 21f3965025f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 5884c46afd6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5c65b91e589 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4b9da52f142 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 3c064a6a4a4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c2669d51eed (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 253df9bf427 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1316cb79ab5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> e3fb7f978cf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 8670005b874 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> d705a7f5830 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> b89af2347e3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> d3abe9bd015 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 64a645069b5 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e3538892b8f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a72d5e76aa8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 28f0c187b75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e703f8474af (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad87db2ebb0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66d19a98428 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 86eb66d5ec4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 508b1195899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9f6ad439d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2012000cea3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f88704ee86e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 946c54d73c0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ea5275d6e19 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 07fd0942d49 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 72292719c58 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 41e9ed1c37d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9b02eedb04d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9ac51f780b7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 09fbc84447a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 29b01d02901 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5aaa49b4df0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5b005f1db8d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c39b13fdb9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c778580f8f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4624130386 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> df6612e6ec0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 001e2fde484 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a9781bb2026 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 73ef9e5c686 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a10b8c237ea (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7801ae9dc3b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ae03a10633 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b398d4187e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13e887e9b62 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4bd39326ff (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2205f4129fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2787ace19dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c2abe2708d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2cc10cb7932 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bb3613638a5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 709d36fab5e (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d788072e189 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c6f43df8275 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb1847becc6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4eadb847604 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66d5d413b21 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0cd2c2195cf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> df56e0b4fd4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3ab0fc7c466 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3a2237177d5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e8cdc4b6294 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e1bc7bc0c7d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9d5aa5a93dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b712cf79eb0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fb53c652aa3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a349824633e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0dbbd51f49b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9b10810a151 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 510c2f4fe95 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 921ed24fe98 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1941bbbae75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae9ccc22c0e (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8e6907185ac (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a50a7b0d07c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c4561e0da47 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 79b18350f53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c856252e4bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1375e7fd76d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 07d5ec61379 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 858cb0e39e4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e78a70560c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e5657746ade (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de0d3bd4725 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c105768b560 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fb8049ef88d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ab46ac2ae83 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a475643048f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e7eb470be26 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 58dd8d20ca1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c012a800625 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ab33667700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 820162570fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0100ea68c90 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6429d451177 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f9a29f55978 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 799c445b25f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ed24c3a4d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0c92ccefb1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 994cb7be424 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e68a6319686 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 03ad1e4fcb3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 106a61eb7a0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c11cab6e6c4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a33d1459222 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9a1e75a1629 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 24e87f012ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b2c5cb17bee (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> bca28211b26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7e94f45bdd1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1bf28a76ed (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6299e3e5f9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb81be58288 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 27f4344a9c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cafe159b7cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ba04fa6960a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 719dfe82334 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d537e16ea10 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5e2a645c4f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 58c99c469b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db5190d175c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8cf40279b08 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a937c867113 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 72fe4d51c07 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d91d2864ea2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ec4da97080 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9d0c0b4b7c7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> acef8d6477e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 862aae30a3b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ada7bd61a61 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f73a841169 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c7b37efcd00 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> eada042dfd5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 46dffcf8831 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dd4dd7a3ed5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb806ae80dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f07d1e863ca (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e2aff21ea9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ad659424239 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 50e677e046 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a72d5e76aa8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 28f0c187b75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 302df4295a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e703f8474af (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ad87db2ebb0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66d19a98428 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 86eb66d5ec4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1db60d5575 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 508b1195899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d9f6ad439d3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2012000cea3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f88704ee86e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 946c54d73c0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ea5275d6e19 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 07fd0942d49 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 72292719c58 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 41e9ed1c37d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9b02eedb04d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9ac51f780b7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 09fbc84447a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 29b01d02901 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5aaa49b4df0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 5b005f1db8d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 4c39b13fdb9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c778580f8f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4624130386 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> df6612e6ec0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 001e2fde484 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a9781bb2026 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 73ef9e5c686 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a10b8c237ea (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7801ae9dc3b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1ae03a10633 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b398d4187e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 13e887e9b62 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b4bd39326ff (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2205f4129fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2787ace19dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c2abe2708d1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2cc10cb7932 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bb3613638a5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 709d36fab5e (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d788072e189 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c6f43df8275 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 49471dea7a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> eb1847becc6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4eadb847604 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0292e04e31 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66d5d413b21 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0cd2c2195cf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d43ceee7c5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> df56e0b4fd4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3ab0fc7c466 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e39c04d2ed (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3a2237177d5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e8cdc4b6294 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 56470bf31b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e1bc7bc0c7d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d5aa5a93dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 653147a99e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b712cf79eb0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fb53c652aa3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a349824633e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0dbbd51f49b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3b3b527400 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9b10810a151 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 510c2f4fe95 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b59955740a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 921ed24fe98 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1941bbbae75 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> ae9ccc22c0e (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8e6907185ac (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a50a7b0d07c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9a8065c960 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c4561e0da47 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 79b18350f53 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2486842735 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c856252e4bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1375e7fd76d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 622d048a77 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 07d5ec61379 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 858cb0e39e4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e78a70560c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e5657746ade (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 476df18562 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> de0d3bd4725 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c105768b560 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fb8049ef88d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> ab46ac2ae83 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a475643048f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e7eb470be26 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 809016b858 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 58dd8d20ca1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c012a800625 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2e3aa14e0f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8ab33667700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 820162570fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0100ea68c90 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6429d451177 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 34eaf97eb7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f9a29f55978 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 799c445b25f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 34a0684bb3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6ed24c3a4d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0c92ccefb1d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 994cb7be424 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e68a6319686 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 03ad1e4fcb3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 106a61eb7a0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c11cab6e6c4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a33d1459222 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9a1e75a1629 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 24e87f012ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> b2c5cb17bee (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bca28211b26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7e94f45bdd1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e4688de07e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a1bf28a76ed (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6299e3e5f9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e0403a237f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cb81be58288 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 27f4344a9c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cafe159b7cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ba04fa6960a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 84b83872bb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 719dfe82334 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d537e16ea10 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f5e2a645c4f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 58c99c469b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> db5190d175c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8cf40279b08 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a937c867113 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 72fe4d51c07 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d91d2864ea2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2ec4da97080 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9d0c0b4b7c7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> acef8d6477e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 862aae30a3b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ada7bd61a61 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0f73a841169 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 08e65a6d0f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> c7b37efcd00 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> eada042dfd5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ff20be1be (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 46dffcf8831 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dd4dd7a3ed5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7f900d0b0b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cb806ae80dc (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f07d1e863ca (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ff20be1be (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2486842735 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e3aa14e0f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 653147a99e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d43ceee7c5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7f900d0b0b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d43ceee7c5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d43ceee7c5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f900d0b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e39c04d2ed (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 622d048a77 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34eaf97eb7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 622d048a77 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 34eaf97eb7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 56470bf31b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 56470bf31b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 56470bf31b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34a0684bb3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 34a0684bb3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 653147a99e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 653147a99e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 653147a99e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3b3b527400 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 476df18562 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 476df18562 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> b59955740a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> b59955740a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> b59955740a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0292e04e31 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e39c04d2ed (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2486842735 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 622d048a77 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 476df18562 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
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
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 08e65a6d0f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ff20be1be (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 50e677e046 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 49471dea7a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0292e04e31 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d43ceee7c5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e39c04d2ed (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 56470bf31b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 653147a99e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3b3b527400 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b59955740a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9a8065c960 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2486842735 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 622d048a77 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 476df18562 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 809016b858 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 809016b858 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e3aa14e0f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2e3aa14e0f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34eaf97eb7 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34a0684bb3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4688de07e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> e0403a237f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 84b83872bb (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08e65a6d0f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ff20be1be (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7f900d0b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 49471dea7a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0292e04e31 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d43ceee7c5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e39c04d2ed (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56470bf31b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 653147a99e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3b3b527400 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b59955740a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9a8065c960 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2486842735 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 622d048a77 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 476df18562 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 809016b858 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e3aa14e0f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34eaf97eb7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34a0684bb3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44901ceea1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4688de07e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0403a237f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 84b83872bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08e65a6d0f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ff20be1be (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f900d0b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0292e04e31 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56470bf31b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 653147a99e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b59955740a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 49471dea7a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3b3b527400 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 9a8065c960 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 622d048a77 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> fa3a402d4 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 476df18562 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 500975957 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
=======
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0403a237f (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ec25cdce7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 79e839e899 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5626ca0b0d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9870b2cd8e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> bc642b27fb (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> aaef016326 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a8155d31a1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f4291966d2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> baa52efeb2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 397a13ab3c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> eccc16ddf (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ccaae60e3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca34f46df7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> da62907bfd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e5af7e7136 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b0af051989 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9863eae6df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6e6d5a01cd (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 08e65a6d0f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 67d0823d3f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> dc0485bb65 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8ba157e9a8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 05e5d9d40f (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> c9d6a275e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d9cbd16a76 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 55873b0ace (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9ddbf4df0c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 319ecdc02f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> afff6fd74b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e2aff21ea9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 50e677e046 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 302df4295a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1eeec89cf0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f3a49e0825 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1db60d5575 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> ca4bea4f41 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 3bdeec0098 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9ac92c0689 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f7bfc7004 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1833cacd09 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6519fcd623 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1d054f2cda (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d4d928ab44 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7b6034d4cb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 94cb6806e0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 0166e9455b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> f669e5c3e5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 149b7a780 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a79eaecf5b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 13d53e6b02 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 25e967804c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 07e49471e8 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f79835b1b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> b17adcecc4 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2ae5d0a688 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 49471dea7a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0292e04e31 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d43ceee7c5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e39c04d2ed (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56470bf31b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 653147a99e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55474e65f0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3b3b527400 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b59955740a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> c83e8187d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7a670e5d35 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0a486b23ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e014d8c87 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8cb3591e39 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9a8065c960 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a27f5014e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 66cba04aa1 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2486842735 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3773ac3a73 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 622d048a77 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 63203d9a42 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 476df18562 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> 74661c314 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6839f8efad (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 45feada390 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1bd75453df (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 809016b858 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae56 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 15a956a712 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 2f9ede2971 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9dce3cdf1c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34eaf97eb7 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 8caee7452 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6d0e01a084 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 773beb5125 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 34eaf97eb7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34a0684bb3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> e734e4d06 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d1cf3ad8b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 354930933d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7f45654b0b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 8bdc705b7b (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 25e967804 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6889ac2407 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 6f2a76d6d7 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 58df64af5a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> a2ac294bf (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 74df8be7bc (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 868c873f5c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> d554ef0b1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
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
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b117271d95 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> b17adcecc (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e4688de07e (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2a5fd16e2a (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 74447e6d9b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> e0403a237f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f519540 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4bf896fb5b (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 1814f5456f (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 52f1b520cd (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 84b83872bb (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4f23877e26 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 225e4b4ede (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 84b83872bb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a12 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ade675c0aa (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 955a17fa3d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7733b864f9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b9e5c92638 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d9a9062a0 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d6289c684 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 54cfc0cf1 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 02ce9d812 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c19ca660e (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 37e26bc137 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 9cf623a4be (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81b8eb5ae (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55324ccde8 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 7dc0abee74 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
>>>>>>> 21308670a5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 706bcb3d36 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08e65a6d0f (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ff20be1be (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7f900d0b0b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
import org.locationtech.geomesa.utils.text.TextTools

import java.io.Flushable
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors}
import scala.util.control.NonFatal

/**
 * Ingestion that uses geomesa converters to process input files
 *
 * @param ds data store
 * @param sft simple feature type
 * @param converterConfig converter definition
 * @param inputs paths to ingest
 * @param numThreads how many threads to use
 */
class LocalConverterIngest(
    ds: DataStore,
    dsParams: java.util.Map[String, _],
    sft: SimpleFeatureType,
    converterConfig: Config,
    inputs: Inputs,
    numThreads: Int
  ) extends Awaitable with LazyLogging {

  private val files = inputs.handles
  private val latch = new CountDownLatch(files.length)

  private val threads = if (numThreads <= files.length) { numThreads } else {
    Command.user.warn("Can't use more threads than there are input files - reducing thread count")
    files.length
  }

  private val batch = IngestCommand.LocalBatchSize.toInt.getOrElse {
    throw new IllegalArgumentException(
      s"Invalid batch size for property ${IngestCommand.LocalBatchSize.property}: " +
          IngestCommand.LocalBatchSize.get)
  }

  private val es = Executors.newFixedThreadPool(threads)

  // global counts shared among threads
  private val written = new AtomicLong(0)
  private val failed = new AtomicLong(0)
  private val errors = new AtomicInteger(0)

  private val bytesRead = new AtomicLong(0L)

  private val batches = new ConcurrentHashMap[FeatureWriter[SimpleFeatureType, SimpleFeature], AtomicInteger](threads)

  // keep track of failure at a global level, keep line counts and success local
  private val globalFailures = new com.codahale.metrics.Counter {
    override def inc(): Unit = failed.incrementAndGet()
    override def inc(n: Long): Unit = failed.addAndGet(n)
    override def dec(): Unit = failed.decrementAndGet()
    override def dec(n: Long): Unit = failed.addAndGet(-1 * n)
    override def getCount: Long = failed.get()
  }

  private val progress: () => Float =
    if (inputs.stdin) {
      () => .99f // we don't know how many bytes are actually available
    } else {
      val length = files.map(_.length).sum.toFloat // only evaluate once
      () => bytesRead.get / length
    }

  Command.user.info(s"Ingesting ${if (inputs.stdin) { "from stdin" } else { TextTools.getPlural(files.length, "file") }} " +
      s"with ${TextTools.getPlural(threads, "thread")}")

  private val converters = CloseablePool(SimpleFeatureConverter(sft, converterConfig), threads)
  private val writers = {
    def factory: FeatureWriter[SimpleFeatureType, SimpleFeature] =
      if (threads > 1 && ds.getClass.getSimpleName.equals("JDBCDataStore")) {
        // creates a new data store for each writer to avoid synchronized blocks in JDBCDataStore.
        // the synchronization is to allow for generated fids from the database.
        // generally, this shouldn't be an issue since we use provided fids,
        // but running with 1 thread  would restore the old behavior
        new DataStoreWriter(dsParams, sft.getTypeName)
      } else {
        ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
      }
    CloseablePool(factory, threads)
  }

  private val closer = new Runnable() {
    override def run(): Unit = {
      latch.await()
      CloseWithLogging(converters)
      CloseWithLogging(writers).foreach(_ => errors.incrementAndGet())
    }
  }

  private val futures = files.map(f => es.submit(new LocalIngestWorker(f))) :+ CachedThreadPool.submit(closer)

  es.shutdown()

  override def await(reporter: StatusCallback): JobResult = {
    while (!es.isTerminated) {
      Thread.sleep(500)
      reporter("", progress(), counters, done = false)
    }
    reporter("", progress(), counters, done = true)

    // Get all futures so that we can propagate the logging up to the top level for handling
    // in org.locationtech.geomesa.tools.Runner to catch missing dependencies
    futures.foreach(_.get)

    if (errors.get > 0) {
      JobFailure("Some files caused errors, check logs for details")
    } else {
      val message =
        if (inputs.stdin) { "from stdin" } else if (files.lengthCompare(1) == 0) { s"for file ${files.head.path}" } else { "" }
      JobSuccess(message, counters.toMap)
    }
  }

  /**
   * Hook to allow modification of the feature returned by the converter
   *
   * @param iter features
   * @return
   */
  protected def features(iter: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = iter

  private def counters: Seq[(String, Long)] =
    Seq((IngestCounters.Ingested, written.get()), (IngestCounters.Failed, failed.get()))

  class LocalIngestWorker(file: FileHandle) extends Runnable {
    override def run(): Unit = {
      try {
        converters.borrow { converter =>
          WithClose(file.open) { streams =>
            streams.foreach { case (name, is) =>
              val params = EvaluationContext.inputFileParam(name.getOrElse(file.path))
              val success = converter.createEvaluationContext().success
              val ec = converter.createEvaluationContext(params, success, globalFailures)
              WithClose(LocalConverterIngest.this.features(converter.process(is, ec))) { features =>
                writers.borrow { writer =>
                  var count = batches.get(writer)
                  if (count == null) {
                    count = new AtomicInteger(0)
                    batches.put(writer, count)
                  }
                  features.foreach { sf =>
                    try {
                      FeatureUtils.write(writer, sf)
                      written.incrementAndGet()
                      count.incrementAndGet()
                    } catch {
                      case NonFatal(e) =>
                        logger.error(s"Failed to write '${DataUtilities.encodeFeature(sf)}'", e)
                        failed.incrementAndGet()
                    }
                    if (count.get % batch == 0) {
                      count.set(0)
                      writer match {
                        case f: Flushable => f.flush()
                        case _ => // no-op
                      }
                    }
                  }
                }
              }
            }
          }
        }
      } catch {
        case e @ (_: ClassNotFoundException | _: NoClassDefFoundError) =>
          // Rethrow exception so it can be caught by getting the future of this runnable in the main thread
          // which will in turn cause the exception to be handled by org.locationtech.geomesa.tools.Runner
          // Likely all threads will fail if a dependency is missing so it will terminate quickly
          throw e

        case NonFatal(e) =>
          // Don't kill the entire program b/c this thread was bad! use outer try/catch
          val msg = s"Fatal error running local ingest worker on ${file.path}"
          Command.user.error(msg)
          logger.error(msg, e)
          errors.incrementAndGet()
      } finally {
        latch.countDown()
        bytesRead.addAndGet(file.length)
      }
    }
  }
}

object LocalConverterIngest {

  class DataStoreWriter(connection: java.util.Map[String, _], typeName: String)
      extends FeatureWriter[SimpleFeatureType, SimpleFeature] {

    private val ds = DataStoreFinder.getDataStore(connection)
    private val writer = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)

    override def getFeatureType: SimpleFeatureType = writer.getFeatureType
    override def next(): SimpleFeature = writer.next()
    override def remove(): Unit = writer.remove()
    override def write(): Unit = writer.write()
    override def hasNext: Boolean = writer.hasNext
    override def close(): Unit = {
      var err: Throwable = null
      CloseQuietly(writer).foreach(err = _)
      CloseQuietly(ds).foreach { e =>
        if (err == null) { err = e } else { err.addSuppressed(e) }
      }
      if (err != null) {
        throw err
      }
    }
  }
}
