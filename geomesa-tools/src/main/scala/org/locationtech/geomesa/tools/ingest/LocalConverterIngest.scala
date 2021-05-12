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
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import java.io.Flushable
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, DataStoreFinder, DataUtilities, FeatureWriter, Transaction}
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 686339d050 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
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
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6289007008 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 5ba418ba6c (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8caee74520 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
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
>>>>>>> e734e4d064 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d1cf3ad8b5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a2ac294bf3 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ba418ba6 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> dd5d6434b0 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0d80bae0c6 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 149b7a7809 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
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
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
=======
import org.locationtech.geomesa.utils.io.{CloseWithLogging, CloseablePool, WithClose}
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 0f4c829f25 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
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
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 55b43ae566 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> cb7d3570f5 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 6ac55e1ef7 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
=======
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging, CloseablePool, WithClose}
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd5d6434b (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> eccc16ddf9 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0d80bae0c (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 71edb3b56e (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 686339d05 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> f7f5195405 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f3a49e082 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 84bc7e0e2a (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 628900700 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 849693a129 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d9a9062a08 (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54cfc0cf1a (GEOMESA-3071 Move all converter state into evaluation context)
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0f4c829f2 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 02ce9d8126 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 455aae09d (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 81b8eb5aeb (GEOMESA-3071 Move all converter state into evaluation context)
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
