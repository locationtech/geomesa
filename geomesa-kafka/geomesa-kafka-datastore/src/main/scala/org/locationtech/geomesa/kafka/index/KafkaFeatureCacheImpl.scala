/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.index.{BucketIndexSupport, SizeSeparatedBucketIndexSupport, SpatialIndexSupport}
import org.locationtech.geomesa.kafka.data.KafkaDataStore.{IndexConfig, LayerView}
import org.locationtech.geomesa.kafka.index.FeatureStateFactory.{FeatureExpiration, FeatureState}

import java.util.concurrent._

/**
  * Feature cache implementation
  *
  * @param sft simple feature type
  * @param config index config
  */
class KafkaFeatureCacheImpl(sft: SimpleFeatureType, config: IndexConfig, layerViews: Seq[LayerView] = Seq.empty)
    extends KafkaFeatureCache with FeatureExpiration {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  // keeps location and expiry keyed by feature ID (we need a way to retrieve a feature based on ID for
  // update/delete operations). to reduce contention, we never iterate over this map
  private val state = new ConcurrentHashMap[String, FeatureState]

  private val support = createSupport(sft)

  private val factory = FeatureStateFactory(sft, support.index, config.expiry, this, config.executor)

  override val views: Seq[KafkaFeatureCacheView] =
    layerViews.map(view => KafkaFeatureCacheView(view, createSupport(view.viewSft)))

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8d7bdcf18d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c1bc2bba7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 357e15e582 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64b99590af (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 304d8d4049 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13347c6405 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 885a095ca8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f95d022f81 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 167163a839 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 918483049d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25f992b5c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 003580e799 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea2bb28fd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 48e91e750a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33f0b95d6b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4d05581b71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2dc926e901 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 722c18e4c2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5aa646c0e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 32b3c541fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08dfdc0d90 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d47b1e15e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d64c097082 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e2d4059d65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c15e33b461 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2c7572a737 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ee948e25d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4d37c35650 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dc2fcab3c1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> da45a85991 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 36fa236b19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 338d9ebe16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 627596d1ec (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46d6a53038 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
<<<<<<< HEAD
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5a56b37afd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 885a095ca8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2dc926e901 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8d7bdcf18d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 338d9ebe16 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8d7bdcf18d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 338d9ebe16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8d7bdcf18d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 338d9ebe16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08dfdc0d90 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f95d022f81 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f95d022f81 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f95d022f81 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c1bc2bba7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 167163a839 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 627596d1ec (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> c1bc2bba7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 167163a839 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 627596d1ec (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> c1bc2bba7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 357e15e582 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 304d8d4049 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 167163a839 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 627596d1ec (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13347c6405 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69287f5812 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d64c097082 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 36fa236b19 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8d7bdcf18d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c1bc2bba7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 357e15e582 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 357e15e582 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64b99590af (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 64b99590af (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 304d8d4049 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 304d8d4049 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13347c6405 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 885a095ca8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f95d022f81 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 167163a839 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 918483049d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25f992b5c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5a56b37afd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ea2bb28fd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 48e91e750a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2fce647015 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4d05581b71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2dc926e901 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 722c18e4c2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 32b3c541fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08dfdc0d90 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d47b1e15e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d64c097082 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e2d4059d65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c15e33b461 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d44f185056 (d)
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2c7572a737 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3ee948e25d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4d37c35650 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dc2fcab3c1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> da45a85991 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 36fa236b19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 338d9ebe16 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 627596d1ec (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 46d6a53038 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8d7bdcf18d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c1bc2bba7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 357e15e582 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64b99590af (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 304d8d4049 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13347c6405 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 885a095ca8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f95d022f81 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 167163a839 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea2bb28fd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 48e91e750a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4d05581b71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2dc926e901 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 722c18e4c2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 32b3c541fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08dfdc0d90 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d47b1e15e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d64c097082 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e2d4059d65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c15e33b461 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2c7572a737 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ee948e25d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4d37c35650 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dc2fcab3c1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> da45a85991 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 36fa236b19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 338d9ebe16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 627596d1ec (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46d6a53038 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 251aea223c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56698b9664 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 86fd8c7136 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 383515822f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 10d15a6a47 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 68a193d485 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e6fdf0626 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 722c18e4c2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e2d4059d65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c15e33b461 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 338d9ebe16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8d7bdcf18d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f95d022f81 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f95d022f81 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea2bb28fd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 48e91e750a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4d05581b71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2dc926e901 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 722c18e4c2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 32b3c541fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 08dfdc0d90 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d47b1e15e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d64c097082 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e2d4059d65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c15e33b461 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d44f185056 (d)
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea2bb28fd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 48e91e750a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2fce647015 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4d05581b71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2dc926e901 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 722c18e4c2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 32b3c541fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08dfdc0d90 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d47b1e15e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d64c097082 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e2d4059d65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c15e33b461 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f95d022f81 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c775836a11 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 48e91e750a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
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
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 722c18e4c2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 66f3ecf55c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> afe0d9d546 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9d180ad11b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7de4b55347 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> a4106a18db (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a80d0b72f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d44f185056 (d)
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d7c360ad3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08dfdc0d90 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f031c0062b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 789b32e4e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 3bc26bbeb7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa833cc6f7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8d7bdcf18d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 338d9ebe16 (GEOMESA-3100 Kafka layer views (#2784))
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8d7bdcf18d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 293943b693 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 338d9ebe16 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8d7bdcf18d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 87e1f9914e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 338d9ebe16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e91c2e02e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
=======
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 885a095ca8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 69287f5812 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13347c6405 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13347c6405 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 98857d43e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> f95d022f81 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
=======
>>>>>>> 167163a839 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 167163a839 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64b99590af (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> bdf8b4bfee (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64b99590af (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 167163a839 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
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
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d583a4bc64 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e0eccec01d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 169a52723b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 684a7cb599 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69287f5812 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9c77968e00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 819f44085b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2ee12dce1b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bdba076e79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 62f440b3a2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 955b731915 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e2aff21ea9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 40b1336067 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 40b1336067 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e2aff21ea9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 0fcff0b315 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 003580e799 (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8fd4b52074 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ca0e7f9e23 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 722ee572e5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 33f0b95d6b (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> dd980b9bf6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c3ab11ea86 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 26721add6a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c269ccf5e3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 70c6e3c2a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 159239e868 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5aa646c0e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 990b3b9b6f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2db2756651 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e9c220c2f9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ae468ad6f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5a33e8c4b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a6480bbdd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7088332aeb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 437b7c9758 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ba1575f931 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 77fda9e6a0 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 52803e45bd (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8f0f1b0f79 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e8dd13fa26 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1587fcc39d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 76e4fe0b29 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b399833c2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> be93b4fe87 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8d7bdcf18d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 61e815afbf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be34966a9a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 44b3849076 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33c4d8c2d7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ded4144b12 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e13c268516 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 16b6057491 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e75f4022e6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1bc2bba7f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 70c27c5cf1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6b51e11e71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1f39195661 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d329149a3b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c708bdc30c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 871923e198 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7d5b16be74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d5b61dfe8c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 10d470378c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d8bd15c19d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f4021c7727 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0b69db934e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4df16f3ea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4ed133dc14 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 14fb82f440 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 85139491a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 57d734da30 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 815cf9ece1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 9dd0f22fc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> ea0ead77f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92c4d1b330 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 33e796f4d0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 71c31e1589 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fcd79cd8d5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f5c61cc65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 357e15e582 (GEOMESA-3100 Kafka layer views (#2784))
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b14a4c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 248f4e6cc4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f3f0807ef9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 292b77d111 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dc84900201 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4bc781ae0d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 357e15e582 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
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
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

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
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43b3328091 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 7ae88ee16 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 64b99590af (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 293f338e96 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 64b99590af (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f07d41dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ad1c8b98d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7e7054316 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec81d420dc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> b0e086ac3a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8bea39acf2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac654400fa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e16ddaa18 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e41937833 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4672dab1df (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2bbb64a7fc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> 0fde036fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4f05dea44b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae2a886e4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d451bccb18 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 1c4a5022a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffaf1a953a (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6a8b6a067c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cf169cf1b6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54c7805369 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25154e2b51 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 602791ab04 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> ff35f42ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 023a497566 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 13fd1a2b83 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f7a7ff6458 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 795731d167 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 2d530ccd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
=======
=======
>>>>>>> 304d8d4049 (GEOMESA-3100 Kafka layer views (#2784))
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a51d9f1aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> efecf8c4e7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> cc8f236b0d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 6d9d7dfd39 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 25172c18fe (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e7bfe001d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a09ef82d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 882c4b4793 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 304d8d4049 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 082c87bda9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e062806100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 13347c6405 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b6b7e6210 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 885a095ca8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cdc534223 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f1d5439655 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0cc6fb789 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c0781d235d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0ab099fcb6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 96f945c352 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f95d022f81 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 834a5179ab (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae3972009 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7e93e55b4c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> cd51d293d1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ef6880dba6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f54609b8f8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7df5825672 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 51dd537849 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 649f2f9c22 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 167163a839 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50fdb9f590 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 40b1336067 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 918483049d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 25f992b5c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5a56b37afd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 003580e799 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea2bb28fd7 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 48e91e750a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3e21e219c0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2fce647015 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33f0b95d6b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 4d05581b71 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2dc926e901 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 451bc550b4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 092fe6a89f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 722c18e4c2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 2c023c70f2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5aa646c0e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 32b3c541fa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08dfdc0d90 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8b0413578 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d47b1e15e8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d64c097082 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 426c760ae9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eb65cbec9b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> cc0343f96c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 16c8888b3f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> e2d4059d65 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 897d857f0e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c15e33b461 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 697c847b3d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> d44f185056 (d)
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cd3ea640c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8fc523dbdf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2c7572a737 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3ee948e25d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4d37c35650 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> dc2fcab3c1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> da45a85991 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 36fa236b19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8a5efa3d0c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 3eed921617 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 338d9ebe16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c102631690 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe7ad2a3ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 36fe4c8b07 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e81109f8f1 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cd42e2dc59 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 44cbc1d279 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5d01abd713 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 015e77ff43 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 627596d1ec (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> fe4d75a20f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ffb56aa0aa (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 46d6a53038 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 7989aa5b6e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 0f4c4f114 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8291191f4f (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
  /**
    * Note: this method is not thread-safe. The `state` and `index` can get out of sync if the same feature
    * is updated simultaneously from two different threads
    *
    * In our usage, this isn't a problem, as a given feature ID is always operated on by a single thread
    * due to kafka consumer partitioning
    */
  override def put(feature: SimpleFeature): Unit = {
    val featureState = factory.createState(feature)
    logger.trace(s"${featureState.id} adding feature $featureState")
    val old = state.put(featureState.id, featureState)
    if (old == null) {
      featureState.insertIntoIndex()
      views.foreach(_.put(feature))
    } else if (old.time <= featureState.time) {
      logger.trace(s"${featureState.id} removing old feature")
      old.removeFromIndex()
      featureState.insertIntoIndex()
      views.foreach { view =>
        view.remove(featureState.id)
        view.put(feature)
      }
    } else {
      logger.trace(s"${featureState.id} ignoring out of sequence feature")
      if (!state.replace(featureState.id, featureState, old)) {
        logger.warn(s"${featureState.id} detected inconsistent state... spatial index may be incorrect")
        old.removeFromIndex()
        views.foreach(_.remove(featureState.id))
      }
    }
    logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
  }

  /**
    * Note: this method is not thread-safe. The `state` and `index` can get out of sync if the same feature
    * is updated simultaneously from two different threads
    *
    * In our usage, this isn't a problem, as a given feature ID is always operated on by a single thread
    * due to kafka consumer partitioning
    */
  override def remove(id: String): Unit = {
    logger.trace(s"$id removing feature")
    val old = state.remove(id)
    if (old != null) {
      old.removeFromIndex()
      views.foreach(_.remove(id))
    }
    logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
  }

  override def expire(featureState: FeatureState): Unit = {
    logger.trace(s"${featureState.id} expiring from index")
    if (state.remove(featureState.id, featureState)) {
      featureState.removeFromIndex()
      views.foreach(_.remove(featureState.id))
    }
    logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
  }

  override def clear(): Unit = {
    logger.trace("Clearing index")
    state.clear()
    support.index.clear()
    views.foreach(_.clear())
  }

  override def size(): Int = state.size()

  // optimized for filter.include
  override def size(f: Filter): Int = if (f == Filter.INCLUDE) { size() } else { query(f).length }

  override def query(id: String): Option[SimpleFeature] =
    Option(state.get(id)).flatMap(f => Option(f.retrieveFromIndex()))

  override def query(filter: Filter): Iterator[SimpleFeature] = support.query(filter)

  override def close(): Unit = factory.close()

  private def createSupport(sft: SimpleFeatureType): SpatialIndexSupport = {
    if (config.cqAttributes.nonEmpty) {
      // note: CQEngine handles points vs non-points internally
      KafkaFeatureCache.cqIndexSupport(sft, config)
    } else if (sft.isPoints) {
      BucketIndexSupport(sft, config.resolution.x, config.resolution.y)
    } else {
      SizeSeparatedBucketIndexSupport(sft, config.ssiTiers, config.resolution.x / 360d, config.resolution.y / 180d)
    }
  }
}
