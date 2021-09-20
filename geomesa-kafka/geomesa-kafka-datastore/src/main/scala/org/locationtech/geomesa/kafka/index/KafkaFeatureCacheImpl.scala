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
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 1c744a07e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ed87a467a1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bd91e28295 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e6fdf06261 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
=======
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a51d9f1aaf (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e7bfe001dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 404627bdcc (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
<<<<<<< HEAD
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
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5db00999ef (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
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
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a1258aa460 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> c46a601b81 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
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
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a1258aa46 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ad1c8b98d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e16ddaa185 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 18dfda309 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
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
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
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
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 18dfda3090 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ec80cd12d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 054a72ed9c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 818885c2b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6444c154b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> d04dc6d34 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc9bafe41 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 42ec5d4ce8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 39183654c3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f2dc074207 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 55d18fef60 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 4e031afd05 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> c5a452d055 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8e36ec0d0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5c61cc655 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d04dc6d340 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ed371dc574 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a974777993 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1a020f43ff (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
=======
=======
>>>>>>> 054a72ed9 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> fc9bafe416 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 20a188f4e0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 739f12059c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 85139491a6 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> a97477799 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7ae88ee16e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 1bd36986b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 2f07d41dc5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 5ad2f6a7b (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
<<<<<<< HEAD
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 67b319bb27 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
=======
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
=======
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 865c4a0be4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 16b14a4c3c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
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
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7e7054316d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c354d233a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 83a98c1a01 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
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
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
=======
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
=======
>>>>>>> 0fde036fa7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 39183654c (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 1c4a5022ad (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f2dc07420 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 08de19a9fd (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ff35f42ba8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2d530ccd74 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
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
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c744a07e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
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
>>>>>>> b57b24571d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
>>>>>>> e21d61704 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
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
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> c46a601b8 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 23799430d (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> ea8fbcfbe5 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> ed371dc57 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> e3dc30faf3 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8c123556c4 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $support")

>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
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
>>>>>>> 2e41c84a73 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 40535eaf00 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
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
>>>>>>> 897d857f0 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> abfd50800e (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
<<<<<<< HEAD
>>>>>>> 8d9eb32656 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
=======
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8347f5d27 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17d152a893 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7691783a57 (GEOMESA-3100 Kafka layer views (#2784))
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
