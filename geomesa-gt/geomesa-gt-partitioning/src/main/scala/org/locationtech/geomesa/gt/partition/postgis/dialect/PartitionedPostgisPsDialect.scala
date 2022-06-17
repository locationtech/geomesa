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
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> fb4b9418a7 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1a6dc23ec5 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 4bc3de3352 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 297e1658f6 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> d06383a1ce (GEOMESA-3215 Postgis - support List-type attributes)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> b597cf01b8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
<<<<<<< HEAD
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> a8e0698bf72 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e68704b1b09 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 2b39d6794f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> b7d126fbb7b (GEOMESA-3215 Postgis - support List-type attributes)
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2590b561114 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> b7d126fbb7b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 7355805fa9c (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
=======
=======
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 8362c3a15dd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 716b38183d9 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 5ef1a7e6d88 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 09aa9c40cda (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 0853537f254 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 4cf84d963c0 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 8c5addb8623 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 15b6bf02d15 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> e11195043a6 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f39b806cb46 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 921bf77948c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> be55533f39c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> be18eccae24 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bf318c77551 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> be18eccae24 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> b21dd7923ba (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> b597cf01b8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 2b39d6794f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> b7d126fbb7b (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 7355805fa9c (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 73db5e19227 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 0141c7f7075 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 8d4e0d03cae (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> b0b6514b0dd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> e4496a66ba1 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 09be37fcacc (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1de674f587b (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> a9c1c6485f2 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> be55533f39c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> be18eccae24 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 689aedaab2c (GEOMESA-3215 Postgis - support List-type attributes)
import org.geotools.data.postgis.PostGISPSDialect
import org.geotools.jdbc.JDBCDataStore

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b597cf01b8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
=======
import java.sql.{Connection, DatabaseMetaData}
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
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
=======
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b597cf01b8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
=======
import java.sql.{Connection, DatabaseMetaData, PreparedStatement, Types}
import java.util.Locale
import java.util.concurrent.TimeUnit
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)

class PartitionedPostgisPsDialect(store: JDBCDataStore, delegate: PartitionedPostgisDialect)
    extends PostGISPSDialect(store, delegate){

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b597cf01b8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
  import PartitionedPostgisPsDialect.PreparedStatementKey

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
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
  import PartitionedPostgisPsDialect.PreparedStatementKey

<<<<<<< HEAD
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
  import PartitionedPostgisPsDialect.PreparedStatementKey

>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
=======
=======
  import PartitionedPostgisPsDialect.PreparedStatementKey

<<<<<<< HEAD
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> b597cf01b8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
  import PartitionedPostgisPsDialect.PreparedStatementKey

>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
  // cache for tracking json-type columns
  private val jsonColumns: LoadingCache[PreparedStatementKey, java.lang.Boolean] =
    Caffeine.newBuilder()
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .build(new CacheLoader[PreparedStatementKey, java.lang.Boolean]() {
          override def load(key: PreparedStatementKey): java.lang.Boolean = {
            key.ps.getParameterMetaData.getParameterTypeName(key.column).toLowerCase(Locale.US) match {
              case "jsonb" | "json" => true
              case _ => false
            }
          }
        })

  override def setValue(value: Any, binding: Class[_], ps: PreparedStatement, column: Int, cx: Connection): Unit = {
    // json columns are string type in geotools, but we have to use setObject or else we get a binding error
    if (binding == classOf[String] && jsonColumns.get(new PreparedStatementKey(ps, column))) {
      ps.setObject(column, value, Types.OTHER)
<<<<<<< HEAD
    } else if (binding == classOf[java.util.List[_]]) {
      // handle bug in jdbc store not calling setArrayValue in update statements
      value match {
        case null =>
          ps.setNull(column, Types.ARRAY)

        case list: java.util.Collection[_] =>
          if (list.isEmpty) {
            ps.setNull(column, Types.ARRAY)
          } else {
            setArray(list.toArray(), ps, column, cx)
          }

        case array: Array[_] =>
          if (array.isEmpty) {
            ps.setNull(column, Types.ARRAY)
          } else {
            setArray(array, ps, column, cx)
          }

        case _ =>
          // this will almost certainly fail...
          super.setValue(value, binding, ps, column, cx)
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
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> b597cf01b8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
    } else {
      super.setValue(value, binding, ps, column, cx)
    }
  }

<<<<<<< HEAD
  // based on setArrayValue, but we don't have the attribute descriptor to use
  private def setArray(array: Array[_], ps: PreparedStatement, column: Int, cx: Connection): Unit = {
    val componentType = array(0).getClass
    val sqlType = dataStore.getSqlTypeNameToClassMappings.asScala.collectFirst { case (k, v) if v == componentType => k }
    val componentTypeName = sqlType.getOrElse {
      throw new java.sql.SQLException(s"Failed to find a SQL type for $componentType")
    }
    ps.setArray(column, super.convertToArray(array, componentTypeName, componentType, cx))
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
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1dae86c846 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
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
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> b597cf01b8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
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
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
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
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 1dae86c846 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
  override protected def convertToArray(
      value: Any, componentTypeName: String, componentType: Class[_], connection: Connection): java.sql.Array = {
    val array = value match {
      case list: java.util.List[_] => list.toArray()
      case _ => value
    }
    super.convertToArray(array, componentTypeName, componentType, connection)
  }

  // fix bug with PostGISPSDialect dialect not delegating these methods
<<<<<<< HEAD
  override def encodeCreateTable(sql: StringBuffer): Unit = delegate.encodeCreateTable(sql)
<<<<<<< HEAD
  override def getDefaultVarcharSize: Int = delegate.getDefaultVarcharSize
  override def encodeTableName(raw: String, sql: StringBuffer): Unit = delegate.encodeTableName(raw, sql)
  override def encodePostCreateTable(tableName: String, sql: StringBuffer): Unit =
    delegate.encodePostCreateTable(tableName, sql)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  override def postCreateAttribute(att: AttributeDescriptor, tableName: String, schemaName: String, cx: Connection): Unit =
    delegate.postCreateAttribute(att, tableName, schemaName, cx)
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)

  override def getDefaultVarcharSize: Int = delegate.getDefaultVarcharSize
  override def encodeTableName(raw: String, sql: StringBuffer): Unit = delegate.encodeTableName(raw, sql)
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
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
  override def getDefaultVarcharSize: Int = delegate.getDefaultVarcharSize
  override def encodeTableName(raw: String, sql: StringBuffer): Unit = delegate.encodeTableName(raw, sql)
  override def encodePostCreateTable(tableName: String, sql: StringBuffer): Unit =
    delegate.encodePostCreateTable(tableName, sql)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======

  override def getDefaultVarcharSize: Int = delegate.getDefaultVarcharSize
  override def encodeTableName(raw: String, sql: StringBuffer): Unit = delegate.encodeTableName(raw, sql)
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
  override def postCreateFeatureType(
      featureType: SimpleFeatureType,
      metadata: DatabaseMetaData,
      schemaName: String,
      cx: Connection): Unit = {
    delegate.postCreateFeatureType(featureType, metadata, schemaName, cx)
  }
  override def splitFilter(filter: Filter, schema: SimpleFeatureType): Array[Filter] =
    delegate.splitFilter(filter, schema)
  override def getDesiredTablesType: Array[String] = delegate.getDesiredTablesType
  override def encodePostColumnCreateTable(att: AttributeDescriptor, sql: StringBuffer): Unit =
    delegate.encodePostColumnCreateTable(att, sql)
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
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
=======
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> b597cf01b8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
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
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
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
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)

object PartitionedPostgisPsDialect {

  // uses eq on the prepared statement to ensure that we compute json fields exactly once per prepared statement/col
  class PreparedStatementKey(val ps: PreparedStatement, val column: Int) {

    override def equals(other: Any): Boolean = {
      other match {
        case that: PreparedStatementKey => ps.eq(that.ps) && column == that.column
        case _ => false
      }
    }

    override def hashCode(): Int = {
      val state = Seq(ps, column)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }
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
=======
>>>>>>> b597cf01b8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
=======
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
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
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
=======
>>>>>>> 9d3260f1dc (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> cf1d94c7a (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 3a1e1d7213 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 1913bc4c2c (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
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
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 7542dc78d8 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 2912d58b06 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> b597cf01b8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> bf9e5cdd91 (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 45ed5ccca0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673bd (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 4b7c48b509 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> e9a70d8c07 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 47f8de1bb (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> afb207dc68 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 69a6e5986e (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
>>>>>>> eb44b0e44 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> f5a0fbbc4a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 9d0679fff9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
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
>>>>>>> e243573ba (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 230ae6c3ab (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> 789a0bdedd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> c3240dd4f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
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
>>>>>>> ee1d5f207 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 42af7673b (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 6d3c0ecb75 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 75e1524a30 (GEOMESA-3215 Postgis - support List-type attributes)
<<<<<<< HEAD
>>>>>>> 6edc66c3cb (GEOMESA-3215 Postgis - support List-type attributes)
=======
=======
=======
=======
>>>>>>> 8dc8f9c76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 61a93c41b7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 215bcb7385 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7e990c0c62 (GEOMESA-3215 Postgis - support List-type attributes)
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> ee1d5f2071 (GEOMESA-3215 Postgis - support List-type attributes)
>>>>>>> 36a7119501 (GEOMESA-3215 Postgis - support List-type attributes)
