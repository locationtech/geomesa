/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import com.codahale.metrics.{MetricRegistry, ScheduledReporter}
<<<<<<< HEAD
import org.apache.commons.lang3.StringUtils
=======
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> 53f954de5a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 526386abb5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbf1b71f0f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 80f2368efa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 642ed3a7de (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d99c7a3f48 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f5f39ce787 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b2289cfef2 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e76314e3b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c6133383ad (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c5713e03ac (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> ac98c35a97 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1b189468f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8efca642d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a55d483c70 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 82a732de33 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9562023e37 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fec2c8e10a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3c9714d337 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4a1c60c6cb (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> b49f469d0d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 19369edf30 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2ff4be6ae0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d45a7d3253 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1f4ba8cada (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c072dd070 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
=======
>>>>>>> a3d3894ba2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c539f59976 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b6e39cadd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4187c63b0a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0efcf5eade (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5975c5956d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 1214ead2d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b75842cda9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 98003811dc (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 88b1286e50 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3779af1b1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a2271134e7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33ee0dcc98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 07dc332b23 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3aa9c41455 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 27985b5bda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 629b3dcaee (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4c6e055332 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e129f5fdde (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 76e9fb4d4e (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ec61d87914 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1565efe823 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> da305c5a5c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ee323ba58e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4433141fd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e46274e0fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c06e05611f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6dc9f2206a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0b7ab4d1d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f7568f34f6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 277e197907 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c03f44fb78 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a3091a8e4d (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 97b63ea477 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> ac87f36639 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b74b73e7b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d094b6fee5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e757e297e4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad3e1f5098 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4327d984b1 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2e7fcc16b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 09b0e1c568 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71df28d69e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fa583ae8fc (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8a86778ccd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 003580e799 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33f0b95d6b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5aa646c0e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cdb70d217a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
=======
=======
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 918483049d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 003580e799 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33f0b95d6b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5aa646c0e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cdb70d217a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CopyOnWriteArrayList, ScheduledExecutorService, SynchronousQueue, TimeUnit}
import java.util.{Collections, Date}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 885a095ca8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33f0b95d6b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5aa646c0e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2c7572a737 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cdb70d217a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cdb70d217a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
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
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
import com.typesafe.scalalogging.LazyLogging
<<<<<<< HEAD
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6e1d4eedb3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d40f742b4 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> c5713e03ac (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
=======
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac98c35a97 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
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
<<<<<<< HEAD
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
import com.typesafe.scalalogging.LazyLogging
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c1b189468f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3779af1b1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec61d87914 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2b74b73e7b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2ff4be6ae0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1f4ba8cada (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
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
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
import com.typesafe.scalalogging.LazyLogging
<<<<<<< HEAD
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5c072dd070 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d40f742b4 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> 98003811dc (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 88b1286e50 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3779af1b1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cdb70d217a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
import com.typesafe.scalalogging.LazyLogging
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ec61d87914 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> da305c5a5c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 5a56b37afd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 003580e799 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2fce647015 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> locationtech-main
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
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
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
import com.typesafe.scalalogging.LazyLogging
<<<<<<< HEAD
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 918483049d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ee323ba58e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d40f742b4 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
<<<<<<< HEAD
>>>>>>> 97b63ea477 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac87f36639 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2b74b73e7b (GEOMESA-3100 Kafka layer views (#2784))
import kafka.admin.ConfigCommand.{ConfigEntity, Entity}
import kafka.zk.{AdminZkClient, KafkaZkClient}
=======
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CopyOnWriteArrayList, ScheduledExecutorService, SynchronousQueue, TimeUnit}
import java.util.{Collections, Date}
import com.typesafe.scalalogging.LazyLogging
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a9de98d0ef9 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e790dd8e090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e885404c30 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d9f163e0286 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d014210d195 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 89298e3f673 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 5c7acfc0f0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae76dae421d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ae1d315eb36 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 72207855ca1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 48ec36260be (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5c7acfc0f0e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> edcdd0e884a (GEOMESA-3100 Kafka layer views (#2784))
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
<<<<<<< HEAD
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
=======
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2e299e67fda (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> cc8792498f0 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> dbd50b37232 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 731097c4df2 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 6322070468d (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 701623c6ebe (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 091ec1b4492 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> ebb4618f942 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 68983f47120 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 6f7176740a5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 1839e81c6d0 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> c410a5ed858 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> d554be9eacb (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> f150e02929a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> d3d37de3843 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 4c974f977f1 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> f727c17f742 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 7542da1d561 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> d274fab9ad4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 20d74aab649 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 7bdaa949cf8 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 16cb2e76153 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 8f7cac8cddd (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 3705c7bd982 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 1d4c5d90398 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 56bf4670d76 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> f3e013e5777 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 217b581009a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 0712c13e235 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 47123b948e2 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> c49f59fd6cb (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 51d82ba56dc (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 7615bb618d7 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> b39e2e8c511 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 226e011e7a9 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> 82d9adf0d2a (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
<<<<<<< HEAD
>>>>>>> 06f58f17265 (GEOMESA-3198 Kafka streams integration (#2854))
=======
<<<<<<< HEAD
>>>>>>> f89e1065ec8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
import org.apache.kafka.common.config.ConfigResource
import org.geotools.api.data._
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
=======
import org.apache.kafka.clients.producer.KafkaProducer
=======
>>>>>>> e68704b1b09 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 9fa04264896 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 8362c3a15dd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 937e9b2999b (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 5ef1a7e6d88 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> a49c83a3f50 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0853537f254 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> bbc112f0c15 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 8c5addb8623 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 82a76f7b2df (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f39b806cb46 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 1e65dacfec8 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> bf318c77551 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 52f029b3d50 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2b39d6794f8 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 4d7b088edb1 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 7355805fa9c (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 6a74b393cd4 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 0141c7f7075 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> bb929d9908f (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> b0b6514b0dd (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 66fa76f2d3d (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 09be37fcacc (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> a568373df6f (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> a9c1c6485f2 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 3c90a54e92c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> be18eccae24 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> cce02a8d0dc (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 01a7662cce9 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 3e59ae821ce (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> f632e7baa6e (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 630e9966018 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 2c655b5973b (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> faa1be96ea1 (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 80d12815adc (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
import org.apache.kafka.clients.producer.KafkaProducer
>>>>>>> 8c2f3af3c0c (GEOMESA-3198 Kafka streams integration (#2854))
=======
>>>>>>> 95fc59976cb (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
import org.apache.kafka.common.utils.Time
>>>>>>> 484744b8297 (GEOMESA-3198 Kafka streams integration (#2854))
import org.geotools.data._
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SerializationType}
import org.locationtech.geomesa.index.InMemoryMetadata
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.metadata.TableBasedMetadata
import org.locationtech.geomesa.kafka.ExpirationMocking.{MockTicker, ScheduledExpiry, WrappedRunnable}
import org.locationtech.geomesa.kafka.KafkaContainerTest
import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult
import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult.BatchResult
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import org.locationtech.geomesa.kafka.utils.GeoMessageSerializer.GeoMessageSerializerFactory
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 1b7313570b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 642ed3a7de (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> f71ad77609 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f5f39ce787 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> ac2d6b46dd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c6133383ad (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> b783b46726 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9562023e37 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 4081d7e9a7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 3c9714d337 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 5c37a71382 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 19369edf30 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> bcebc6d295 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d45a7d3253 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 8400833da0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9b6e39cadd (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 3ebcf23622 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0efcf5eade (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 46b8c593f5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b75842cda9 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 72af818f67 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 3aa9c41455 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 4a3b715000 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 629b3dcaee (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 7b21c1aca3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 76e9fb4d4e (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> b10234cc9b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1565efe823 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 187b0fd973 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6dc9f2206a (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 9611d960b6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7568f34f6 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> be9e6b5c0a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a3091a8e4d (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 33148ba22e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4327d984b1 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> 3b7450e7b5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 09b0e1c568 (Merge branch 'feature/postgis-fixes')
=======
import org.locationtech.geomesa.kafka.data.KafkaDataStoreFactory.KafkaDataStoreFactoryParams
>>>>>>> a3ef940730 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.{KafkaFeatureChanged, KafkaFeatureCleared, KafkaFeatureRemoved}
import org.locationtech.geomesa.kafka.utils.{GeoMessage, GeoMessageProcessor}
import org.locationtech.geomesa.metrics.core.GeoMesaMetrics
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityUtils}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.index.SizeSeparatedBucketIndex
import org.locationtech.geomesa.utils.io.{CloseQuietly, WithClose}
import org.locationtech.jts.geom.Point
import org.mockito.ArgumentMatchers
import org.specs2.mock.Mockito
import org.specs2.runner.JUnitRunner

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CopyOnWriteArrayList, ScheduledExecutorService, SynchronousQueue, TimeUnit}
import java.util.{Collections, Date, Properties}

@RunWith(classOf[JUnitRunner])
class KafkaDataStoreTest extends KafkaContainerTest with Mockito {

  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  lazy val baseParams = Map(
//    "kafka.serialization.type" -> "avro",
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
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
    "kafka.brokers"            -> brokers,
=======
    "kafka.brokers"            -> kafka.brokers,
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
    "kafka.brokers"            -> brokers,
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
    "kafka.brokers"            -> brokers,
=======
    "kafka.brokers"            -> kafka.brokers,
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
    "kafka.brokers"            -> brokers,
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
    "kafka.brokers"            -> brokers,
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
    "kafka.topic.partitions"   -> 1,
    "kafka.topic.replication"  -> 1,
    "kafka.consumer.read-back" -> "Inf"
  )

  val gf = JTSFactoryFinder.getGeometryFactory
  val paths = new AtomicInteger(0)

  def getUniquePath: String = s"geomesa/${paths.getAndIncrement()}/test/"

  def getStore(zkPath: String, consumers: Int, extras: Map[String, AnyRef] = Map.empty): KafkaDataStore = {
    val catalog = if (extras.contains("kafka.zookeepers")) { KafkaDataStoreParams.ZkPath } else { KafkaDataStoreParams.Catalog }
    val params = baseParams ++ Map(catalog.key -> zkPath, "kafka.consumer.count" -> consumers) ++ extras
    DataStoreFinder.getDataStore(params.asJava).asInstanceOf[KafkaDataStore]
  }

  def createStorePair(params: Map[String, AnyRef] = Map.empty): (KafkaDataStore, KafkaDataStore, SimpleFeatureType) = {
    // note: the topic gets set in the user data, so don't re-use the same sft instance
    val sft = SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
    val path = getUniquePath
    (getStore(path, 0, params), getStore(path, 1, params), sft)
  }

  "KafkaDataStore" should {

    "return correctly from canProcess" >> {
      import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams._
      val factory = new KafkaDataStoreFactory
      factory.canProcess(Collections.emptyMap[String, java.io.Serializable]) must beFalse
      factory.canProcess(Map[String, java.io.Serializable](Brokers.key -> "test", Zookeepers.key -> "test").asJava) must beTrue
    }

    "handle old read-back params" >> {
      val deprecated = Seq(
        "autoOffsetReset" -> "earliest",
        "autoOffsetReset" -> "latest",
        "kafka.consumer.from-beginning" -> "true",
        "kafka.consumer.from-beginning" -> "false"
      )
      foreach(deprecated) { case (k, v) =>
        KafkaDataStoreParams.ConsumerReadBack.lookupOpt(Collections.singletonMap(k, v)) must not(throwAn[Exception])
      }
    }

    "create unique topics based on zkPath" >> {
      val path = s"geomesa/topics/test/${paths.getAndIncrement()}"
      val ds = getStore(path, 0)
      try {
        ds.createSchema(SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326"))
        ds.getSchema("kafka").getUserData.get(KafkaDataStore.TopicKey) mustEqual s"$path-kafka".replaceAll("/", "-")
        ds.getSchema("kafka").getUserData.get(KafkaDataStore.PartitioningKey) mustEqual KafkaDataStore.PartitioningDefault
      } finally {
        ds.dispose()
      }
    }

    "use default kafka partitioning" >> {
      val path = s"geomesa/topics/test/${paths.getAndIncrement()}"
      val ds = getStore(path, 0)
      try {
        ds.createSchema(SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326"))
        KafkaDataStore.usesDefaultPartitioning(ds.getSchema("kafka")) must beTrue
      } finally {
        ds.dispose()
      }
    }

    "clean up metrics" >> {
      val reporter = mock[ScheduledReporter]
      val metrics = new GeoMesaMetrics(new MetricRegistry(), "", Seq(reporter))
      val config = {
        val orig = KafkaDataStoreFactory.buildConfig(baseParams.asJava)
        CloseQuietly(orig.metrics)
        orig.copy(metrics = Some(metrics))
      }
      val serializer = new GeoMessageSerializerFactory(SerializationType.KRYO)
      new KafkaDataStore(config, new InMemoryMetadata[String](), serializer).dispose()

      there was one(reporter).close()
    }

    "use namespaces" >> {
      import org.locationtech.geomesa.kafka.data.KafkaDataStoreParams._
      val path = s"geomesa/namespace/test/${paths.getAndIncrement()}"
      val ds = getStore(path, 0, Map(NamespaceParam.key -> "ns0"))
      try {
        ds.createSchema(SimpleFeatureTypes.createType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326"))
        ds.getSchema("kafka").getName.getNamespaceURI mustEqual "ns0"
        ds.getSchema("kafka").getName.getLocalPart mustEqual "kafka"
      } finally {
        ds.dispose()
      }
      val ds2 = getStore(path, 0, Map(NamespaceParam.key -> "ns1"))
      try {
        ds2.getSchema("kafka").getName.getNamespaceURI mustEqual "ns1"
        ds2.getSchema("kafka").getName.getLocalPart mustEqual "kafka"
      } finally {
        ds2.dispose()
      }
    }

    "allow schemas to be created and deleted" >> {
      foreach(Seq(true, false)) { zk =>
        TableBasedMetadata.Expiry.threadLocalValue.set("10ms")
        val (producer, consumer, _) = try {
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
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
          createStorePair(if (zk) { Map("kafka.zookeepers" -> zookeepers) } else { Map.empty[String, String] })
=======
          createStorePair(if (zk) { Map("kafka.zookeepers" -> kafka.zookeepers) } else { Map.empty[String, String] })
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
          createStorePair(if (zk) { Map("kafka.zookeepers" -> zookeepers) } else { Map.empty[String, String] })
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
          createStorePair(if (zk) { Map("kafka.zookeepers" -> zookeepers) } else { Map.empty[String, String] })
=======
          createStorePair(if (zk) { Map("kafka.zookeepers" -> kafka.zookeepers) } else { Map.empty[String, String] })
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
          createStorePair(if (zk) { Map("kafka.zookeepers" -> zookeepers) } else { Map.empty[String, String] })
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
          createStorePair(if (zk) { Map("kafka.zookeepers" -> zookeepers) } else { Map.empty[String, String] })
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
        } finally {
          TableBasedMetadata.Expiry.threadLocalValue.remove()
        }
        consumer must not(beNull)
        producer must not(beNull)
        try {
          val sft = SimpleFeatureTypes.createImmutableType("kafka", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.foo='bar'")
          val topic = s"${producer.config.catalog}-${sft.getTypeName}".replaceAll("/", "-")
          producer.createSchema(sft)
          consumer.metadata.resetCache()
          foreach(Seq(producer, consumer)) { ds =>
            ds.getTypeNames.toSeq mustEqual Seq(sft.getTypeName)
            val schema = ds.getSchema(sft.getTypeName)
            schema must not(beNull)
            schema mustEqual sft
            schema.getUserData.get("geomesa.foo") mustEqual "bar"
            schema.getUserData.get(KafkaDataStore.TopicKey) mustEqual topic
          }

          val props = Collections.singletonMap[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
          WithClose(AdminClient.create(props)) { admin =>
            admin.listTopics().names().get.asScala must contain(topic)
          }
          consumer.removeSchema(sft.getTypeName)
          foreach(Seq(consumer, producer)) { ds =>
            eventually(40, 100.millis)(ds.getTypeNames.toSeq must beEmpty)
            ds.getSchema(sft.getTypeName) must beNull
          }
          WithClose(AdminClient.create(props)) { admin =>
            eventually(40, 100.millis)(admin.listTopics().names().get.asScala must not(contain(topic)))
          }
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "write/update/read/delete features" >> {
      foreach(Seq(true, false)) { cqEngine =>
        val params = if (cqEngine) {
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
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> kafka.zookeepers)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> kafka.zookeepers)
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
        } else {
          Map.empty[String, String]
        }
        val (producer, consumer, sft) = createStorePair(params)
        try {
          producer.createSchema(sft)
          consumer.metadata.resetCache()
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
          val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must containTheSameElementsAs(Seq(f0, f1)))

          // update
          val f2 = ScalaSimpleFeature.create(sft, "sm", "smith2", 32, "2017-01-01T00:00:02.000Z", "POINT (2 2)")
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            FeatureUtils.write(writer, f2, useProvidedFid = true)
          }
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must containTheSameElementsAs(Seq(f1, f2)))

          // query
          val queries = Seq(
            "strToUpperCase(name) = 'JONES'",
            "name = 'jones' OR name = 'smith'",
            "name = 'foo' OR name = 'bar' OR name = 'baz' OR name = 'blarg' OR name = 'jones' OR name = 'smith'",
            "name = 'jones'",
            "age < 25",
            "bbox(geom, -15, -15, -5, -5) AND age < 25",
            "bbox(geom, -15, -15, 5, 5) AND dtg DURING 2017-01-01T12:00:00.000Z/2017-01-02T12:00:00.000Z",
            "INTERSECTS(geom, POLYGON((-11 -11, -9 -11, -9 -9, -11 -9, -11 -11))) AND bbox(geom, -15, -15, 5, 5)"
          )

          forall(queries) { ecql =>
            val query = new Query(sft.getTypeName, ECQL.toFilter(ecql))
            val features = SelfClosingIterator(consumer.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq
            features mustEqual Seq(f1)
          }

          // delete
          producer.getFeatureSource(sft.getTypeName).removeFeatures(ECQL.toFilter("IN('sm')"))
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must beEqualTo(Seq(f1)))

          // clear
          producer.getFeatureSource(sft.getTypeName).removeFeatures(Filter.INCLUDE)
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must beEmpty)
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "write/read with visibilities" >> {
      import org.locationtech.geomesa.security.AuthProviderParam

      foreach(Seq(true, false)) { cqEngine =>
        var auths: Set[String] = null
        val provider = new AuthorizationsProvider() {
          import scala.collection.JavaConverters._
          override def getAuthorizations: java.util.List[String] = auths.toList.asJava
          override def configure(params: java.util.Map[String, _]): Unit = {}
        }
        val params = if (cqEngine) {
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
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> kafka.zookeepers)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> kafka.zookeepers)
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
          Map("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
        } else {
          Map.empty[String, String]
        }
        val (producer, consumer, sft) = createStorePair(params + (AuthProviderParam.key -> provider))
        try {
          producer.createSchema(sft)
          consumer.metadata.resetCache()
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
          f0.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "USER")
          val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")
          f1.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "USER&ADMIN")

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }

          val q = new Query(sft.getTypeName)
          q.getHints.put(QueryHints.EXACT_COUNT, java.lang.Boolean.TRUE)

          // admin user
          auths = Set("USER", "ADMIN")
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must containTheSameElementsAs(Seq(f0, f1)))
          store.getCount(q) mustEqual 2

          // regular user
          auths = Set("USER")
          SelfClosingIterator(store.getFeatures.features).toSeq mustEqual Seq(f0)
          store.getCount(q) mustEqual 1

          // unauthorized
          auths = Set.empty
          SelfClosingIterator(store.getFeatures.features).toSeq must beEmpty
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "require visibilities on write" >> {
      val (producer, consumer, sft) = createStorePair()
      try {
        sft.getUserData.put(Configs.RequireVisibility, "true")
        producer.createSchema(sft)
        consumer.metadata.resetCache()

        val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
        val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true)) must throwAn[IllegalArgumentException]
          f0.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "USER")
          f1.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "USER&ADMIN")
          Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true)) must not(throwAn[Exception]) // ok
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "write/read json array attributes" >> {
      val sft = SimpleFeatureTypes.createType("kafka", "name:String:json=true,age:Int,dtg:Date,*geom:Point:srid=4326")
      val path = getUniquePath
      val (producer, consumer) = (getStore(path, 0), getStore(path, 1))
      try {
        producer.createSchema(sft)
        consumer.metadata.resetCache()
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d0df7c5dc2 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
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
=======
=======
<<<<<<< HEAD
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f8a4dfe0b3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 05825c803f (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d0df7c5dc2 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
        val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

        val f0 = ScalaSimpleFeature.create(sft, "sm", "[\"smith1\",\"smith2\"]", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
        val f1 = ScalaSimpleFeature.create(sft, "jo", "[\"jones\"]", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")

        // initial write
        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
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
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d0df7c5dc2 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
        val q = new Query(sft.getTypeName)
<<<<<<< HEAD
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        val q = new Query(sft.getTypeName)
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 8dc8f9c76d (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        val q = new Query(sft.getTypeName)
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> ce0f6336d5 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 95fa3152e7 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
        val q = new Query(sft.getTypeName)
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> bbcfc938d3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
=======
=======
        val q = new Query(sft.getTypeName)
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
=======
=======
        val q = new Query(sft.getTypeName)
<<<<<<< HEAD
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f8a4dfe0b3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 05825c803f (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d0df7c5dc2 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
        eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must
            containTheSameElementsAs(Seq(f0, f1)))
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "write/read avro collection attributes" >> {
      foreach(KafkaDataStoreParams.SerializationTypes.Types) { serde =>
        val params = Map(KafkaDataStoreParams.SerializationType.key -> serde)
        val sft =
          SimpleFeatureTypes.createType(
            "kafka",
            "names:List[String],props:Map[String,String],uuid:UUID,dtg:Date,*geom:Point:srid=4326")
        val path = getUniquePath
        val (producer, consumer) = (getStore(path, 0, params), getStore(path, 1, params))
        try {
          producer.createSchema(sft)
          consumer.metadata.resetCache()
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val f0 =
            ScalaSimpleFeature.create(
              sft,
              "sm",
              List("smith1", "smith2"),
              Map("s" -> "smith"),
              "8e619e92-e894-4553-b65d-ce65681a75f4",
              "2017-01-01T00:00:00.000Z",
              "POINT (0 0)")
          val f1 =
            ScalaSimpleFeature.create(
              sft,
              "jo",
              List("jones"),
              Map("j1" -> "jones1", "j2" -> "jones2"),
              "d6505c88-c5ea-4bb3-99d7-26af5b531eda",
              "2017-01-02T00:00:00.000Z",
              "POINT (-10 -10)")

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }

          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must
              containTheSameElementsAs(Seq(f0, f1)))
        } finally {
          consumer.dispose()
          producer.dispose()
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
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d0df7c5dc2 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
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
=======
=======
<<<<<<< HEAD
>>>>>>> 865887e960 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
<<<<<<< HEAD
>>>>>>> f8a4dfe0b3 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 865887e96 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> e243573ba4 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
>>>>>>> 05825c803f (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d0df7c5dc2 (GEOMESA-3217,GEOMESA-3216 Support Postgis json attributes, top-level arrays in json)
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
    "expire entries" >> {
      foreach(Seq(true, false)) { cqEngine =>
        val executor = mock[ScheduledExecutorService]
        val ticker = new MockTicker()
        val params = if (cqEngine) {
          Map("kafka.cache.expiry" -> "100ms",
            "kafka.cache.executor" -> (executor, ticker),
            "kafka.index.cqengine" -> "geom:default,name:unique",
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
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
            "kafka.zookeepers" -> zookeepers)
=======
            "kafka.zookeepers" -> kafka.zookeepers)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
            "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
            "kafka.zookeepers" -> zookeepers)
=======
            "kafka.zookeepers" -> kafka.zookeepers)
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
            "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
            "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
        } else {
          Map("kafka.cache.expiry" -> "100ms", "kafka.cache.executor" -> (executor, ticker))
        }
        val (producer, consumer, sft) = createStorePair(params)
        try {
          producer.createSchema(sft)
          consumer.metadata.resetCache()
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
          val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")

          val bbox = ECQL.toFilter("bbox(geom,-10,-10,10,10)")

          val expirations = Collections.synchronizedList(new java.util.ArrayList[WrappedRunnable](2))
          executor.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.anyLong(), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
            val expire = new WrappedRunnable(0L)
            expire.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
            expirations.add(expire)
            new ScheduledExpiry(expire)
          }

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }
          // check the cache directly
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must
              containTheSameElementsAs(Seq(f0, f1)))
          // check the spatial index
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures(bbox).features).toSeq must
              containTheSameElementsAs(Seq(f0, f1)))

          // expire the cache
          expirations.asScala.foreach(_.runnable.run())

          // verify feature has expired - hit the cache directly
          SelfClosingIterator(store.getFeatures.features) must beEmpty
          // verify feature has expired - hit the spatial index
          SelfClosingIterator(store.getFeatures(bbox).features) must beEmpty
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "expire entries based on cql filters" >> {
      foreach(Seq(true, false)) { cqEngine =>
        val executor = mock[ScheduledExecutorService]
        val ticker = new MockTicker()
        val params = {
          val expiry =
            """{
               |"name = 'smith'": "100ms",
               |"name = 'jones'": "200ms"
               |}""".stripMargin
          val base = Map(
            "kafka.cache.expiry.dynamic" -> expiry,
            "kafka.cache.expiry"         -> "300ms",
            "kafka.cache.executor"       -> (executor, ticker)
          )
          if (cqEngine) {
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
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
            base + ("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
=======
            base + ("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> kafka.zookeepers)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
            base + ("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
            base + ("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
=======
            base + ("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> kafka.zookeepers)
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
            base + ("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
            base + ("kafka.index.cqengine" -> "geom:default,name:unique", "kafka.zookeepers" -> zookeepers)
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
          } else {
            base
          }
        }
        val (producer, consumer, sft) = createStorePair(params)
        try {
          producer.createSchema(sft)
          consumer.metadata.resetCache()
          val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

          val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
          val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")
          val f2 = ScalaSimpleFeature.create(sft, "wi", "wilson", 10, "2017-01-03T00:00:00.000Z", "POINT (10 10)")

          val bbox = ECQL.toFilter("bbox(geom,-10,-10,10,10)")

          val expirations = Collections.synchronizedList(new java.util.ArrayList[WrappedRunnable](2))

          // test the first filter expiry
          executor.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
            val expire = new WrappedRunnable(0L)
            expire.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
            expirations.add(expire)
            new ScheduledExpiry(expire)
          }

          // initial write
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f0).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }
          // check the cache directly
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must
              containTheSameElementsAs(Seq(f0)))
          // check the spatial index
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures(bbox).features).toSeq must
              containTheSameElementsAs(Seq(f0)))

          there was one(executor).schedule(ArgumentMatchers.eq(expirations.get(0).runnable), ArgumentMatchers.eq(100L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

          // test the second filter expiry
          executor.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(200L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
            val expire = new WrappedRunnable(0L)
            expire.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
            expirations.add(expire)
            new ScheduledExpiry(expire)
          }

          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }

          // check the cache directly
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must
              containTheSameElementsAs(Seq(f0, f1)))
          // check the spatial index
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures(bbox).features).toSeq must
              containTheSameElementsAs(Seq(f0, f1)))

          there was one(executor).schedule(ArgumentMatchers.eq(expirations.get(1).runnable), ArgumentMatchers.eq(200L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

          // test the fallback expiry
          executor.schedule(ArgumentMatchers.any[Runnable](), ArgumentMatchers.eq(300L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS)) responds { args =>
            val expire = new WrappedRunnable(0L)
            expire.runnable = args.asInstanceOf[Array[AnyRef]](0).asInstanceOf[Runnable]
            expirations.add(expire)
            new ScheduledExpiry(expire)
          }

          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            Seq(f2).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }

          // check the cache directly
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must
              containTheSameElementsAs(Seq(f0, f1, f2)))
          // check the spatial index
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures(bbox).features).toSeq must
              containTheSameElementsAs(Seq(f0, f1, f2)))

          there was one(executor).schedule(ArgumentMatchers.eq(expirations.get(2).runnable), ArgumentMatchers.eq(300L), ArgumentMatchers.eq(TimeUnit.MILLISECONDS))

          // expire the cache
          expirations.asScala.foreach(_.runnable.run())

          // verify feature has expired - hit the cache directly
          SelfClosingIterator(store.getFeatures.features) must beEmpty
          // verify feature has expired - hit the spatial index
          SelfClosingIterator(store.getFeatures(bbox).features) must beEmpty
        } finally {
          consumer.dispose()
          producer.dispose()
        }
      }
    }

    "clear on startup" >> {
      val params = Map("kafka.producer.clear" -> "true")
      val (producer, consumer, sft) = createStorePair(params)
      try {
        producer.createSchema(sft)
        consumer.metadata.resetCache()
        val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

        val f0 = ScalaSimpleFeature.create(sft, "sm", "smith", 30, "2017-01-01T00:00:00.000Z", "POINT (0 0)")
        val f1 = ScalaSimpleFeature.create(sft, "jo", "jones", 20, "2017-01-02T00:00:00.000Z", "POINT (-10 -10)")
        val f2 = ScalaSimpleFeature.create(sft, "do", "doe", 40, "2017-01-03T00:00:00.000Z", "POINT (10 10)")

        // initial write
        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          Seq(f0, f1).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
        }
        eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq must containTheSameElementsAs(Seq(f0, f1)))

        // new producer - clears on startup
        val producer2 = getStore(producer.config.catalog, 0, params)
        try {
          // write the third feature
          WithClose(producer2.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            FeatureUtils.write(writer, f2, useProvidedFid = true)
          }
          eventually(40, 100.millis)(SelfClosingIterator(store.getFeatures.features).toSeq mustEqual Seq(f2))
        } finally {
          producer2.dispose()
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support listeners" >> {
      val (producer, consumer, sft) = createStorePair()
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        consumer.metadata.resetCache()
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair(params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        consumer.metadata.resetCache()
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
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
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fea30ed215 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8cd6d6241d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 985a302623 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e1c99f18f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
>>>>>>> 6972876138 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> fccb4dad62 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8cd6d6241d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c6f03ade07 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d1e5218220 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2083e85e99 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 6cf796911a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 115829ea19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1826b0963c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 0185d52d3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 985a302623 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5195730fc9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 5e702d2570 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
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
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 96066f1981 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1826b0963c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
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
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> a87bd189fb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7fa7ba70e4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8f1f84a66b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 0185d52d3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fea30ed215 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 8f1f84a66b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b621266505 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
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
=======
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9d0e85ae73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e1483489ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> a87bd189fb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b50c79638e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> afddec3996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cda8d2cfe5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 4835e26d82 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 629653f5fd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
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
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 252da2e91b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
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
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0185d52d3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
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
=======
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 640fe82450 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cda8d2cfe5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> dae3edbeaa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
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
=======
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> afddec3996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4835e26d82 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d8834ca541 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 247660d1e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5e702d2570 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
=======
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> afddec3996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d8834ca541 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 247660d1e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 5e702d2570 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
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
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 76054ef7c8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 247660d1e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 13d2a99a4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13d2a99a4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13d2a99a4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 70a1dcd662 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 740b573c96 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
<<<<<<< HEAD
=======
>>>>>>> ab7fea7977 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 76054ef7c8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 740b573c96 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
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
=======
>>>>>>> 207b1ea7fb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e0bad155c5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 40152ae834 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b05eef827c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 70a1dcd662 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> b6e06ebf5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b6e06ebf5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> cedd74041 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1cb34aff19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 835ba25ca9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> dbe55d85a5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 9a412492c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 4906df322a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> d7cf8eba5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c1afaaa1d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 82a3553e3d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 0185d52d3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fd19482a69 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 33e26d959a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> d7cf8eba5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2cbe9d9703 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> e94ef984bb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e1c99f18f0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 676a994502 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cda8d2cfe5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 6972876138 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 252da2e91b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6b0a6ab84b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 640fe82450 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> dae3edbeaa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bf20a66dbf (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 194ef285fa (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0ef6ebd072 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 23a67ee28d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8cc9f75926 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 03fc449fc3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6b217aa354 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fccb4dad62 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> def9ca3cde (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> befd97dfe9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 1c638ccdf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d7cf8eba56 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 8cc9f75926 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> d213e47a85 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 40152ae834 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> b6e06ebf5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d3cff0b4f8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 207b1ea7fb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b6e06ebf5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> cedd74041 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 1cb34aff19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> e0bad155c5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 835ba25ca9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> e1483489ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> afddec3996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a412492c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 4906df322a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ab7fea7977 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
>>>>>>> dbe55d85a5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c1afaaa1d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 82a3553e3d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ccbff33ea7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
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
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 0185d52d3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fd19482a69 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fea30ed215 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> def9ca3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 33e26d959a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 68ae69d6fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a87bd189fb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> e554a4e73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 555809484e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> addf81bcf8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 76054ef7c8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> d7cf8eba5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7b135ef4c9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2cbe9d9703 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 79241ee21c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> dbe55d85a5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 4b8c118ac (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> e94ef984bb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ee05a4e996 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7b135ef4c9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8cd6d6241d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> e236a49854 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7fa7ba70e4 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8f1f84a66b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> af2bf6769c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> b50c79638e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4835e26d82 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c6f03ade07 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 96066f1981 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b621266505 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9d0e85ae73 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 629653f5fd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 2083e85e99 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 6cf796911a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 1826b0963c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d8834ca541 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 03fc449fc3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d1e5218220 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 115829ea19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> e554a4e738 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 247660d1e5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 13d2a99a4f (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4b8c118ac7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 70a1dcd662 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 40152ae834 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b05eef827c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> b6e06ebf5 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> cedd74041 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5195730fc9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 1cb34aff19 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c2f23a900d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b05eef827c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> 7c0f257ce (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 5e702d2570 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 835ba25ca9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 15e3939cd7 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5195730fc9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> 46554dec2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 740b573c96 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 214797e54a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> b5fb52fc92 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5e702d2570 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
>>>>>>> 9a412492c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 4906df322a (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> fbfbc5fd7b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 740b573c96 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
    "support listeners without indexing" >> {
      val params = Map(KafkaDataStoreParams.CacheExpiry.getName -> "0s")
      val (producer, consumer, sft) = createStorePair("listenersNonIndexing", params)
      try {
        val id = "fid-0"
        val numUpdates = 1
        val maxLon = 80.0

        var latestLon = -1.0
        var count = 0

        val listener = new FeatureListener {
          override def changed(event: FeatureEvent): Unit = {
            val feature = event.asInstanceOf[KafkaFeatureChanged].feature
            feature.getID mustEqual id
            latestLon = feature.getDefaultGeometry.asInstanceOf[Point].getX
            count += 1
          }
        }

        producer.createSchema(sft)
        val consumerStore = consumer.getFeatureSource(sft.getTypeName)
        consumerStore.addFeatureListener(listener)

        WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          (numUpdates to 1 by -1).foreach { i =>
            val ll = maxLon - maxLon / i
            val sf = writer.next()
            sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
            sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
            sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
            writer.write()
          }
        }

        eventually(40, 100.millis)(count must beEqualTo(numUpdates))
        latestLon must be equalTo 0.0
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

<<<<<<< HEAD
>>>>>>> c1afaaa1d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 82a3553e3d (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 5132751c2c (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bfbaa0e3cd (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
=======
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
=======
=======
=======
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> 0185d52d3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> fd19482a69 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c02ba24c88 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 985a302623 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
    "support transactions" >> {
      val (producer, consumer, _) = createStorePair()
      try {
        val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
        producer.createSchema(sft)
        consumer.metadata.resetCache()

        val features = Seq.tabulate(10) { i =>
          ScalaSimpleFeature.create(sft, s"$i", s"name$i", i, f"2018-01-01T$i%02d:00:00.000Z", s"POINT (4$i 55)")
        }

        val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

        val ids = new CopyOnWriteArrayList[String]()

        val listener = new FeatureListener() {
          override def changed(event: FeatureEvent): Unit = {
            ids.add(event.asInstanceOf[KafkaFeatureChanged].feature.getID)
          }
        }

        store.addFeatureListener(listener)

        try {
          WithClose(new DefaultTransaction()) { transaction =>
            WithClose(producer.getFeatureWriterAppend(sft.getTypeName, transaction)) { writer =>
              features.take(2).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
              transaction.rollback()
              features.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
              transaction.commit()
            }
            eventually(40, 100.millis)(ids.asScala mustEqual Seq.tabulate(3)(_.toString))

            WithClose(producer.getFeatureWriterAppend(sft.getTypeName, transaction)) { writer =>
              features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
              transaction.commit()
            }

            eventually(40, 100.millis)(ids.asScala mustEqual Seq.tabulate(3)(_.toString) ++ Seq.tabulate(10)(_.toString))
          }
        } finally {
          store.removeFeatureListener(listener)
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support layer views" >> {
      val views =
        """{
          |  test = [
          |    { type-name = test2, filter = "dtg > '2018-01-01T05:00:00.000Z'", transform = [ "name", "dtg", "geom" ] }
          |    { type-name = test3, transform = [ "derived=strConcat(name,'-d')", "dtg", "geom" ] }
          |    { type-name = test4, filter = "dtg > '2018-01-01T05:00:00.000Z'" }
          |  ]
          |}
          |
          |""".stripMargin
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2fce647015 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 003580e799 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
=======
=======
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 885a095ca8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33f0b95d6b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5aa646c0e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2c7572a737 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> cdb70d217a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5a56b37afd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
      val (producer, consumer, _) = createStorePair("views", Map(KafkaDataStoreParams.LayerViews.key -> views))
=======
<<<<<<< HEAD
=======
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
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
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
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
>>>>>>> cf457d8543 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 846e61d5c9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cdb70d217a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cdb70d217a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> eecabb92a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 98e7bf59d2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
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
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fc4546247e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cdb70d217a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
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
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
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
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
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
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
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
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
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
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 885a095ca8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5a56b37afd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 003580e799 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2fce647015 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 33f0b95d6b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5aa646c0e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2c7572a737 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a0314fb7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8ed2c9f7e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> eddb4eaca1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cdb70d217a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 17f13b3a7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6069aba6b4 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
=======
      val (producer, consumer, _) = createStorePair("views", Map(KafkaDataStoreParams.LayerViews.key -> views))
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
=======
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2fce647015 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 003580e799 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d309fbbbbf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 885048dd7d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 64b99590af (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 13347c6405 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 885a095ca8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 33f0b95d6b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5aa646c0e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d64c097082 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2a9887a387 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2c7572a737 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 36fa236b19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a743856afa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4cbae2f6f9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa68dd43c6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950ef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c589b832a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fa68dd43c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89fb9950e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ffbc2ce32a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5a56b37afd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b5abc19ca4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 4480b2d1b9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 54ece16d1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> ffbc2ce32 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 626c8f9b93 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 54ece16d15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 73767737b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 34a22265fd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> f187a4fcf9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46457ea8f3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb4aa55ae3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 2df2640494 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdf (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d3b123117a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1b0e2fd343 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 88ef67cdfe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dd3eb3a6d (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 826636f69d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c57b5f538 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
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
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e230912b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d530f7b24a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 976f319530 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 121a1d3470 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 1e11ed51a6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fe3378ccaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
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
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f340d6feab (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
=======
<<<<<<< HEAD
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 39d28fffcb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> bc97b07a31 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 72ec0b2107 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c589b832a5 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1a5f68233 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
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
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 65a3a6e36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 429ffc55e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> e7f3e6588 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6f6887eb8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 42549e8e3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> e8cc4971c6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a743856afa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 9bda77cfe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> cb6bda89b6 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 26e5afc4ea (Merge branch 'feature/postgis-fixes')
=======
=======
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
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
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 69287f5812 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12fe94b860 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0e42c51dc0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 19aa61c0de (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f4b2369e9a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 305a9263eb (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> da00c7bd68 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
=======
>>>>>>> 984dd84de9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 770d928c15 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5dc1f86700 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 808743e257 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> ad0d29873e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0a0bf0292e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f43f8303d3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6368ee48e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4516873dab (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 34472778d3 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 64da8137f1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4b0dd3e513 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e453fd3d0 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> d76a495d83 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bab87f2458 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> bbc8576d19 (GEOMESA-3100 Kafka layer views (#2784))
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
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 42e8565e9b (Merge branch 'feature/postgis-fixes')
=======
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 66bdd8fd8d (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> f6e840b5dd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1c99a27679 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 92dc7a8411 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2766fc9b5b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> de8804123e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2f673a268a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c44e474495 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c2a94543ba (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 117b13aaaa (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 67de3c3202 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a94a1be83c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 0cd029fb3b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c61bffed00 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 12f226f36d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 89f0c9f2c3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 222e44653b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4f4ecaeb43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> b4cd5a7733 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 750ebbabe7 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> 638b68d081 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 65e544d20e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> cc8b9d0320 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> 02eb81c815 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c64e652363 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
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
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 30905e530b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 43ac4062d9 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
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
>>>>>>> 297ca0be9c (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 71adb695c5 (Merge branch 'feature/postgis-fixes')
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
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
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
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 64b99590af (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0d5f6377de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 64b99590af (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> f6e840b5d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
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
=======
>>>>>>> fb32a36e0e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9e433ba57e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 2750a5beef (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 41df6a2aa3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
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
>>>>>>> ea8384d9b7 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 23f312535d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 468da42855 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 340134cdda (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
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
>>>>>>> 79f56900f0 (GEOMESA-3100 Kafka layer views (#2784))
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
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
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
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb (Merge branch 'feature/postgis-fixes')
>>>>>>> 699117eca9 (Merge branch 'feature/postgis-fixes')
>>>>>>> c69897d7bd (Merge branch 'feature/postgis-fixes')
>>>>>>> 30559ea5c9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 13347c6405 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 64646d61ed (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> dcd54df39d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 885a095ca8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 0e1c98d0b5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2444ce9a2d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c4a3e8f36b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 9a5d6d2815 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a53bfd6caf (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fae9296238 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7a3c0ae880 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ec6d06b576 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 918483049d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5a56b37afd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 003580e799 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2fce647015 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 33f0b95d6b (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6a4d447614 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d01ebe8de2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d83cb8858c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5aa646c0e8 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 4c41429da9 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> d64c097082 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> adaa3bad65 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e47b308fed (GEOMESA-3100 Kafka layer views (#2784))
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
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> 6e0709aba8 (Merge branch 'feature/postgis-fixes')
=======
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 36fa236b19 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c36fdbc429 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 658596b211 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 2e052ef497 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5e33f21035 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 911c8eab1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c57b5f538a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 1f919641cb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c45703da7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b608d18cff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ed25decdd5 (Merge branch 'feature/postgis-fixes')
=======
=======
=======
>>>>>>> e1f939a3e (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> f3ae53295d (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 86104ff6cd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> 581f1dd15 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e24613dc4a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fed5922f34 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> dae1cf3cef (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 56f2dbb6ed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 50a71079c1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fc21067b13 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e910620b3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e09e0a77f2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4c325746bd (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7bbf2809a9 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3ebcdb99d9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 7cd6694c69 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fdf51402cf (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2e46425fed (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a377061090 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e148628c16 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> e944f1c88b (Merge branch 'feature/postgis-fixes')
>>>>>>> f893d9a594 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 06956ccf98 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 814854c5db (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a743856afa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 9236b0208 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c1d0543aac (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> be0c9d161a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 12d5933983 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 8b9e99d11a (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7bdc1feea8 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a7a764745e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 28427dfd8 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 614fcb6bc3 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 9b15c8e14c (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 407df12f59 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4231e686d (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 69e3273ac1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 3977ecc140 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 486c33f7ac (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> ac0357d36 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 04f82bbc57 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 495a1efc9f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5c7b9bc8ee (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> a62b956fb (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> e0773a1bd7 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 5d8c5baea0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4a73166192 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f47 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ae5eea67f1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 7628960f66 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 35673b8369 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
>>>>>>> 6af71ff43 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 964e86145a (Merge branch 'feature/postgis-fixes')
>>>>>>> 23a7d9849f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 416a95a4c0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 21d0f33e7c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
=======
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
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
      val (producer, consumer, _) = createStorePair(Map(KafkaDataStoreParams.LayerViews.key -> views))
>>>>>>> d845d7c1b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4cbae2f6f9 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 09d87762c5 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
=======
>>>>>>> d2002b5af (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d5f6f5af59 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 314efe4dee (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4cbae2f6f9 (GEOMESA-3100 Kafka layer views (#2784))
      try {
        val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
        producer.createSchema(sft)
        consumer.metadata.resetCache()
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
      val (producer, consumer, _) = createStorePair("views", Map(KafkaDataStoreParams.LayerViews.key -> views))
      try {
        val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
        producer.createSchema(sft)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 337ecd16e5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c860f21098 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 8ec26b8c2a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 3010fc384e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d4039e3d9a (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad362b1341 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 642891ac8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 5f3c172ab0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> fbff2623fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ccfb3bd95e (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 78d62931d0 (GEOMESA-3100 Kafka layer views (#2784))

        val sft2 = SimpleFeatureTypes.createType("test2", "name:String,dtg:Date,*geom:Point:srid=4326")
        val sft3 = SimpleFeatureTypes.createType("test3", "derived:String,dtg:Date,*geom:Point:srid=4326")
        val sft4 = SimpleFeatureTypes.createType("test4", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

        val features = Seq.tabulate(10) { i =>
          ScalaSimpleFeature.create(sft, s"$i", s"name$i", i, f"2018-01-01T$i%02d:00:00.000Z", s"POINT (4$i 55)")
        }
        val derived = Seq.tabulate(10) { i =>
          ScalaSimpleFeature.create(sft3, s"$i", s"name$i-d", f"2018-01-01T$i%02d:00:00.000Z", s"POINT (4$i 55)")
        }

        consumer.getTypeNames.toSeq must containTheSameElementsAs(Seq("test", "test2", "test3", "test4"))
        SimpleFeatureTypes.encodeType(consumer.getSchema("test2")) mustEqual SimpleFeatureTypes.encodeType(sft2)
        SimpleFeatureTypes.encodeType(consumer.getSchema("test3")) mustEqual SimpleFeatureTypes.encodeType(sft3)
        SimpleFeatureTypes.encodeType(consumer.getSchema("test4")) mustEqual SimpleFeatureTypes.encodeType(sft4)

        val store = consumer.getFeatureSource(sft.getTypeName) // start the consumer polling

        val ids = new CopyOnWriteArrayList[String]()

        val listener = new FeatureListener() {
          override def changed(event: FeatureEvent): Unit = {
            event match {
              case e: KafkaFeatureChanged => ids.add(e.feature.getID)
              case e: KafkaFeatureRemoved => ids.remove(e.id)
              case _: KafkaFeatureCleared => ids.clear()
              case _ => failure(s"Unexpected event: $event")
            }
          }
        }

        store.addFeatureListener(listener)

        try {
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
          }

          eventually(40, 100.millis)(ids.asScala mustEqual Seq.tabulate(10)(_.toString))
          SelfClosingIterator(consumer.getFeatureReader(new Query("test"), Transaction.AUTO_COMMIT)).toSeq must
            containTheSameElementsAs(features)
          SelfClosingIterator(consumer.getFeatureReader(new Query("test2"), Transaction.AUTO_COMMIT)).toSeq must
            containTheSameElementsAs(features.drop(6).map(ScalaSimpleFeature.retype(sft2, _)))
          SelfClosingIterator(consumer.getFeatureReader(new Query("test3"), Transaction.AUTO_COMMIT)).toSeq must
            containTheSameElementsAs(derived)
          SelfClosingIterator(consumer.getFeatureReader(new Query("test4"), Transaction.AUTO_COMMIT))
            containTheSameElementsAs(features.drop(6).map(ScalaSimpleFeature.retype(sft4, _)))

          val toRemove = ECQL.toFilter("IN('0','9')")
          WithClose(producer.getFeatureWriter(sft.getTypeName, toRemove, Transaction.AUTO_COMMIT)) { writer =>
            while(writer.hasNext) {
              writer.next()
              writer.remove()
            }
          }

          eventually(40, 100.millis)(ids.asScala mustEqual Seq.tabulate(10)(_.toString).slice(1, 9))
          SelfClosingIterator(consumer.getFeatureReader(new Query("test"), Transaction.AUTO_COMMIT)).toSeq must
            containTheSameElementsAs(features.slice(1, 9))
          SelfClosingIterator(consumer.getFeatureReader(new Query("test2"), Transaction.AUTO_COMMIT)).toSeq must
            containTheSameElementsAs(features.drop(6).dropRight(1).map(ScalaSimpleFeature.retype(sft2, _)))
          SelfClosingIterator(consumer.getFeatureReader(new Query("test3"), Transaction.AUTO_COMMIT)).toSeq must
            containTheSameElementsAs(derived.slice(1, 9))
          SelfClosingIterator(consumer.getFeatureReader(new Query("test4"), Transaction.AUTO_COMMIT)).toSeq must
            containTheSameElementsAs(features.drop(6).dropRight(1).map(ScalaSimpleFeature.retype(sft4, _)))

          producer.getFeatureSource(sft.getTypeName).removeFeatures(Filter.INCLUDE)
          eventually(40, 100.millis)(ids.asScala must beEmpty)
          SelfClosingIterator(consumer.getFeatureReader(new Query("test"), Transaction.AUTO_COMMIT)).toSeq must beEmpty
          SelfClosingIterator(consumer.getFeatureReader(new Query("test2"), Transaction.AUTO_COMMIT)).toSeq must beEmpty
          SelfClosingIterator(consumer.getFeatureReader(new Query("test3"), Transaction.AUTO_COMMIT)).toSeq must beEmpty
          SelfClosingIterator(consumer.getFeatureReader(new Query("test4"), Transaction.AUTO_COMMIT)).toSeq must beEmpty
        } finally {
          store.removeFeatureListener(listener)
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support at-least-once consumers" >> {
      skipped("inconsistent")
      val params = Map(
        KafkaDataStoreParams.ConsumerConfig.key -> "auto.offset.reset=earliest",
        KafkaDataStoreParams.ConsumerCount.key -> "2",
        KafkaDataStoreParams.TopicPartitions.key -> "2"
      )
      val (producer, consumer, sft) = createStorePair(params)
      try {
        val id = "fid-0"
        val numUpdates = 3
        val maxLon = 80.0

        val seen = new AtomicBoolean(false)
        val results = new CopyOnWriteArrayList[SimpleFeature]().asScala

        val processor = new GeoMessageProcessor() {
          override def consume(records: Seq[GeoMessage]): BatchResult = {
            if (!seen.get) {
              seen.set(true)
              BatchResult.Continue // this should cause the messages to be replayed
            } else {
              results ++= records.collect { case GeoMessage.Change(f) => f }
              BatchResult.Commit
            }
          }
        }

        producer.createSchema(sft)
        consumer.metadata.resetCache()

        def writeUpdates(): Unit = {
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            (numUpdates to 1 by -1).foreach { i =>
              val ll = maxLon - maxLon / i
              val sf = writer.next()
              sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
              sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(s"$id-$ll")
              sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
              writer.write()
            }
          }
        }

        WithClose(consumer.createConsumer(sft.getTypeName, "mygroup", processor)) { _ =>
          writeUpdates()
          eventually(seen.get must beTrue)
          eventually(results must haveLength(numUpdates))
        }

        // verify that we can read a second batch
        writeUpdates()
        WithClose(consumer.createConsumer(sft.getTypeName, "mygroup", processor)) { _ =>
          eventually(results must haveLength(numUpdates * 2))
        }
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "support pausing at-least-once consumers" >> {
      skipped("inconsistent")
      val params = Map(
        KafkaDataStoreParams.ConsumerConfig.key -> "auto.offset.reset=earliest",
        KafkaDataStoreParams.ConsumerCount.key -> "2",
        KafkaDataStoreParams.TopicPartitions.key -> "2"
      )
      val (producer, consumer, sft) = createStorePair(params)
      try {
        val id = "fid-0"
        val numUpdates = 3
        val maxLon = 80.0

        val in = new SynchronousQueue[Seq[SimpleFeature]]()
        val out = new SynchronousQueue[BatchResult]()

        val processor = new GeoMessageProcessor() {
          override def consume(records: Seq[GeoMessage]): BatchResult = {
            in.offer(records.collect { case GeoMessage.Change(f) => f }, 10, TimeUnit.SECONDS)
            Option(out.poll(10, TimeUnit.SECONDS)).getOrElse(BatchResult.Continue)
          }
        }

        producer.createSchema(sft)
        consumer.metadata.resetCache()

        def writeUpdates(): Unit = {
          WithClose(producer.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
            (numUpdates to 1 by -1).foreach { i =>
              val ll = maxLon - maxLon / i
              val sf = writer.next()
              sf.setAttributes(Array[AnyRef]("smith", Int.box(30), new Date(), s"POINT ($ll $ll)"))
              sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(s"$id-$ll")
              sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
              writer.write()
            }
          }
        }

        WithClose(consumer.createConsumer(sft.getTypeName, "mygroup", processor)) { _ =>
          writeUpdates()
          in.poll(10, TimeUnit.SECONDS) must haveLength(numUpdates)
          out.put(BatchResult.Pause)
          writeUpdates()
          in.poll(10, TimeUnit.SECONDS) must haveLength(numUpdates)
          foreach(0 until 10) { _ =>
            out.put(BatchResult.Pause)
            in.poll(10, TimeUnit.SECONDS) must haveLength(numUpdates)
          }
          out.put(BatchResult.Continue)
          eventually {
            val res = in.poll(10, TimeUnit.SECONDS)
            out.put(BatchResult.Continue)
            res must haveLength(numUpdates * 2)
          }
          in.poll(10, TimeUnit.SECONDS) must haveLength(numUpdates * 2)
          out.put(BatchResult.Commit)
          writeUpdates()
          in.poll(10, TimeUnit.SECONDS) must haveLength(numUpdates)
          out.put(BatchResult.Commit)
        }
        ok
      } finally {
        consumer.dispose()
        producer.dispose()
      }
    }

    "migrate old kafka data store schemas" >> {
      val spec = "test:String,dtg:Date,*location:Point:srid=4326"

      val path = s"geomesa/migrate/test/${paths.getAndIncrement()}"
      val client = CuratorFrameworkFactory.builder()
          .namespace(path)
          .connectString(zookeepers)
          .retryPolicy(new ExponentialBackoffRetry(1000, 3))
          .build()
      client.start()

      try {
        client.create.forPath("/test", s"$spec;geomesa.index.dtg=dtg".getBytes(StandardCharsets.UTF_8))
        client.create.forPath("/test/Topic", "test-topic".getBytes(StandardCharsets.UTF_8))

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
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
        val ds = getStore(path, 0, Map("kafka.zookeepers" -> zookeepers))
=======
        val ds = getStore(path, 0, Map("kafka.zookeepers" -> kafka.zookeepers))
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
        val ds = getStore(path, 0, Map("kafka.zookeepers" -> zookeepers))
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
        val ds = getStore(path, 0, Map("kafka.zookeepers" -> zookeepers))
=======
        val ds = getStore(path, 0, Map("kafka.zookeepers" -> kafka.zookeepers))
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
        val ds = getStore(path, 0, Map("kafka.zookeepers" -> zookeepers))
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 247707e7e5 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
        val ds = getStore(path, 0, Map("kafka.zookeepers" -> zookeepers))
>>>>>>> 0b203c6713 (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
>>>>>>> 4794e7a57e (GEOMESA-3258 Use docker instead of embedded Kafka for tests (#2957))
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
        try {
          ds.getTypeNames.toSeq mustEqual Seq("test")
          val sft = ds.getSchema("test")
          sft must not(beNull)
          KafkaDataStore.topic(sft) mustEqual "test-topic"
          SimpleFeatureTypes.encodeType(sft) mustEqual spec

          client.checkExists().forPath("/test") must beNull
        } finally {
          ds.dispose()
        }
      } finally {
        client.close()
      }
    }

    "configure topics by feature type" in {
      val ds = getStore(getUniquePath, 0)
      try {
        val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;")
        sft.getUserData.put("kafka.topic.config", "cleanup.policy=compact\nretention.ms=86400000")
        ds.createSchema(sft)
        val topic = KafkaDataStore.topic(ds.getSchema(sft.getTypeName))
        val props = new Properties()
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)

        WithClose(AdminClient.create(props)) { admin =>
          val configs =
            admin.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC, topic)))
          val config = configs.values().get(new ConfigResource(ConfigResource.Type.TOPIC, topic)).get()
          config must not(beNull)
          config.entries().asScala.map(e => e.name() -> e.value()).toMap must
              containAllOf(Seq("cleanup.policy" -> "compact", "retention.ms" -> "86400000"))
        }
      } finally {
        ds.dispose()
      }
    }

    "update compaction policy for catalog topics if not set" in {
      val props = new Properties()
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      val path = getUniquePath
      val topic = StringUtils.strip(path, " /").replace("/", "-")
      //Create the topic
      WithClose(AdminClient.create(props)) { admin =>
        val newTopic = new NewTopic(topic, 1, 1.toShort)
        admin.createTopics(Collections.singletonList(newTopic)).all().get
      }
      val ds = getStore(path, 0)
      try {
        ds.getTypeNames()
        //Verify the compaction policy
        WithClose(AdminClient.create(props)) { admin =>
          val configs =
            admin.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC, topic)))
          val config = configs.values().get(new ConfigResource(ConfigResource.Type.TOPIC, topic)).get()
          config must not(beNull)
          config.entries().asScala.map(e => e.name() -> e.value()).toMap must
            containAllOf(Seq("cleanup.policy" -> "compact"))
        }
      } finally {
        ds.dispose()
      }
    }

  }

  "KafkaDataStoreFactory" should {
    "clean zkPath" >> {
      def getNamespace(path: java.io.Serializable): String =
        KafkaDataStoreFactory.createZkNamespace(Map(KafkaDataStoreParams.ZkPath.getName -> path).asJava)

      // a well formed path starts does not start or end with a /
      getNamespace("foo/bar/baz") mustEqual "foo/bar/baz"
      getNamespace("foo/bar/baz/") mustEqual "foo/bar/baz" // trailing slash
      getNamespace("/foo/bar/baz") mustEqual "foo/bar/baz" // leading slash
      getNamespace("/foo/bar/baz/") mustEqual "foo/bar/baz" // both leading and trailing slash
      forall(Seq("/", "//", "", null))(n => getNamespace(n) mustEqual KafkaDataStoreFactory.DefaultZkPath) // empty
    }
    "Parse SSI tiers" >> {
      val key = KafkaDataStoreParams.IndexTiers.getName
      KafkaDataStoreFactory.parseSsiTiers(Collections.emptyMap()) mustEqual SizeSeparatedBucketIndex.DefaultTiers
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "foo")) mustEqual SizeSeparatedBucketIndex.DefaultTiers
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "1:2")) mustEqual Seq((1d, 2d))
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "1:2,3:4")) mustEqual Seq((1d, 2d), (3d, 4d))
      KafkaDataStoreFactory.parseSsiTiers(Collections.singletonMap(key, "3:4,1:2")) mustEqual Seq((1d, 2d), (3d, 4d))
    }
  }

  "KafkaFeatureSource" should {
    "handle Query instances with null TypeName (GeoServer querylayer extension implementation nuance)" >> {
      val (producer, consumer, sft) = createStorePair()
      try {
        producer.createSchema(sft)
        consumer.metadata.resetCache()
        val fs = consumer.getFeatureSource(sft.getTypeName)
        val q = new Query(null, Filter.INCLUDE)
        fs.getFeatures(q).features().close() must not(throwA[NullPointerException])
      } finally {
        producer.dispose()
        consumer.dispose()
      }
    }
  }
}
