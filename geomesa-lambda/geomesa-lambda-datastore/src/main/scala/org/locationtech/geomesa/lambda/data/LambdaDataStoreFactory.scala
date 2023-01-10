/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import org.geotools.api.data.DataAccessFactory.Param
import org.geotools.api.data.{DataStore, DataStoreFactorySpi}
=======
=======
>>>>>>> 0884e75348d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
=======
>>>>>>> 15b6bf02d15 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9b5b23eb090 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 2af63e167d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 97b68a5fbb8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7c6dac7c346 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 04ca02e264f (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5ba80a089cc (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
=======
>>>>>>> 29826bdce01 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f5016115b5a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 93e36893445 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 9f1e983c633 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> de3e5a3cc80 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bde70962716 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bf685aa0b4b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 37636fb3b99 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> e4edd3d6ceb (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
>>>>>>> e54506ef011 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
>>>>>>> 75ae649304a (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
>>>>>>> a104e87b93f (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a76720eebac (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
import java.awt.RenderingHints.Key
import java.io.Serializable
import java.time.Clock

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
>>>>>>> 69a1e5094b5 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStoreFactory, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreInfo, GeoMesaDataStoreParams}
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import java.awt.RenderingHints.Key
import java.time.Clock
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
import scala.reflect.ClassTag

class LambdaDataStoreFactory extends DataStoreFactorySpi {

  import LambdaDataStoreParams.{ClockParam, NamespaceParam}

  override def createDataStore(params: java.util.Map[String, _]): DataStore = {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
    // TODO GEOMESA-1891 attribute level vis
    val persistence = new AccumuloDataStoreFactory().createDataStore(LambdaDataStoreFactory.filter(params))
    val config = LambdaDataStoreParams.parse(params, persistence.config.catalog)
    val clock = ClockParam.lookupOpt(params).getOrElse(Clock.systemUTC())
    new LambdaDataStore(persistence, config)(clock)
  }

  override def createNewDataStore(params: java.util.Map[String, _]): DataStore = createDataStore(params)

  override def isAvailable: Boolean = true

  override def getDisplayName: String = LambdaDataStoreFactory.DisplayName

  override def getDescription: String = LambdaDataStoreFactory.Description

  override def getParametersInfo: Array[Param] = Array(LambdaDataStoreFactory.ParameterInfo :+ NamespaceParam: _*)

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    LambdaDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[Key, _] = java.util.Collections.emptyMap()
}

object LambdaDataStoreFactory extends GeoMesaDataStoreInfo {

  import LambdaDataStoreParams._

  override val DisplayName = "Kafka/Accumulo Lambda (GeoMesa)"

  override val Description = "Hybrid store using Kafka for recent events and Accumulo for long-term storage"

  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    Array(
      Params.Accumulo.InstanceParam,
      Params.Accumulo.ZookeepersParam,
      Params.Accumulo.CatalogParam,
      Params.Accumulo.UserParam,
      Params.Accumulo.PasswordParam,
      Params.Accumulo.KeytabParam,
      BrokersParam,
      ZookeepersParam,
      ExpiryParam,
      PersistParam,
      AuthsParam,
      ForceEmptyAuthsParam,
      QueryTimeoutParam,
      QueryThreadsParam,
      Params.Accumulo.RecordThreadsParam,
      Params.Accumulo.WriteThreadsParam,
      PartitionsParam,
      ConsumersParam,
      ProducerOptsParam,
      ConsumerOptsParam,
      LooseBBoxParam,
      GenerateStatsParam,
      AuditQueriesParam
    )

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    AccumuloDataStoreFactory.canProcess(LambdaDataStoreFactory.filter(params)) &&
        Seq(ExpiryParam, BrokersParam, ZookeepersParam).forall(_.exists(params))

  // noinspection TypeAnnotation
  object Params extends GeoMesaDataStoreParams with SecurityParams {

    object Accumulo {
      val InstanceParam      = copy(AccumuloDataStoreParams.InstanceNameParam)
      val ZookeepersParam    = copy(AccumuloDataStoreParams.ZookeepersParam)
      val UserParam          = copy(AccumuloDataStoreParams.UserParam)
      val PasswordParam      = copy(AccumuloDataStoreParams.PasswordParam)
      val KeytabParam        = copy(AccumuloDataStoreParams.KeytabPathParam)
      val RecordThreadsParam = copy(AccumuloDataStoreParams.RecordThreadsParam)
      val WriteThreadsParam  = copy(AccumuloDataStoreParams.WriteThreadsParam)
      val CatalogParam       = copy(AccumuloDataStoreParams.CatalogParam)
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
=======
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)

    @deprecated("replaced with LambdaDataStoreParams")
    object Kafka {
      val BrokersParam      = LambdaDataStoreParams.BrokersParam
      val ZookeepersParam   = LambdaDataStoreParams.ZookeepersParam
      val PartitionsParam   = LambdaDataStoreParams.PartitionsParam
      val ConsumersParam    = LambdaDataStoreParams.ConsumersParam
      val ProducerOptsParam = LambdaDataStoreParams.ProducerOptsParam
      val ConsumerOptsParam = LambdaDataStoreParams.ConsumerOptsParam
    }

    @deprecated("replaced with LambdaDataStoreParams")
    val ExpiryParam        = LambdaDataStoreParams.ExpiryParam
    @deprecated("replaced with LambdaDataStoreParams")
    val PersistParam       = LambdaDataStoreParams.PersistParam

    // test params
    @deprecated("replaced with LambdaDataStoreParams")
    val ClockParam         = LambdaDataStoreParams.ClockParam
    @deprecated("replaced with LambdaDataStoreParams")
    val OffsetManagerParam = LambdaDataStoreParams.OffsetManagerParam
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
  }

  private def copy[T <: AnyRef](p: GeoMesaParam[T])(implicit ct: ClassTag[T]): GeoMesaParam[T] = {
    new GeoMesaParam[T](s"lambda.${p.key}", p.description.toString, optional = !p.required, default = p.default,
      password = p.password, largeText = p.largeText, extension = p.extension, deprecatedKeys = p.deprecatedKeys,
      deprecatedParams = p.deprecatedParams, systemProperty = p.systemProperty)
  }

  private def filter(params: java.util.Map[String, _]): java.util.Map[String, _] = {
    // note: includes a bit of redirection to allow us to pass non-serializable values in to tests
    import scala.collection.JavaConverters._
    Map[String, Any](params.asScala.toSeq: _ *)
        .map { case (k, v) => (if (k.startsWith("lambda.")) { k.substring(7) } else { k }, v) }
        .asJava
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
        .asJava.asInstanceOf[java.util.Map[String, Serializable]]
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
        .asJava.asInstanceOf[java.util.Map[String, Serializable]]
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
=======
        .asJava.asInstanceOf[java.util.Map[String, Serializable]]
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
        .asJava.asInstanceOf[java.util.Map[String, Serializable]]
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
        .asJava.asInstanceOf[java.util.Map[String, Serializable]]
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f1532f2313 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 60be59afe8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 5a4c24e020 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 89085e28b8 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 541079bbd0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
        .asJava.asInstanceOf[java.util.Map[String, Serializable]]
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 4a51affd9e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> a154b4927b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 7a84c9d22d (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 7fff0a6154 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b298e017f1 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 991f10e2a4 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> ce026b5c8a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
        .asJava.asInstanceOf[java.util.Map[String, Serializable]]
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> d420f80210 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 87ab0f22da (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 0283274bf0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c738f63bd9 (GEOMESA-3254 Add Bloop build support)
  }
}
