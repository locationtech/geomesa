/***********************************************************************
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
 * Crown Copyright (c) 2016-2024 Dstl
=======
=======
>>>>>>> 58c93ed806 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> 90c7b688e9 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> 422349e56d (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> d136bd4a2c (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 903b3b81c6 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> b368bb796e (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> c8d2cfae9c (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 5e3d21a94f (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> 823f734cb8 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
 * Crown Copyright (c) 2016-2023 Dstl
=======
 * Crown Copyright (c) 2016-2021 Dstl
>>>>>>> e5f251e08c (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
<<<<<<< HEAD
>>>>>>> 1913317092 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
=======
=======
>>>>>>> 21f7547af3 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> d1ecc0df13 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> e980963df5 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
 * Crown Copyright (c) 2016-2023 Dstl
=======
=======
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> e22b6ade6f (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
 * Crown Copyright (c) 2016-2022 Dstl
=======
 * Crown Copyright (c) 2016-2021 Dstl
>>>>>>> e5f251e08 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 0e7ab435a8 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
<<<<<<< HEAD
>>>>>>> 58c93ed806 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
=======
>>>>>>> 21f7547af3 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
<<<<<<< HEAD
>>>>>>> 90c7b688e9 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
=======
>>>>>>> d1ecc0df13 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
<<<<<<< HEAD
>>>>>>> 422349e56d (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
=======
 * Crown Copyright (c) 2016-2022 Dstl
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> d136bd4a2c (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> e980963df5 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
<<<<<<< HEAD
>>>>>>> 903b3b81c6 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
=======
=======
 * Crown Copyright (c) 2016-2022 Dstl
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
>>>>>>> b368bb796e (Merge branch 'feature/postgis-fixes')
=======
=======
 * Crown Copyright (c) 2016-2022 Dstl
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> c8d2cfae9c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e980963df (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
>>>>>>> e22b6ade6f (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
<<<<<<< HEAD
>>>>>>> 5e3d21a94f (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
=======
>>>>>>> e980963df (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
=======
 * Crown Copyright (c) 2016-2022 Dstl
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa (Merge branch 'feature/postgis-fixes')
>>>>>>> 1b25d7ddb4 (Merge branch 'feature/postgis-fixes')
>>>>>>> 823f734cb8 (Merge branch 'feature/postgis-fixes')
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.api.data._
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.ScalaSimpleFeature.copy
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.runner.JUnitRunner

/**
 * This mostly tests KryoVisibilityRowEncoder
 */
@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreNullAttributeVisibilityTest extends TestWithFeatureType {

<<<<<<< HEAD
  import scala.collection.JavaConverters._
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
  import scala.collection.JavaConversions._
<<<<<<< HEAD
>>>>>>> e5f251e08c (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> e5f251e08 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
>>>>>>> e980963df5 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
  import scala.collection.JavaConversions._
>>>>>>> e5f251e08 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
>>>>>>> e22b6ade6f (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))

  override val spec = "some_id:String,dtg:Date,*geo_location:Point:srid=4326,number:Integer,text:String;geomesa.visibility.level='attribute'"

  val visibility = "admin,user,user,user,user"

  val complete_feature = {
    val sf = new ScalaSimpleFeature(sft, "complete_feature")
    sf.setAttribute(0, "ABC123")
    sf.setAttribute(1, "2021-05-23T08:30:23.000Z)")
    sf.setAttribute(2, "POINT (36.1234 23.224)")
    sf.setAttribute(3, "42")
    sf.setAttribute(4, "The previous value was 42")
    SecurityUtils.setFeatureVisibility(sf, visibility)
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  val null_string_feature = {
    val sf = new ScalaSimpleFeature(sft, "null_string_feature")
    sf.setAttribute(0, "ABC1234")
    sf.setAttribute(1, "2021-05-23T08:30:23.000Z)")
    sf.setAttribute(2, "POINT (36.1234 23.224)")
    sf.setAttribute(3, null)
    sf.setAttribute(4, "The previous value was null")
    SecurityUtils.setFeatureVisibility(sf, visibility)
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  step {
    // Write features to datastore
    addFeatures(Seq(complete_feature, null_string_feature))
  }

  def queryByAuths(auths: String, filter: String): Seq[SimpleFeature] = {
<<<<<<< HEAD
    val ds = DataStoreFinder.getDataStore((dsParams ++ Map(AccumuloDataStoreParams.AuthsParam.key -> auths)).asJava).asInstanceOf[AccumuloDataStore]
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
    val ds = DataStoreFinder.getDataStore(dsParams ++ Map(AccumuloDataStoreParams.AuthsParam.key -> auths)).asInstanceOf[AccumuloDataStore]
<<<<<<< HEAD
>>>>>>> e5f251e08c (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> e5f251e08 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
>>>>>>> e980963df5 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
    val ds = DataStoreFinder.getDataStore(dsParams ++ Map(AccumuloDataStoreParams.AuthsParam.key -> auths)).asInstanceOf[AccumuloDataStore]
>>>>>>> e5f251e08 (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
>>>>>>> e22b6ade6f (GEOMESA-3091 Attribute level visibilities error with null attribute values (#2775))
    val query = new Query(sftName, ECQL.toFilter(filter))
    SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq
  }

  def featureWithUserAuth(feature: ScalaSimpleFeature): SimpleFeature = {
    val sf = copy(feature) // deep (enough) copy
    sf.setAttribute(0, null)
    sf
  }

  def featureWithAdminAuth(feature: ScalaSimpleFeature): SimpleFeature = {
    val sf = copy(feature) // deep (enough) copy
    sf.setAttribute(1, null)
    sf.setAttribute(2, null)
    sf.setAttribute(3, null)
    sf.setAttribute(4, null)
    sf
  }

  "AccumuloDataStore" should {

    "correctly return all features with just user auth" in {
      val features = queryByAuths("user", "INCLUDE")
      features must containTheSameElementsAs(Seq(complete_feature, null_string_feature).map(featureWithUserAuth))
    }

    "correctly return all features with just admin auth" in {
      val features = queryByAuths("admin", "INCLUDE")
      features must containTheSameElementsAs(Seq(complete_feature, null_string_feature).map(featureWithAdminAuth))
    }

    "correctly return all features with user and admin auth" in {
      val features = queryByAuths("user,admin", "INCLUDE")
      features must containTheSameElementsAs(Seq(complete_feature, null_string_feature))
    }
  }
}
