/***********************************************************************
 * Crown Copyright (c) 2016-2021 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.ScalaSimpleFeature.copy
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.SimpleFeature
import org.specs2.runner.JUnitRunner

/**
 * This mostly tests KryoVisibilityRowEncoder
 */
@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreNullAttributeVisibilityTest extends TestWithFeatureType {

  import scala.collection.JavaConversions._

  sequential

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
    val ds = DataStoreFinder.getDataStore(dsParams ++ Map(AccumuloDataStoreParams.AuthsParam.key -> auths)).asInstanceOf[AccumuloDataStore]
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
