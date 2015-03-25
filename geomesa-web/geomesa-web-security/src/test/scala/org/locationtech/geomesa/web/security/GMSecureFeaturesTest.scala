package org.locationtech.geomesa.web.security

import com.vividsolutions.jts.geom.Coordinate
import org.geoserver.platform.GeoServerExtensions
import org.geoserver.security.WrapperPolicy
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator, SimpleFeatureSource}
import org.geotools.data.{DataStore, DataUtilities, FeatureSource, Query}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.security.SecurityUtils
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.springframework.context.support.GenericApplicationContext
import org.springframework.security.authentication.TestingAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder

@RunWith(classOf[JUnitRunner])
class GMSecureFeaturesTest extends Specification {

  sequential

  val testSFT = SimpleFeatureTypes.createType("test", "name:String,*geom:Point:srid=4326")
  val builder = AvroSimpleFeatureFactory.featureBuilder(testSFT)
  val gf = JTSFactoryFinder.getGeometryFactory

  "GMSecuredDataFactory" should {

    val gmsdf = new GMSecuredDataFactory

    "be able to secure a DataStore" >> {
      gmsdf.canSecure(classOf[DataStore]) must beTrue
    }

    "be able to secure a FeatureCollection" >> {
      gmsdf.canSecure(classOf[SimpleFeatureCollection]) must beTrue
    }

    "be able to secure a FeatureSource" >> {
      gmsdf.canSecure(classOf[SimpleFeatureSource]) must beTrue
    }

    val features = (0 until 20).map { i =>
      builder.reset()
      builder.addAll(Array[AnyRef](s"$i", gf.createPoint(new Coordinate(45.0, 45.0))))
      val f = builder.buildFeature(s"$i")
      val viz = i match {
        case _ if i % 3 == 0 => "USER|ADMIN"
        case _ if i % 2 == 0 => "ADMIN"
        case _               => "USER&ADMIN"
      }

      SecurityUtils.setFeatureVisibilities(f, viz)
      f
    }

    // required for geoserver extensions to kick in properly
    val extensions = new GeoServerExtensions()
    val ctx = new GenericApplicationContext()
    extensions.setApplicationContext(ctx)

    val src = DataUtilities.source(features.toArray)
    val policy = WrapperPolicy.readOnlyHide(null)
    val wrapped = gmsdf.secure(src, policy).asInstanceOf[FeatureSource[SimpleFeatureType, SimpleFeature]]

    "wrap and secure a source" >> {

      import org.locationtech.geomesa.utils.geotools.Conversions._

      "user with ADMIN and USER roles should see all 20 features" >> {
        val ctx = SecurityContextHolder.createEmptyContext()
        ctx.setAuthentication(new TestingAuthenticationToken(null, null, "ADMIN", "USER"))
        SecurityContextHolder.setContext(ctx)

        wrapped.getFeatures(Query.ALL).features().asInstanceOf[SimpleFeatureIterator].toList.size must be equalTo 20
      }

      "user with just USER should see 7 features" >> {
        val ctx = SecurityContextHolder.createEmptyContext()
        ctx.setAuthentication(new TestingAuthenticationToken(null, null, "USER"))
        SecurityContextHolder.setContext(ctx)

        wrapped.getFeatures(Query.ALL).features().asInstanceOf[SimpleFeatureIterator].toList.size must be equalTo 7
      }
    }


  }
}
