/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data.Query
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ComplexFeatureTest extends Specification with TestWithDataStore {

  override val spec =
    """
      |names:List[String],
      |fingers:List[String],
      |skills:Map[String,Integer],
      |metadata:Map[Double,String],
      |dtg:Date,
      |*geom:Point:srid=4326
    """.stripMargin

  addFeatures({
    // create and add a feature
    val builder = new SimpleFeatureBuilder(sft, CommonFactoryFinder.getFeatureFactory(null))
    builder.addAll(List(
      List("joe", "joseph"),
      List("pointer", "thumb", "ring").asJava,
      Map("java" -> 1, "scala" -> 100),
      Map(1.0 -> "value1", 2.0 -> "value2").asJava,
      "2010-01-01T00:00:00.000Z",
      "POINT(45.0 49.0)"
    ).asJava)
    val liveFeature = builder.buildFeature("fid-1")
    liveFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    Seq(liveFeature)
  })

  "SimpleFeatures" should {

    "set and return list values" >> {
      "in java" >> {
        val query = new Query(sftName, Filter.INCLUDE)
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(1)
        val fingers = features.head.getAttribute("fingers")
        fingers must not(beNull)
        fingers must beAnInstanceOf[java.util.List[_]]
        val fingersList = fingers.asInstanceOf[java.util.List[String]].asScala
        fingersList must haveSize(3)
        fingersList must contain("pointer", "thumb", "ring")
      }
      "in scala" >> {
        val query = new Query(sftName, Filter.INCLUDE)
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(1)
        val names = features.head.getAttribute("names")
        names must not(beNull)
        names must beAnInstanceOf[java.util.List[_]]
        val namesList = names.asInstanceOf[java.util.List[String]].asScala
        namesList must haveSize(2)
        namesList must contain("joe", "joseph")
      }
    }

    "set and return map values" >> {
      "in java" >> {
        val query = new Query(sftName, Filter.INCLUDE)
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(1)
        val metadata = features.head.getAttribute("metadata")
        metadata must not(beNull)
        metadata must beAnInstanceOf[java.util.Map[_, _]]
        val metadataMap = metadata.asInstanceOf[java.util.Map[Double, String]].asScala
        metadataMap must haveSize(2)
        metadataMap must havePairs(1.0 -> "value1", 2.0 -> "value2")
      }
      "in scala" >> {
        val query = new Query(sftName, Filter.INCLUDE)
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(1)
        val skills = features.head.getAttribute("skills")
        skills must not(beNull)
        skills must beAnInstanceOf[java.util.Map[_, _]]
        val skillsMap = skills.asInstanceOf[java.util.Map[String, Int]].asScala
        skillsMap must haveSize(2)
        skillsMap must havePairs("java" -> 1, "scala" -> 100)
      }
    }

    "query on list items" >> {
      "for single entries in the list" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom, 44, 48, 46, 51) AND names = 'joe'"))
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(1)
      }
      "for multiple entries in the list" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom, 44, 48, 46, 51) AND fingers = 'thumb' AND fingers = 'ring'"))
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(1)
      }
      "for ranges" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom, 44, 48, 46, 51) AND names > 'jane'"))
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(1)
      }
      "not match invalid filters" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom, 44, 48, 46, 51) AND fingers = 'index'"))
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(0)
      }
      "not match invalid ranges" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom, 44, 48, 46, 51) AND names < 'jane'"))
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(0)
      }
    }

    "query on map items" >> {
      "for keys in the map" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom, 44, 48, 46, 51) AND skills = 'java'"))
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(1)
      }.pendingUntilFixed("GEOMESA-454 - can't query on maps")

      "for values in the map" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom, 44, 48, 46, 51) AND skills = '100'"))
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(1)
      }.pendingUntilFixed("GEOMESA-454 - can't query on maps")

      "not match invalid filters" >> {
        val query = new Query(sftName, ECQL.toFilter("bbox(geom, 44, 48, 46, 51) AND skills = 'fortran'"))
        val features = SelfClosingIterator(fs.getFeatures(query).features).toList
        features must haveSize(0)
      }
    }
  }

}
