/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IteratorTriggerTest extends Specification {
  sequential

  object TestTable {
    val TEST_TABLE = "test_table"
    val featureName = "feature"
    val schemaEncoding = "%~#s%" + featureName + "#cstr%10#r%0,1#gh%yyyyMM#d::%~#s%1,3#gh::%~#s%4,3#gh%ddHH#d%10#id"

    val testFeatureTypeSpec: String = {
      s"POINT:String,LINESTRING:String,POLYGON:String,attr1:String:$OPT_INDEX_VALUE=true,attr2:String,geom:Point:srid=4326,dtg:Date,dtg_end_time:Date"
    }

    val testFeatureType: SimpleFeatureType = {
      val featureType: SimpleFeatureType = SimpleFeatureTypes.createType(featureName, testFeatureTypeSpec)
      featureType.setDtgField("dtg")
      featureType
    }


    def sampleQuery(ecql: org.opengis.filter.Filter, finalAttributes: Array[String]): Query = {
      val aQuery = new Query(testFeatureType.getTypeName, ecql, finalAttributes)
      QueryPlanner.setQueryTransforms(aQuery, testFeatureType) // normally called by data store when getting feature reader
      aQuery
    }

    /**
     * Function that duplicates the filter mutation from StIdxStrategy
     *
     * This will attempt to factor out the time and space components of the ECQL query.
     */

    def extractReWrittenCQL(query: Query, featureType: SimpleFeatureType): Option[Filter] = {
      val (_, otherFilters) = partitionPrimarySpatials(query.getFilter, featureType)
      val (_, ecqlFilters: Seq[Filter]) = partitionPrimaryTemporals(otherFilters, featureType)

      andOption(ecqlFilters)
    }
  }

  object TriggerTest {
    // filters for testing

    val trivialFilterString = "true = true"

    val anotherTrivialFilterString = "(INCLUDE)"

    val spatialFilterString =
      "WITHIN(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"

    val spatialTemporalFilterString =
      "WITHIN(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND (dtg before 2010-08-08T23:59:59Z)"

    val extraAttributeFilterString =
      "WITHIN(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND (attr2 like '2nd___')"

    val nonReducibleFilterString =
      "WITHIN(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND (dtg before 2010-08-08T23:59:59Z) AND (dtg_end_time after 2010-08-08T00:00:00Z)"

    val reducibleFilterString =
      "WITHIN(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND (dtg between '2010-08-08T00:00:00.000Z' AND '2010-08-08T23:59:59.000Z')"

    // transforms for testing
    val geomTransformToIndex = {
      Array("geom")
    }
    val simpleTransformToIndex = {
      Array("geom", "dtg")
    }
    val renameTransformToIndex = {
      Array("newgeo=geom", "dtg")
    }
    val complexTransformToIndex = {
      Array("geom=buffer(geom,2)", "dtg")
    }
    val simpleTransformToIndexPlusAnother = {
      Array("geom", "dtg", "attr2")
    }
    val extraTransformToIndex = {
      Array("geom", "dtg", "attr1")
    }
    val nullTransform = null

    /**
     * Function for use in testing useIndexOnlyIterator
     */
    def useIndexOnlyIteratorTest(ecqlPred: String, transformText: Array[String]): Boolean = {
      val aQuery = TestTable.sampleQuery(ECQL.toFilter(ecqlPred), transformText)
      val modECQLPred = TestTable.extractReWrittenCQL(aQuery, TestTable.testFeatureType)
      IteratorTrigger.useIndexOnlyIterator(modECQLPred, aQuery.getHints, TestTable.testFeatureType)
    }

    /**
     * Function for use in testing useSimpleFeatureFilteringIterator
     */
    def useSimpleFeatureFilteringIteratorTest(ecqlPred: String, transformText: Array[String]): Boolean = {
      val aQuery = TestTable.sampleQuery(ECQL.toFilter(ecqlPred), transformText)
      val modECQLPred = TestTable.extractReWrittenCQL(aQuery, TestTable.testFeatureType)
      IteratorTrigger.useSimpleFeatureFilteringIterator(modECQLPred, aQuery.getHints)
    }

    /**
     * Function for use in testing chooseIterator
     */
    def chooseIteratorTest(ecqlPred: String, transformText: Array[String]): IteratorConfig = {
      val aQuery = TestTable.sampleQuery(ECQL.toFilter(ecqlPred), transformText)
      val modECQLPred = TestTable.extractReWrittenCQL(aQuery, TestTable.testFeatureType)
      IteratorTrigger.chooseIterator(aQuery.getFilter, modECQLPred, aQuery.getHints, TestTable.testFeatureType)
    }
  }
    "useIndexOnlyIterator" should {
      "be run when requesting only index attributes" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.anotherTrivialFilterString, TriggerTest.simpleTransformToIndex)
        isTriggered must beTrue
      }

      "be run when renaming only index attributes" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.anotherTrivialFilterString, TriggerTest.renameTransformToIndex)
        isTriggered must beTrue
      }

      "not be run when transforming an index attribute" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.anotherTrivialFilterString, TriggerTest.complexTransformToIndex)
        isTriggered must beFalse
      }

      "not be run when requesting a non-index attribute" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.anotherTrivialFilterString, TriggerTest.simpleTransformToIndexPlusAnother)
        isTriggered must beFalse
      }

      "be run when requesting an extra indexed attribute" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.anotherTrivialFilterString, TriggerTest.extraTransformToIndex)
        isTriggered must beTrue
      }

      "not be run when requesting all attributes via a null transform" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.anotherTrivialFilterString, TriggerTest.nullTransform)
        isTriggered must beFalse
      }

      "be run when requesting index attributes and using a trivial filter" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.trivialFilterString, TriggerTest.simpleTransformToIndex)
        isTriggered must beTrue
      }

      "be run when requesting index attributes and using another trivial filter" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.anotherTrivialFilterString, TriggerTest.simpleTransformToIndex)
        isTriggered must beTrue
      }

      "not be run when requesting index attributes and filtering on a non-index attribute" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.extraAttributeFilterString, TriggerTest.simpleTransformToIndex)
        isTriggered must beFalse
      }

      "not be run when requesting index attributes and filtering on a non-index attribute" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.extraAttributeFilterString, TriggerTest.simpleTransformToIndex)
        isTriggered must beFalse
      }

      "not be run when requesting index attributes and dealing with a filter that can not be fully reduced" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.nonReducibleFilterString, TriggerTest.simpleTransformToIndex)
        isTriggered must beFalse
      }

      "be run when requesting index attributes and dealing with a filter that can be fully reduced" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.reducibleFilterString, TriggerTest.simpleTransformToIndex)
        isTriggered must beTrue
      }

      "be run when transforms overlap filters" in {
        val isTriggered = TriggerTest.useIndexOnlyIteratorTest(TriggerTest.spatialTemporalFilterString, TriggerTest.geomTransformToIndex)
        isTriggered must beTrue
      }
    }


  "SimpleFeatureFilteringIterator" should {
    "be run when requesting a transform" in {
       val isTriggered = TriggerTest.useSimpleFeatureFilteringIteratorTest(TriggerTest.anotherTrivialFilterString, TriggerTest.complexTransformToIndex)
       isTriggered must beTrue
    }

    "be run when passed a non-trivial ECQL filter and a simple transform" in {
      val isTriggered = TriggerTest.useSimpleFeatureFilteringIteratorTest(TriggerTest.extraAttributeFilterString, TriggerTest.simpleTransformToIndex)
      isTriggered must beTrue
    }

    "be run when passed a non-trivial ECQL filter and a null transform" in {
      val isTriggered = TriggerTest.useSimpleFeatureFilteringIteratorTest(TriggerTest.extraAttributeFilterString, TriggerTest.nullTransform)
      isTriggered must beTrue
    }

    "not be run when passed a trivial ECQL filter and a null transform" in {
      val isTriggered = TriggerTest.useSimpleFeatureFilteringIteratorTest(TriggerTest.anotherTrivialFilterString, TriggerTest.nullTransform)
      isTriggered must beFalse
   }

    "not be run when transforms overlap filters" in {
      val choice = TriggerTest.chooseIteratorTest(TriggerTest.spatialTemporalFilterString, TriggerTest.simpleTransformToIndex)
      choice.hasTransformOrFilter must beFalse
    }

    "not be run for geom transform and filter" in {
      val choice = TriggerTest.chooseIteratorTest(TriggerTest.spatialFilterString, TriggerTest.geomTransformToIndex)
      choice.hasTransformOrFilter must beFalse
    }

    "be run when transforms don't overlap filters" in {
      val choice = TriggerTest.chooseIteratorTest(TriggerTest.spatialTemporalFilterString, TriggerTest.geomTransformToIndex)
      choice.iterator mustEqual IndexOnlyIterator
      choice.transformCoversFilter must beFalse
    }
  }

  "IteratorTrigger" should {
    "accept INCLUDE as a pass through filter" in {
      IteratorTrigger.passThroughFilter(Filter.INCLUDE) mustEqual(true)
    }

    "determine overlap between transforms and filters" >> {
      val sft = SimpleFeatureTypes.createType("overlaptest", "name:String,dtg:Date,*geom:Point:srid=4326")

      def testOverlap(filter: String, attributes: Array[String]) = {
        val query = new Query("overlaptest", ECQL.toFilter(filter), attributes)
        QueryPlanner.setQueryTransforms(query, sft)
        IteratorTrigger.doTransformsCoverFilters(query.getHints, query.getFilter)
      }

      "for single geom attribute" >> {
        val result = testOverlap("BBOX(geom, -180, -90, 180, 90)", Array("geom"))
        result must beTrue
      }

      "for multiple overlapping attributes" >> {
        val filter = "BBOX(geom, -180, -90, 180, 90) AND dtg = 2010-08-08T23:59:59Z OR name = 'joe'"
        val attributes = Array("geom", "dtg", "name")
        val result = testOverlap(filter, attributes)
        result must beTrue
      }

      "for missing attributes" >> {
        val filter = "BBOX(geom, -180, -90, 180, 90) AND dtg = 2010-08-08T23:59:59Z OR name = 'joe'"
        val attributes = Array("geom", "dtg")
        val result = testOverlap(filter, attributes)
        result must beFalse
      }

      "for non overlapping transforms" >> {
        val filter = "BBOX(geom, -180, -90, 180, 90) AND dtg = 2010-08-08T23:59:59Z"
        val attributes = Array("geom")
        val result = testOverlap(filter, attributes)
        result must beFalse
      }

      "for non-included geoms" >> {
        // geom will always get added to the transforms
        val filter = "BBOX(geom, -180, -90, 180, 90) AND dtg = 2010-08-08T23:59:59Z"
        val attributes = Array("dtg")
        val result = testOverlap(filter, attributes)
        result must beTrue
      }
    }
  }

  "AttributeIndexIterator" should {
    "be run when requesting simple attributes" in {
      val sftName = "AttributeIndexIteratorTriggerTest"
      val spec = "name:String:index=true,age:Integer:index=true,dtg:Date:index=true,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType(sftName, spec)
      val query = new Query(sftName, Filter.INCLUDE, Array("geom", "dtg", "name"))
      QueryPlanner.setQueryTransforms(query, sft) // normally called by data store when getting feature reader
      val iteratorChoice = IteratorTrigger.chooseAttributeIterator(None, query.getHints, sft, "name")
      iteratorChoice.iterator mustEqual IndexOnlyIterator
    }
    "be run when requesting extra index-encoded attributes" in {
      val sftName = "AttributeIndexIteratorTriggerTest"
      val spec = "name:String:index=true,age:Integer:index-value=true,dtg:Date:index=true,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType(sftName, spec)
      val query = new Query(sftName, ECQL.toFilter("name='bob'"), Array("geom", "dtg", "name", "age"))
      QueryPlanner.setQueryTransforms(query, sft) // normally called by data store when getting feature reader
      val iteratorChoice = IteratorTrigger.chooseAttributeIterator(None, query.getHints, sft, "name")
      iteratorChoice.iterator mustEqual IndexOnlyIterator
    }
  }
}
