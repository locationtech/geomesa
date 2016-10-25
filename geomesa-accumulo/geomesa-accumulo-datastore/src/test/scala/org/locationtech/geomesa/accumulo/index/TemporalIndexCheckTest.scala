/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.TemporalIndexCheck
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TemporalIndexCheckTest extends Specification {
  // setup the basic types
  def noDTGType = SimpleFeatureTypes.createType("noDTGType", s"foo:String,bar:Geometry,baz:String,$DEFAULT_GEOMETRY_PROPERTY_NAME:Point")
  def oneDTGType = SimpleFeatureTypes.createType("oneDTGType", s"foo:String,bar:Geometry,baz:String,$DEFAULT_GEOMETRY_PROPERTY_NAME:Point,$DEFAULT_DTG_PROPERTY_NAME:Date")
  def twoDTGType = SimpleFeatureTypes.createType("twoDTGType", s"foo:String,bar:Geometry,baz:String,$DEFAULT_GEOMETRY_PROPERTY_NAME:Point,$DEFAULT_DTG_PROPERTY_NAME:Date,dtg_end_time:Date")

  val DEFAULT_DATE_KEY = org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs.DEFAULT_DATE_KEY

  def copy(sft: SimpleFeatureType) = {
    val b = new SimpleFeatureTypeBuilder()
    b.init(sft)
    b.buildFeatureType()
  }

  "TemporalIndexCheck" should {
    "detect no valid DTG" in {
      val testType = copy(noDTGType)
      TemporalIndexCheck.validateDtgField(testType)
      testType.getDtgField must beNone
    }

    "detect no valid DTG even if SF_PROPERTY_START_TIME is set incorrectly" in {
      val testType = copy(noDTGType)
      testType.getUserData.put(DEFAULT_DATE_KEY, DEFAULT_DTG_PROPERTY_NAME)
      TemporalIndexCheck.validateDtgField(testType)
      testType.getDtgField must beNone
    }

    "detect a valid DTG if SF_PROPERTY_START_TIME is not set" in {
      val testType = copy(oneDTGType)
      testType.getUserData.remove(DEFAULT_DATE_KEY)
      TemporalIndexCheck.validateDtgField(testType)
      testType.getDtgField must beSome(DEFAULT_DTG_PROPERTY_NAME)
    }

    "detect a valid DTG if SF_PROPERTY_START_TIME is not properly set" in {
      val testType = copy(oneDTGType)
      testType.getUserData.put(DEFAULT_DATE_KEY, "no_such_dtg")
      TemporalIndexCheck.validateDtgField(testType)
      testType.getDtgField must beSome(DEFAULT_DTG_PROPERTY_NAME)
    }

    "present no DTG candidate if SF_PROPERTY_START_TIME is set properly" in {
      val testType = copy(oneDTGType)
      testType.setDtgField(DEFAULT_DTG_PROPERTY_NAME)
      TemporalIndexCheck.validateDtgField(testType)
      testType.getDtgField must beSome(DEFAULT_DTG_PROPERTY_NAME)
    }

    "detect valid DTG candidates and select the first if SF_PROPERTY_START_TIME is not set correctly" in {
      val testType = copy(twoDTGType)
      testType.getUserData.put(DEFAULT_DATE_KEY, "no_such_dtg")
      TemporalIndexCheck.validateDtgField(testType)
      testType.getDtgField must beSome(DEFAULT_DTG_PROPERTY_NAME)
    }

    "present no DTG candidate if SF_PROPERTY_START_TIME is set properly and there are multiple Date attributes" in {
      val testType = copy(twoDTGType)
      testType.getUserData.put(DEFAULT_DATE_KEY, DEFAULT_DTG_PROPERTY_NAME)
      TemporalIndexCheck.validateDtgField(testType)
      testType.getDtgField must beSome(DEFAULT_DTG_PROPERTY_NAME)
    }
  }

  "getDTGFieldName" should {
    "return a dtg field name if SF_PROPERTY_START_TIME is set properly" in {
      val testType = copy(oneDTGType)
      testType.setDtgField(DEFAULT_DTG_PROPERTY_NAME)
      testType.getDtgField must beSome(DEFAULT_DTG_PROPERTY_NAME)
    }

    "not return a dtg field name if SF_PROPERTY_START_TIME is not set correctly" in {
      val testType = copy(noDTGType)
      testType.setDtgField(DEFAULT_DTG_PROPERTY_NAME) must throwAn[IllegalArgumentException]
      testType.getDtgField must beNone
    }
  }

  "getDTGDescriptor" should {
    "return a dtg attribute descriptor if SF_PROPERTY_START_TIME is set properly" in {
      val testType = copy(oneDTGType)
      testType.setDtgField(DEFAULT_DTG_PROPERTY_NAME)
      testType.getDtgDescriptor must beSome(oneDTGType.getDescriptor(DEFAULT_DTG_PROPERTY_NAME))
    }

    "not return a dtg attribute descriptor if SF_PROPERTY_START_TIME is not set correctly" in {
      val testType = copy(noDTGType)
      testType.setDtgField(DEFAULT_DTG_PROPERTY_NAME) must throwAn[IllegalArgumentException]
      testType.getDtgDescriptor must beNone
    }
  }
}
