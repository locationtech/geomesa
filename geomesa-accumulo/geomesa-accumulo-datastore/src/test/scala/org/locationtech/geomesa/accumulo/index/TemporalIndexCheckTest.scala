/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TemporalIndexCheckTest extends Specification {
  // setup the basic types
  def noDTGType = SimpleFeatureTypes.createType("noDTGType", s"foo:String,bar:Geometry,baz:String,$DEFAULT_GEOMETRY_PROPERTY_NAME:Geometry")
  def oneDTGType = SimpleFeatureTypes.createType("oneDTGType", s"foo:String,bar:Geometry,baz:String,$DEFAULT_GEOMETRY_PROPERTY_NAME:Geometry,$DEFAULT_DTG_PROPERTY_NAME:Date")
  def twoDTGType = SimpleFeatureTypes.createType("twoDTGType", s"foo:String,bar:Geometry,baz:String,$DEFAULT_GEOMETRY_PROPERTY_NAME:Geometry,$DEFAULT_DTG_PROPERTY_NAME:Date,$DEFAULT_DTG_END_PROPERTY_NAME:Date")

  "TemporalIndexCheck" should {
    "detect no valid DTG" in {
      val testType = noDTGType
      val dtgCandidate = TemporalIndexCheck.extractNewDTGFieldCandidate(testType)
      dtgCandidate.isDefined must beFalse
    }

    "detect no valid DTG even if SF_PROPERTY_START_TIME is set incorrectly" in {
      val testType = noDTGType
      testType.getUserData.put(SF_PROPERTY_START_TIME, DEFAULT_DTG_PROPERTY_NAME)
      val dtgCandidate = TemporalIndexCheck.extractNewDTGFieldCandidate(testType)
      dtgCandidate.isDefined must beFalse
    }

    "detect a valid DTG if SF_PROPERTY_START_TIME is not set" in {
      val testType = oneDTGType
      testType.getUserData.remove(SF_PROPERTY_START_TIME)
      val dtgCandidate = TemporalIndexCheck.extractNewDTGFieldCandidate(testType)
      dtgCandidate.get must be equalTo DEFAULT_DTG_PROPERTY_NAME
    }

    "detect a valid DTG if SF_PROPERTY_START_TIME is not properly set" in {
      val testType = oneDTGType
      testType.getUserData.put(SF_PROPERTY_START_TIME, "no_such_dtg")
      val dtgCandidate = TemporalIndexCheck.extractNewDTGFieldCandidate(testType)
      dtgCandidate.get must be equalTo DEFAULT_DTG_PROPERTY_NAME
    }

    "present no DTG candidate if SF_PROPERTY_START_TIME is set properly" in {
      val testType = oneDTGType
      testType.getUserData.put(SF_PROPERTY_START_TIME, DEFAULT_DTG_PROPERTY_NAME)
      val dtgCandidate = TemporalIndexCheck.extractNewDTGFieldCandidate(testType)
      dtgCandidate.isDefined must beFalse
    }

    "detect valid DTG candidates and select the first if SF_PROPERTY_START_TIME is not set correctly" in {
      val testType = twoDTGType
      testType.getUserData.put(SF_PROPERTY_START_TIME, "no_such_dtg")
      val dtgCandidate = TemporalIndexCheck.extractNewDTGFieldCandidate(testType)
      dtgCandidate.get must be equalTo DEFAULT_DTG_PROPERTY_NAME
    }

    "present no DTG candidate if SF_PROPERTY_START_TIME is set properly and there are multiple Date attributes" in {
      val testType = twoDTGType
      testType.getUserData.put(SF_PROPERTY_START_TIME, DEFAULT_DTG_PROPERTY_NAME)
      val dtgCandidate = TemporalIndexCheck.extractNewDTGFieldCandidate(testType)
      dtgCandidate.isDefined must beFalse
    }
  }

  "getDTGFieldName" should {
    "return a dtg field name if SF_PROPERTY_START_TIME is set properly" in {
      val testType = oneDTGType
      testType.getUserData.put(SF_PROPERTY_START_TIME, DEFAULT_DTG_PROPERTY_NAME)
      val theName= getDtgFieldName(testType)
      theName.isDefined must beTrue
      theName.get must equalTo(DEFAULT_DTG_PROPERTY_NAME)
    }

    "not return a dtg field name if SF_PROPERTY_START_TIME is not set correctly" in {
      val testType = noDTGType
      testType.getUserData.put(SF_PROPERTY_START_TIME, DEFAULT_DTG_PROPERTY_NAME)
      val theName= getDtgFieldName(testType)
      theName.isDefined must beFalse
    }
  }

  "getDTGDescriptor" should {
    "return a dtg attribute descriptor if SF_PROPERTY_START_TIME is set properly" in {
      val testType = oneDTGType
      testType.getUserData.put(SF_PROPERTY_START_TIME, DEFAULT_DTG_PROPERTY_NAME)
      val theDescriptor = getDtgDescriptor(testType)
      theDescriptor.isDefined must beTrue
      theDescriptor.get must equalTo(oneDTGType.getDescriptor(DEFAULT_DTG_PROPERTY_NAME))
    }

    "not return a dtg attribute descriptor if SF_PROPERTY_START_TIME is not set correctly" in {
      val testType = noDTGType
      testType.getUserData.put(SF_PROPERTY_START_TIME, DEFAULT_DTG_PROPERTY_NAME)
      val theDescriptor = getDtgDescriptor(testType)
      theDescriptor.isDefined must beFalse
    }
  }
}
