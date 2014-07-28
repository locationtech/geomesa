/*
* Copyright 2013 Commonwealth Computer Research, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package geomesa.core.index

import geomesa.core._
import geomesa.utils.geotools.SimpleFeatureTypes
import org.junit.runner.RunWith
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
}
