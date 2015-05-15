/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.geotools.process.vector.TransformProcess
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IndexPackageObjectTest extends Specification {

  "index" should {
    "compute target schemas from transformation expressions" in {
      val sftName = "targetSchemaTest"
      val defaultSchema = "name:String,geom:Point:srid=4326,dtg:Date"
      val origSFT = SimpleFeatureTypes.createType(sftName, defaultSchema)
      origSFT.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

      val query = new Query(sftName, Filter.INCLUDE, Array("name", "helloName=strConcat('hello', name)", "geom"))
      setQueryTransforms(query, origSFT)

      val transform = getTransformSchema(query)
      transform must beSome
      SimpleFeatureTypes.encodeType(transform.get) mustEqual
          s"name:String,*geom:Point:srid=4326:$OPT_INDEX=full:index-value=true,helloName:String"
    }
  }
}
