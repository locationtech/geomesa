/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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


package org.locationtech.geomesa.plugin.process

import org.geotools.process.factory.AnnotatedBeanProcessFactory
import org.geotools.text.Text
import org.locationtech.geomesa.core.process.knn.KNearestNeighborSearchProcess
import org.locationtech.geomesa.core.process.proximity.ProximitySearchProcess
import org.locationtech.geomesa.core.process.query.QueryProcess
import org.locationtech.geomesa.core.process.tube.TubeSelectProcess
import org.locationtech.geomesa.core.process.unique.UniqueProcess

class ProcessFactory
  extends AnnotatedBeanProcessFactory(
    Text.text("GeoMesa Process Factory"),
    "geomesa",
    classOf[DensityProcess],
    classOf[TubeSelectProcess],
    classOf[ProximitySearchProcess],
    classOf[QueryProcess],
    classOf[KNearestNeighborSearchProcess],
    classOf[UniqueProcess]
  )
