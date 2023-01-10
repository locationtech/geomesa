/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.wps

import org.geotools.api.util.InternationalString
import org.geotools.process.factory.AnnotatedBeanProcessFactory
import org.geotools.text.Text
import org.locationtech.geomesa.process.GeoMesaProcess

import java.util.ServiceLoader

class GeoMesaProcessFactory
    extends AnnotatedBeanProcessFactory(
      GeoMesaProcessFactory.Name, GeoMesaProcessFactory.NameSpace, GeoMesaProcessFactory.processes: _*)

object GeoMesaProcessFactory {

  val NameSpace = "geomesa"
  val Name: InternationalString = Text.text("GeoMesa Process Factory")

  def processes: Array[Class[_]] = {
    import scala.collection.JavaConverters._
    ServiceLoader.load(classOf[GeoMesaProcess]).iterator().asScala.map(_.getClass).toArray
  }
}
