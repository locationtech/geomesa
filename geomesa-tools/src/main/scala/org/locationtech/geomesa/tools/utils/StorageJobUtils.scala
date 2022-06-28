/***********************************************************************
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
=======
>>>>>>> b54ff06b30 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> c591b977be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f23883eba7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> bfa6d08a0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d136bd4a2c (Merge branch 'feature/postgis-fixes')
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> 2b1d931578 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9cdd96d1c0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b54ff06b30 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> 92dccb4d7a (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> c591b977be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> f487c279d9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> f23883eba7 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
>>>>>>> b36757a532 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> bfa6d08a0b (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
>>>>>>> d136bd4a2c (Merge branch 'feature/postgis-fixes')
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

<<<<<<< HEAD
=======
import java.util.Collections

>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.jobs.{JobResult, StatusCallback}
<<<<<<< HEAD
=======
import org.locationtech.geomesa.tools.Command

<<<<<<< HEAD
import java.util.Collections
>>>>>>> 5090f41d15b (GEOMESA-3092 Support Lambda NiFi processor (#2777))

=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
object StorageJobUtils extends LazyLogging {

  @deprecated("Replaced with org.locationtech.geomesa.tools.utils.DistributedCopy")
  def distCopy(
      srcRoot: Path,
      destRoot: Path,
      statusCallback: StatusCallback,
      conf: Configuration = new Configuration()): JobResult =
    new DistributedCopy(conf).copy(srcRoot, destRoot, statusCallback)
}
