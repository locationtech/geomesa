/***********************************************************************
<<<<<<< HEAD
<<<<<<< HEAD
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
=======
>>>>>>> b137d29d1e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> b368bb796e (Merge branch 'feature/postgis-fixes')
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
=======
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
>>>>>>> d136bd4a2c (Merge branch 'feature/postgis-fixes')
=======
=======
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
<<<<<<< HEAD
>>>>>>> b137d29d1e (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
=======
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
>>>>>>> b368bb796e (Merge branch 'feature/postgis-fixes')
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

<<<<<<< HEAD
=======
import java.util.Collections

<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.locationtech.geomesa.jobs.JobResult.JobSuccess
import org.locationtech.geomesa.jobs.{JobResult, StatusCallback}
import org.locationtech.geomesa.tools.Command

<<<<<<< HEAD
import java.util.Collections

=======
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
object StorageJobUtils extends LazyLogging {

  def distCopy(
      srcRoot: Path,
      destRoot: Path,
      statusCallback: StatusCallback,
      conf: Configuration = new Configuration()): JobResult = {
    statusCallback.reset()

    Command.user.info("Submitting job 'DistCp' - please wait...")

    val opts = distCpOptions(srcRoot, destRoot)
    val job = new DistCp(conf, opts).execute()

    Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")

    JobRunner.monitor(job, statusCallback, Seq.empty, Seq.empty).merge {
      Some(JobSuccess(s"Successfully copied data to $destRoot", Map.empty))
    }
  }

  private def distCpOptions(src: Path, dest: Path): DistCpOptions =
    try { distCpOptions3(src, dest) } catch { case _: ClassNotFoundException => distCpOptions2(src, dest) }

  // hadoop 3 API
  private def distCpOptions3(src: Path, dest: Path): DistCpOptions = {
    val clas = Class.forName("org.apache.hadoop.tools.DistCpOptions$Builder")
    val constructor = clas.getConstructor(classOf[java.util.List[Path]], classOf[Path])
    val builder = constructor.newInstance(Collections.singletonList(src), dest)
    clas.getMethod("withAppend", classOf[Boolean]).invoke(builder, java.lang.Boolean.FALSE)
    clas.getMethod("withOverwrite", classOf[Boolean]).invoke(builder, java.lang.Boolean.TRUE)
    clas.getMethod("withBlocking", classOf[Boolean]).invoke(builder, java.lang.Boolean.FALSE)
    clas.getMethod("withCopyStrategy", classOf[String]).invoke(builder, "dynamic")
    clas.getMethod("build").invoke(builder).asInstanceOf[DistCpOptions]
  }

  // hadoop 2 API
  private def distCpOptions2(src: Path, dest: Path): DistCpOptions = {
    val constructor = classOf[DistCpOptions].getConstructor(classOf[java.util.List[Path]], classOf[Path])
    val opts = constructor.newInstance(Collections.singletonList(src), dest)
    classOf[DistCpOptions].getMethod("setAppend", classOf[Boolean]).invoke(opts, java.lang.Boolean.FALSE)
    classOf[DistCpOptions].getMethod("setOverwrite", classOf[Boolean]).invoke(opts, java.lang.Boolean.TRUE)
    classOf[DistCpOptions].getMethod("setCopyStrategy", classOf[String]).invoke(opts, "dynamic")
    opts
  }

}
