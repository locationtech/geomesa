/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs

import java.util.{List => JList, Map => JMap}

import cascading.flow.{Flow, FlowStep, FlowStepStrategy}
import com.twitter.chill.config.ConfiguredInstantiator
import com.twitter.scalding._
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.locationtech.geomesa.jobs.scalding.serialization.SimpleFeatureKryoHadoop

abstract class GeoMesaBaseJob(args: Args) extends Job(args) with Logging {

  def jobName: String = s"GeoMesa ${getClass.getSimpleName}"

  // hook to set the job name
  override def stepStrategy: Option[FlowStepStrategy[_]] = Some(new JobNameFlowStepStrategy(jobName))

  // we need to register our custom simple feature serialization
  override def config: Map[AnyRef, AnyRef] =
    super.config ++ Map(ConfiguredInstantiator.KEY -> classOf[SimpleFeatureKryoHadoop].getName)

  // override the run method to perform tasks after the job completes
  override def run: Boolean = {
    val result = super.run
    if (result) {
      afterJobTasks()
      logger.info("Job completed successfully")
    } else {
      logger.error("Job failed")
    }
    result
  }

  // subclasses can override to perform cleanup or final tasks after the job completes
  def afterJobTasks(): Unit = {}
}

/**
 * Sets the job name in the flow step
 */
class JobNameFlowStepStrategy(name: String) extends FlowStepStrategy[Any] {
  override def apply(flow: Flow[Any], previous: JList[FlowStep[Any]], flowStep: FlowStep[Any]) =
    flowStep.getConfig match {
      case conf: JobConf => conf.setJobName(name)
      case _ => // no-op
    }
}

object GeoMesaBaseJob {

  def runJob(conf: Configuration, args: Map[String, List[String]], instantiateJob: (Args) => Job) = {

    // set libjars so that our dependent libs get propagated to the cluster
    JobUtils.setLibJars(conf)

    // run the scalding job on HDFS
    val hdfsMode = Hdfs(strict = true, conf)
    val arguments = Mode.putMode(hdfsMode, new Args(args))

    val job = instantiateJob(arguments)
    val flow = job.buildFlow
    flow.complete() // this blocks until the job is done
  }

  implicit class RichArgs(val args: Args) extends AnyVal {

    /**
     * Allows a comma-separate list, instead of space-separated like scalding uses - but use caution, any
     * commas in the list will cause splits.
     */
    def nonStrictList(key: String): List[String] = {
      val list = args.list(key)
      if (list.length == 1) list.flatMap(_.split(",")) else list
    }
  }
}