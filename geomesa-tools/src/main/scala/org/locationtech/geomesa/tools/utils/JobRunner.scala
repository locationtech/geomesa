/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.utils

import org.apache.hadoop.mapreduce.{Job, JobStatus}
import org.locationtech.geomesa.jobs.JobResult.{JobFailure, JobSuccess}
import org.locationtech.geomesa.jobs.{JobResult, StatusCallback}
import org.locationtech.geomesa.tools.Command

/**
  * Helper for running a job and reporting back status
  */
object JobRunner {

  /**
   * Submit and monitor a job
   *
   * @param job job
   * @param reporter status callback
   * @param mapCounters map status counters
   * @param reduceCounters reduce status counters (will be added to map phase if no reduce phase)
   * @return result
   */
  def run(
      job: Job,
      reporter: StatusCallback,
      mapCounters: => Seq[(String, Long)],
      reduceCounters: => Seq[(String, Long)]): JobResult = {
    submit(job)
    monitor(job, reporter, mapCounters, reduceCounters)
  }

  /**
   * Run a job asynchronously
   *
   * @param job job
   */
  def submit(job: Job): Unit = {
    Command.user.info(s"Submitting job '${job.getJobName}' - please wait...")
    job.submit()
    Command.user.info(s"Tracking available at ${job.getStatus.getTrackingUrl}")
  }

  /**
   * Monitor a job that has already been submitted
   *
   * @param job job
   * @param reporter status callback
   * @param mapCounters map status counters
   * @param reduceCounters reduce status counters (will be added to map phase if no reduce phase)
   * @return result
   */
  def monitor(
      job: Job,
      reporter: StatusCallback,
      mapCounters: => Seq[(String, Long)],
      reduceCounters: => Seq[(String, Long)]): JobResult = {

    val status: Boolean => Unit = if (job.getNumReduceTasks != 0) {
      var mapping = true
      done => {
        if (mapping) {
          val mapProgress = job.mapProgress()
          if (mapProgress < 1f) {
            reporter("Map:    ", mapProgress, mapCounters, done = false)
          } else {
            reporter("Map:    ", mapProgress, mapCounters, done = true)
            reporter.reset()
            mapping = false
          }
        } else {
          reporter("Reduce: ", job.reduceProgress(), reduceCounters, done)
        }
      }
    } else {
      // we don't have any reducers, just track mapper progress
      done => reporter("", job.mapProgress(), mapCounters ++ reduceCounters, done)
    }

    while (!job.isComplete) {
      if (job.getStatus.getState != JobStatus.State.PREP) {
        status(false)
      }
      Thread.sleep(500)
    }
    status(true)

    if (job.isSuccessful) {
      JobSuccess("", (mapCounters ++ reduceCounters).toMap)
    } else {
      JobFailure(s"Job failed with state ${job.getStatus.getState} due to: ${job.getStatus.getFailureInfo}")
    }
  }
}
