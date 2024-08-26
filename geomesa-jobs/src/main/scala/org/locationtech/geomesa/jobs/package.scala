/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

package object jobs {

  /**
   * Trait for tracking the result of m/r jobs
   */
  sealed trait JobResult {

    /**
     * Did the job fail
     *
     * @return
     */
    def failed: Boolean

    /**
     * Did the job succeed
     *
     * @return
     */
    def success: Boolean = !failed

    /**
     * Status message
     *
     * @return
     */
    def message: String

    /**
     * Chain a second action with this result. The second action will only run if the first job was a success
     *
     * @param f second job function
     * @return
     */
    def merge(f: => Option[JobResult]): JobResult
  }

  object JobResult {

    case class JobFailure(message: String) extends JobResult {
      override def failed: Boolean = true
      override def merge(f: => Option[JobResult]): JobResult = this
    }

    case class JobSuccess(message: String, counts: Map[String, Long]) extends JobResult {
      override def failed: Boolean = false
      override def merge(f: => Option[JobResult]): JobResult = {
        f match {
          case None => this
          case Some(j: JobFailure) => j
          case Some(j: JobSuccess) =>
            val msg = (message, j.message) match {
              case ("", m2) => m2
              case (m1, "") => m1
              case (m1, m2) => s"$m1\n$m2"
            }
            JobSuccess(msg, counts ++ j.counts)
        }
      }
    }
  }

  /**
   * Job tracking
   */
  trait Awaitable {

    /**
     * Wait for a job to finish
     *
     * @param reporter status callback
     * @return
     */
    def await(reporter: StatusCallback): JobResult
  }

  trait StatusCallback {
    def reset(): Unit
    def apply(prefix: String, progress: Float, counters: Seq[(String, Long)], done: Boolean): Unit
  }

  object NoStatus extends StatusCallback {
    override def reset(): Unit = {}
    override def apply(prefix: String, progress: Float, counters: Seq[(String, Long)], done: Boolean): Unit = {}
  }
}
