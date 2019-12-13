/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conf

import scala.util.Try
import scala.util.control.NonFatal

case class SemanticVersion(major: Int,
                           minor: Int,
                           patch: Int,
                           preRelease: Option[String] = None,
                           build: Option[String] = None) extends Ordered[SemanticVersion] {

  import SemanticVersion.comparePreReleases

  // see https://semver.org/#spec-item-11
  override def compare(that: SemanticVersion): Int = {
    if (this.major > that.major) { 1 } else if (this.major < that.major) { -1 } else
    if (this.minor > that.minor) { 1 } else if (this.minor < that.minor) { -1 } else
    if (this.patch > that.patch) { 1 } else if (this.patch < that.patch) { -1 } else
    { comparePreReleases(this.preRelease, that.preRelease) }
    // note: 'build' is not used for comparison
  }

  override def toString: String =
    s"$major.$minor.$patch${preRelease.map(v => s"-$v").getOrElse("")}${build.map(v => s"+$v").getOrElse("")}"
}

object SemanticVersion {

  private val Regex = """(\d+)\.(\d+)\.(\d+)(-([0-9A-Za-z-.]+))?(\+([0-9A-Za-z-.]+))?""".r

  private val LenientRegex = """(\d+)\.?""".r

  def apply(version: String): SemanticVersion = {
    val Regex(major, minor, patch, _, preRelease, _, build) = version
    SemanticVersion(major.toInt, minor.toInt, patch.toInt, Option(preRelease), Option(build))
  }

  /**
    * Can be used to parse '.' delimited numbers that don't exactly match SemVer, e.g. 1.3.5.1-SNAPSHOT.
    * The first 3 digit groups found will be used for major, minor, and patch version, respectively.
    * Everything else is ignored.
    */
  def apply(version: String, lenient: Boolean): SemanticVersion = {
    try { apply(version) } catch {
      case NonFatal(e) if lenient =>
        val versions = LenientRegex.findAllMatchIn(version).flatMap(m => Try(m.group(1).toInt).toOption.iterator)
        if (versions.hasNext) {
          val Seq(major, minor, patch) = (versions ++ Iterator(0, 0)).take(3).toSeq
          SemanticVersion(major, minor, patch)
        } else {
          throw e
        }
    }
  }

  /**
    * Comparison based only on the major version number
    */
  object MajorOrdering extends Ordering[SemanticVersion] {
    override def compare(x: SemanticVersion, y: SemanticVersion): Int = Integer.compare(x.major, y.major)
  }

  /**
    * Comparison based only on the major and minor version numbers
    */
  object MinorOrdering extends Ordering[SemanticVersion] {
    override def compare(x: SemanticVersion, y: SemanticVersion): Int = {
      if (x.major > y.major) { 1 } else if (x.major < y.major) { -1 } else
      if (x.minor > y.minor) { 1 } else if (x.minor < y.minor) { -1 } else
      { 0 }
    }
  }

  // see https://semver.org/#spec-item-11
  private def comparePreReleases(first: Option[String], second: Option[String]): Int = {
    if (first.isEmpty && second.isEmpty) { 0 }
    else if (first.isEmpty && second.isDefined) { 1 }
    else if (first.isDefined && second.isEmpty) { -1 }
    else {
      val firstParsed = first.get.split('.').map(parsePreRelease)
      val secondParsed = second.get.split('.').map(parsePreRelease)
      // compare left to right until a difference is found
      val compare = firstParsed.zip(secondParsed).map { case (firstInt, secondInt) =>
        firstInt match {
          case Left(f) =>
            secondInt match {
              case Left(s)  => f.compareTo(s)
              case Right(_) => 1 // numeric values always have lower precedence than non-numeric ones
            }
          case Right(f) =>
            secondInt match {
              case Left(_)  => -1 // numeric values always have lower precedence than non-numeric ones
              case Right(s) => if (f > s) { 1 } else if (f < s) { -1 } else { 0 }
            }
        }
      }
      // if no difference, the longer version is higher precedence
      compare.find(_ != 0).getOrElse(firstParsed.lengthCompare(secondParsed.length))
    }
  }

  // pre-release values can either be numbers or non-numbers, and are treated differently during compare
  private def parsePreRelease(version: String): Either[String, Int] =
    try { Right(version.toInt) } catch { case NonFatal(_) => Left(version) }
}
