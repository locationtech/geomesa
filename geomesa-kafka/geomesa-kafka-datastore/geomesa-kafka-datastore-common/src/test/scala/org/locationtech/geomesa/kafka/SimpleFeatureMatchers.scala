/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import java.util.{List => JList}

import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.matcher.{Matcher, ValueCheck}
import org.specs2.mutable.Specification

import scala.collection.JavaConverters._

trait SimpleFeatureMatchers extends Specification {

  def containFeatures(sf: Set[SimpleFeature]): Matcher[Seq[SimpleFeature]] =
    contain(exactly(sf.map(equalSF).toSeq : _*))

  def containSF(expected: SimpleFeature): Matcher[Seq[SimpleFeature]] = {
    val matcher = equalSF(expected)

    seq: Seq[SimpleFeature] => seq.exists(matcher.test)
  }

  def equalSF(expected: SimpleFeature): Matcher[SimpleFeature] = {
    sf: SimpleFeature => {
      sf.getID mustEqual expected.getID
      sf.getDefaultGeometry mustEqual expected.getDefaultGeometry
      sf.getAttributes mustEqual expected.getAttributes
      sf.getUserData mustEqual expected.getUserData
    }
  }

  def equalFeatureHolder(expected: SimpleFeature): Matcher[FeatureHolder] = {
    fh: FeatureHolder => fh.sf must equalSF(expected)
  }

  def featureHolder(expected: SimpleFeature): ValueCheck[FeatureHolder] = {
    fh: FeatureHolder => fh.sf must equalSF(expected)
  }

  def containTheSameFeatureHoldersAs(expected: SimpleFeature*): Matcher[JList[_]] = {
    // don't care about order so convert to a set
    val expectedSet = expected.toSet

    actual: JList[_] => {
      actual must not(beNull)
      actual.size() mustEqual expected.size
      actual.asScala.toSet mustEqual expectedSet
    }
  }

  def containGeoMessages(sfs: Seq[GeoMessage]): Matcher[Seq[GeoMessage]] =
    contain(exactly(sfs.map(equalGeoMessage) : _*))

  def equalGeoMessages(expected: Seq[GeoMessage]): Matcher[Seq[GeoMessage]] =
    contain(exactly(expected.map(equalGeoMessage) : _*))

  def equalGeoMessage(expected: GeoMessage): Matcher[GeoMessage] = expected match {
    case _: Delete => equalTo(expected)
    case _: Clear => equalTo(expected)
    case CreateOrUpdate(ts, sf) => actual: GeoMessage => {
      actual must beAnInstanceOf[CreateOrUpdate]
      actual.timestamp mustEqual ts
      actual.asInstanceOf[CreateOrUpdate].feature must equalSF(sf)
    }
  }

  // modifies simple features switch to the replay type and adding replay time attribute
  def expect(replayType: SimpleFeatureType, replayTime: Long, sf: SimpleFeature*): Set[SimpleFeature] = {
    val helper = new ReplayTimeHelper(replayType, replayTime)
    sf.map(helper.reType).toSet
  }
}
