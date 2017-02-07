/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import java.util.{List => JList}

import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.matcher.{Expectable, MatchResult, Matcher}

trait SimpleFeatureMatchers extends org.specs2.mutable.Spec with org.specs2.matcher.SequenceMatchersCreation {

  def containFeatures(expected: Set[SimpleFeature]): Matcher[Seq[SimpleFeature]] = new Matcher[Seq[SimpleFeature]] {
    override def apply[S <: Seq[SimpleFeature]](t: Expectable[S]): MatchResult[S] = {
      val actual = t.value
      actual must haveSize(expected.size)
      forall(actual)(a => expected must contain(equalSF(a)))
      ok.asInstanceOf[MatchResult[S]]
    }
  }

  def containSF(expected: SimpleFeature): Matcher[Seq[SimpleFeature]] = new Matcher[Seq[SimpleFeature]] {
    val matcher = equalSF(expected)
    override def apply[S <: Seq[SimpleFeature]](t: Expectable[S]): MatchResult[S] = {
      t.value must contain(matcher)
    }
  }

  def equalSF(expected: SimpleFeature): Matcher[SimpleFeature] = new Matcher[SimpleFeature] {
    override def apply[S <: SimpleFeature](t: Expectable[S]): MatchResult[S] = {
      val sf = t.value
      sf.getID mustEqual expected.getID
      sf.getDefaultGeometry mustEqual expected.getDefaultGeometry
      sf.getAttributes mustEqual expected.getAttributes
      sf.getUserData mustEqual expected.getUserData
      ok.asInstanceOf[MatchResult[S]]
    }
  }

  def equalFeatureHolder(expected: SimpleFeature): Matcher[FeatureHolder] = new Matcher[FeatureHolder] {
    override def apply[S <: FeatureHolder](t: Expectable[S]): MatchResult[S] = {
      t.value.sf must equalSF(expected)
      ok.asInstanceOf[MatchResult[S]]
    }
  }

  def containTheSameFeatureHoldersAs(expected: SimpleFeature*): Matcher[JList[_]] = new Matcher[JList[_]] {

    import scala.collection.JavaConversions._

    // don't care about order so convert to a set
    val expectedSet = expected.toSet

    override def apply[S <: JList[_]](t: Expectable[S]): MatchResult[S] = {
      val actual = t.value
      // actual must not(beNull)
      actual.size() mustEqual expected.size
      actual.asInstanceOf[JList[_]].toSet mustEqual expectedSet
      ok.asInstanceOf[MatchResult[S]]
    }
  }

  def equalGeoMessages(expected: Seq[GeoMessage]): Matcher[Seq[GeoMessage]] = new Matcher[Seq[GeoMessage]] {
    override def apply[S <: Seq[GeoMessage]](t: Expectable[S]): MatchResult[S] = {
      val actual = t.value
      actual must haveLength(expected.length)
      forall(actual)(a => expected must contain(equalGeoMessage(a)))
      ok.asInstanceOf[MatchResult[S]]
    }
  }

  def equalGeoMessage(expected: GeoMessage): Matcher[GeoMessage] = new Matcher[GeoMessage] {
    override def apply[S <: GeoMessage](t: Expectable[S]): MatchResult[S] = {
      expected match {
        case _: Delete => t.value mustEqual expected
        case _: Clear => t.value mustEqual expected
        case CreateOrUpdate(ts, sf) =>
          val actual = t.value
          actual must beAnInstanceOf[CreateOrUpdate]
          actual.timestamp mustEqual ts
          actual.asInstanceOf[CreateOrUpdate].feature must equalSF(sf)
      }
      ok.asInstanceOf[MatchResult[S]]
    }
  }

  // modifies simple features switch to the replay type and adding replay time attribute
  def expect(replayType: SimpleFeatureType, replayTime: Long, sf: SimpleFeature*): Set[SimpleFeature] = {
    val helper = new ReplayTimeHelper(replayType, replayTime)
    sf.map(helper.reType).toSet
  }
}
