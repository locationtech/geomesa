/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.util.Date

import org.locationtech.geomesa.curve.BinnedTime
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait SimpleFeatureValidator {
  def name: String
  def init(sft: SimpleFeatureType): Unit
  def validate(sf: SimpleFeature): Boolean
  def lastError: String
}

object ValidatorLoader {
  def createValidator(names: Seq[String], sft: SimpleFeatureType): SimpleFeatureValidator = {
    val validators = names.map {
      case "none" => NoneValidator
      case "has-geo" => new HasGeoValidator
      case "has-dtg" => new HasDtgValidator
      case "z-index" => new ZIndexValidator
      case unk => throw new IllegalArgumentException(s"Unknown validator $unk")
    }

    val validator = if (validators.size == 1) {
      validators.head
    } else {
      new CompositeValidator(validators)
    }
    validator.init(sft)
    validator
  }
}

object NoneValidator extends SimpleFeatureValidator {
  override def name: String = "none"
  override def validate(sf: SimpleFeature): Boolean = true
  override def init(sft: SimpleFeatureType): Unit = {}
  override def lastError: String = null
}

class HasDtgValidator extends SimpleFeatureValidator {
  private var valid: (SimpleFeature) => Boolean = _
  private var lastErr: String = _

  override def name: String = "has-dtg"

  override def init(sft: SimpleFeatureType): Unit = {
    valid = {
      lastErr = null
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
      sft.getDtgIndex match {
        case Some(dtgIdx) => (sf: SimpleFeature) =>
          val res = sf.getAttribute(dtgIdx) != null
          if (!res) {
            lastErr = "Null date attribute"
          }
          res
        case None => (_) => true
      }
    }
  }

  override def validate(sf: SimpleFeature): Boolean = valid(sf)

  override def lastError: String = lastErr
}

class HasGeoValidator extends SimpleFeatureValidator {
  private var valid: (SimpleFeature) => Boolean = _
  private var lastErr: String = _

  override def name: String = "has-geo"

  override def init(sft: SimpleFeatureType): Unit = {
    valid = {
      lastErr = null
      if (sft.getGeometryDescriptor != null) {
        (sf: SimpleFeature) =>
          val res = sf.getDefaultGeometry != null
          if (!res) {
            lastErr = "Null geom attribute"
          }
          res
      } else {
        (_) => true
      }
    }
  }

  override def validate(sf: SimpleFeature): Boolean = valid(sf)

  override def lastError: String = lastErr
}

/**
  * Validates that the geometry fits into a Z2 or Z3 style WGS84 index scheme
  */
class ZIndexValidator extends SimpleFeatureValidator {
  private var valid: (SimpleFeature) => Boolean = _
  private var lastErr: String = _

  override def name: String = "z-index"

  override def init(sft: SimpleFeatureType): Unit = {
    valid = {
      import org.locationtech.geomesa.utils.geotools.Conversions._
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      val maxDate = BinnedTime.maxDate(sft.getZ3Interval).toDate
      val dtgFn: (SimpleFeature) => Boolean = sft.getDtgIndex match {
        case Some(dtgIdx) => (sf: SimpleFeature) =>
          lastErr = null
          if (sf.getAttribute(dtgIdx) == null){
            lastErr = "Null date attribute"
            false
          } else if (sf.get[Date](dtgIdx).before(BinnedTime.ZMinDate)) {
            lastErr = "Date is before Z3 Min Date"
            false
          } else if (sf.get[Date](dtgIdx).after(maxDate)) {
            lastErr = "Date is after Z3 Max Date"
            false
          } else {
            true
          }
        case None =>
          throw new IllegalArgumentException("Z3 validator cannot be used on a type lacking a dtg index")
      }

      val geomFn: (SimpleFeature) => Boolean =
        if (sft.getGeometryDescriptor != null) {
          (sf: SimpleFeature) =>
            lastErr = null
            if (sf.getDefaultGeometry == null) {
              lastErr = "Geom is null"
              false
            } else if (!org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope.contains(sf.geometry.getEnvelopeInternal)) {
              lastErr = "Geom is not contained in whole world envelope"
              false
            } else {
              true
            }
        } else {
          throw new IllegalArgumentException("Z3 validator cannot be used on a type lacking a geom index")
        }

      (sf: SimpleFeature) => dtgFn(sf) && geomFn(sf)
    }
  }

  override def validate(sf: SimpleFeature): Boolean = valid(sf)

  override def lastError: String = lastErr
}

class CompositeValidator(validators: Seq[SimpleFeatureValidator]) extends SimpleFeatureValidator {
  override def validate(sf: SimpleFeature): Boolean = validators.forall(_.validate(sf))
  override def init(sft: SimpleFeatureType): Unit = validators.foreach(_.init(sft))
  override def name: String = s"CompositeValidator[${validators.map(_.name).mkString(", ")}]"
  override def lastError: String = validators.find(_.lastError != null).map(_.lastError).orNull
}