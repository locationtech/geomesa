/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.curve.BinnedTime
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait SimpleFeatureValidator {
  def name: String
  def init(sft: SimpleFeatureType): Unit
  def validate(sf: SimpleFeature): Boolean
  def lastError: String
}

object SimpleFeatureValidator {

  def default: SimpleFeatureValidator = {
    val defaultConverterString = SystemProperty("converter.validators", ZIndexValidator.name).get
    println(s" USING CONVERTER VALIDATOR: $defaultConverterString")
    apply(defaultConverterString.split(","))

    //apply(Seq(HasDtgValidator.name, HasGeoValidator.name))
  }

  def apply(names: Seq[String]): SimpleFeatureValidator = {
    val validators = names.map {
      case "none"    => NoneValidator
      case "has-geo" => new HasGeoValidator
      case "has-dtg" => new HasDtgValidator
      case "z-index" => new ZIndexValidator
      case unk => throw new IllegalArgumentException(s"Unknown validator $unk")
    }

    if (validators.isEmpty) {
      NoneValidator
    } else if (validators.lengthCompare(1) == 0) {
      validators.head
    } else {
      new CompositeValidator(validators)
    }
  }

  object HasDtgValidator {
    val name: String = "has-dtg"
  }

  class HasDtgValidator extends SimpleFeatureValidator {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    private var i: Int = -1
    private var lastErr: String = _

    override def name: String = HasDtgValidator.name

    override def init(sft: SimpleFeatureType): Unit = {
      lastErr = null
      i = sft.getDtgIndex.getOrElse(-1)
    }

    override def validate(sf: SimpleFeature): Boolean = {
      if (i == -1) { true } else {
        val res = sf.getAttribute(i) != null
        lastErr = if (res) { null } else { "Null date attribute" }
        res
      }
    }

    override def lastError: String = lastErr
  }

  object HasGeoValidator {
    val name: String = "has-geo"
  }

  class HasGeoValidator extends SimpleFeatureValidator {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    private var i: Int = -1
    private var lastErr: String = _

    override def name: String = HasGeoValidator.name

    override def init(sft: SimpleFeatureType): Unit = {
      lastErr = null
      i = sft.getGeomIndex
    }

    override def validate(sf: SimpleFeature): Boolean = {
      if (i == -1) { true } else {
        val res = sf.getAttribute(i) != null
        lastErr = if (res) { null } else { "Null geometry attribute" }
        res
      }
    }

    override def lastError: String = lastErr
  }

  object ZIndexValidator {
    val name: String = "z-index"
  }

  /**
    * Validates that the geometry fits into a Z2 or Z3 style WGS84 index scheme
    */
  class ZIndexValidator extends SimpleFeatureValidator {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    import org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope

    private var dtgIndex: Int = -1
    private var geomIndex: Int = -1
    private var minDate: Date = _
    private var maxDate: Date = _
    private var lastErr: String = _

    override def name: String = ZIndexValidator.name

    override def init(sft: SimpleFeatureType): Unit = {
      lastErr = null
      dtgIndex = sft.getDtgIndex.getOrElse(-1)
      geomIndex = sft.getGeomIndex
      minDate = Date.from(BinnedTime.ZMinDate.toInstant)
      maxDate = Date.from(BinnedTime.maxDate(sft.getZ3Interval).toInstant)

      if (dtgIndex == -1 || geomIndex == -1) {
        throw new IllegalArgumentException("Z3 validator cannot be used on a type lacking a date or geometry")
      }
    }

    override def validate(sf: SimpleFeature): Boolean = {
      lastErr = null
      val date = sf.getAttribute(dtgIndex).asInstanceOf[Date]
      val geom = sf.getAttribute(geomIndex).asInstanceOf[Geometry]
      if (date == null) {
        lastErr = "Null date attribute"
        false
      } else if (geom == null) {
        lastErr = "Null geometry attribute"
        false
      } else if (date.before(minDate)) {
        lastErr = s"Date is before Z3 Min Date ($minDate)"
        false
      } else if (date.after(maxDate)) {
        lastErr = s"Date is after Z3 Max Date ($maxDate)"
        false
      } else if (!wholeWorldEnvelope.contains(geom.getEnvelopeInternal)) {
        lastErr = s"Geometry exceeds world bounds ($wholeWorldEnvelope)"
        false
      } else {
        true
      }
    }

    override def lastError: String = lastErr
  }

  class CompositeValidator(validators: Seq[SimpleFeatureValidator]) extends SimpleFeatureValidator {
    override def validate(sf: SimpleFeature): Boolean = validators.forall(_.validate(sf))
    override def init(sft: SimpleFeatureType): Unit = validators.foreach(_.init(sft))
    override def name: String = validators.map(_.name).mkString(",")
    override def lastError: String = validators.find(_.lastError != null).map(_.lastError).orNull
  }

  object NoneValidator extends SimpleFeatureValidator {
    override def name: String = "none"
    override def validate(sf: SimpleFeature): Boolean = true
    override def init(sft: SimpleFeatureType): Unit = {}
    override def lastError: String = null
  }
}
