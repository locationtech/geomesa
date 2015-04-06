package org.locationtech.geomesa.feature

import org.locationtech.geomesa.feature.EncodingOption.EncodingOption

/**
 * Created by mmatz on 4/6/15.
 */
object EncodingOptions {

  /**
   * Same as ``Set.empty`` but provides better readability.
   *
   * e.g. ``SimpleFeatureEncoder(sft, FeatureEncoding.AVRO, EncodingOptions.none)``
   */
  val none: Set[EncodingOption] = Set.empty
}

/**
 * Options to be applied when encoding.  The same options must be specified when decoding.
 */
object EncodingOption extends Enumeration {
  type EncodingOption = Value

  /**
   * If this [[EncodingOption]] is specified then the security marking associated with the sample feature will be
   * serialized and deserialized.
   */
  val WITH_VISIBILITIES = Value("withVisibilities")
}