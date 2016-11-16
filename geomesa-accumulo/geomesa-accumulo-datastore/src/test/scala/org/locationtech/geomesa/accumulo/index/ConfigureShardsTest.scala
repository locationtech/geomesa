package org.locationtech.geomesa.accumulo.index

import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.locationtech.geomesa.accumulo.index.SplitArrays._

@RunWith(classOf[JUnitRunner])
class ConfigureShardsTest extends Specification with TestWithDataStore {

  sequential

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val spec = "name:String,dtg:Date,*geom:Polygon:srid=4326;geomesa.z.splits='6'"

  def features(sft: SimpleFeatureType): Seq[ScalaSimpleFeature] = {
    (0 until 10).map { i =>
      val sf = new ScalaSimpleFeature(s"$i", sft)
      sf.setAttributes(Array[AnyRef](s"name$i", s"2010-05-07T$i:00:00.000Z",
        s"POLYGON((40 3$i, 42 3$i, 42 2$i, 40 2$i, 40 3$i))"))
      sf
    }
  }

  "Indexes" should {
    "configure from spec" >> {
      val feats = features(sft)
      addFeatures(feats)
      feats.head.getType.getZShards mustEqual 6
    }

    "configure from code" >> {
      val sftPrivate = sft
      sftPrivate.setZShards(8)
      val feats = features(sftPrivate)
      addFeatures(feats)
      feats.head.getType.getZShards mustEqual 8
    }

    "throw exception" >> {
      val sftPrivate = sft
      sftPrivate.setZShards(128)
      val feats = features(sftPrivate)
      addFeatures(feats)
      val numSplits = sftPrivate.getZShards
      getSplitArray(numSplits) must throwAn[IllegalArgumentException]
    }
  }
}
