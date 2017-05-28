package org.locationtech.geomesa.fs

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Date

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

@RunWith(classOf[JUnitRunner])
class PartitionSchemeTest extends Specification with AllExpectations {

  "DatePartitionScheme" should {
    import scala.collection.JavaConversions._

    val gf = JTSFactoryFinder.getGeometryFactory
    val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
    val sf = new SimpleFeatureImpl(
      List[AnyRef]("test", Integer.valueOf(10), Date.from(Instant.parse("2017-01-03T10:15:30Z")),
        gf.createPoint(new Coordinate(10, 10))), sft, new FeatureIdImpl("1"))

    "partition based on date" >> {
      val ps = new DatePartitionScheme(DateTimeFormatter.ofPattern("yyyy-MM-dd"), sft, "dtg")
      val Partition(p) = ps.getPartition(sf)
      p must be equalTo "2017-01-03"
    }

    "partition based on date with slash delimiter" >> {
      val ps = new DatePartitionScheme(DateTimeFormatter.ofPattern("yyyy/DDD/HH"), sft, "dtg")
      val Partition(p) = ps.getPartition(sf)
      p must be equalTo "2017/003/10"
    }

    "partition based on date with slash delimiter" >> {
      val ps = new DatePartitionScheme(DateTimeFormatter.ofPattern("yyyy/DDD/HH"), sft, "dtg")
      val Partition(p) = ps.getPartition(sf)
      p must be equalTo "2017/003/10"
    }

    "intra-hour partition appropriately" >> {
      val ps = new IntraHourPartitionScheme(15, DateTimeFormatter.ofPattern("yyyy/DDD/HHmm"), sft, "dtg")
      val Partition(p) = ps.getPartition(sf)
      p must be equalTo "2017/003/1015"
    }

    "intra-hour 5 minute partitions" >> {
      val ps = new IntraHourPartitionScheme(5, DateTimeFormatter.ofPattern("yyyy/DDD/HHmm"), sft, "dtg")
      val Partition(p) = ps.getPartition(sf)
      p must be equalTo "2017/003/1015"
    }
  }
}
