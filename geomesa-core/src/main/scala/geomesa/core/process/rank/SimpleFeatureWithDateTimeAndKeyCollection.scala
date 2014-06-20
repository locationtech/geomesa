package geomesa.core.process.rank

import java.io.File
import java.util.Date

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.geotools.data.Transaction
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.SimpleFeatureCollection
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.opengis.feature.simple.SimpleFeature
import geomesa.utils.geotools.Conversions._
import scala.collection.JavaConversions._

import scala.util.Try

/**
 * Created with IntelliJ IDEA.
 * User: kevin
 * Date: 6/18/14
 * Time: 10:39 AM
 */
class GridAxis(val min: Double, val max: Double, val nDivisions: Int) {
  def getRange = {
    val delta = (max - min) / nDivisions.toDouble
    val first = min + (delta / 2.0)
    val last = max - (delta / 2.0)
    first to last by delta
  }

  def getIdx(c: Double): Option[Int] = {
    val delta = (max - min) / nDivisions.toDouble
    if (c < min || c > max) None
    else Some(math.round(math.floor((c - min) / delta)).toInt)
  }
}

class Grid(val box: Envelope, val nDivisions: Int) {
  val xAxis = new GridAxis(box.getMinX, box.getMaxX, nDivisions)
  val yAxis = new GridAxis(box.getMinY, box.getMaxY, nDivisions)

  def getIndexPairsWithLatLons = {
    for {
      x <- xAxis.getRange.zipWithIndex
      y <- yAxis.getRange.zipWithIndex
    } yield ((x._2, y._2), (x._1, y._1))
  }
}

class SimpleFeatureCollectionExt(val sfc: SimpleFeatureCollection) {
  def countAttribute(attr: String) = sfc.features().map { f =>
    f.getAttribute(attr).toString
  }.foldLeft[Map[String,Int]](Map.empty)((m, c) => m + (c -> (m.getOrElse(c, 0) + 1)))
}

object SimpleFeatureCollectionExt {
  implicit def ext2Sfc(e: SimpleFeatureCollectionExt) = e.sfc
  def featureOrderHack(attr: List[Object]) = List(attr.last) ++ attr.slice(0, attr.size() - 1)
}

class SimpleFeatureWithDateTimeAndKeyCollection(override val sfc: SimpleFeatureCollection, val spec: SfSpec)
  extends SimpleFeatureCollectionExt(sfc) {

  def groupByKey = sfc.features().toIterable.map(new SimpleFeatureWithDateTimeAndKey(_, spec)).
    groupBy { sf => sf.getAttribute(spec.keyAttr).toString }

  def countKeys = countAttribute(spec.keyAttr)

  def gridCounts(grid: Grid) = sfc.features().foldLeft(Map[(Int, Int),Map[String,Int]]()) { case(allGridCounts, f) =>
    val coord = f.getDefaultGeometry.asInstanceOf[Geometry].getCentroid.getCoordinate
    val xIndex = grid.xAxis.getIdx(coord.x)
    val yIndex = grid.yAxis.getIdx(coord.y)
    if (xIndex.isDefined && yIndex.isDefined) {
      val key = f.getAttribute(spec.keyAttr).toString
      val pos = (xIndex.get, yIndex.get)
      val gc = allGridCounts.getOrElse(pos, Map[String,Int]())
      val cnt = gc.getOrElse(key, 0) + 1
      allGridCounts ++ Map(pos -> (gc ++ Map(key -> cnt)))
    }
    else {
      allGridCounts
    }
  }
}

case class SfSpec(keyAttr: String, timeAttr: String, dateTimeFormat: DateTimeFormatter =
  DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

class SimpleFeatureWithDateTimeAndKey(val sf: SimpleFeature, val spec: SfSpec) {
  def dateTime: Option[DateTime] = {
    sf.getAttribute(spec.timeAttr) match {
      case s: String => Try(spec.dateTimeFormat.parseDateTime(s)).toOption
      case d: Date => Some(new DateTime(d))
      case _ => None
    }
  }

  def centroidCoordinate = sf.getDefaultGeometry.asInstanceOf[Geometry].getCentroid.getCoordinate

  def getAttribute(s: String) = sf.getAttribute(s)
}
