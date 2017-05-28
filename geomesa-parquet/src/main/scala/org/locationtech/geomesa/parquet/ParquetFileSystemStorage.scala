package org.locationtech.geomesa.parquet

import java.{io, util}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, FileSystemStorageFactory}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.{Failure, Success, Try}

class ParquetFileSystemStorageFactory extends FileSystemStorageFactory {
  override def canProcess(params: util.Map[String, io.Serializable]): Boolean = {
    params.containsKey("fs.encoding") && params.get("fs.encoding").asInstanceOf[String].equals("parquet")
  }

  override def build(params: util.Map[String, io.Serializable]): FileSystemStorage = {
    val path = params.get("fs.path").asInstanceOf[String]
    val root = new Path(path)
    // TODO: how do we thread configuration through
    new ParquetFileSystemStorage(root, root.getFileSystem(new Configuration))
  }
}

/**
  * Created by anthony on 5/28/17.
  */
class ParquetFileSystemStorage(root: Path, fs: FileSystem) extends FileSystemStorage {
  override val getSimpleFeatureType: SimpleFeatureType = {
    val iter = fs.listFiles(root, true)
    val result = new Iterator[Try[SimpleFeatureType]] {
      override def hasNext: Boolean = iter.hasNext

      override def next(): Try[SimpleFeatureType] = {
        val f = iter.next()
        if(f.isDirectory) Failure(null)
        else Try {
          val readFooter = ParquetFileReader.readFooter(new Configuration, f.getPath, ParquetMetadataConverter.NO_FILTER)
          val schema = readFooter.getFileMetaData.getSchema
          SFTSchemaConverter.inverse(schema)
        }
      }
    }
    result.collectFirst { case Success(x) => x }.getOrElse(throw new RuntimeException("Could not find SimpleFeatureType"))
  }

  override def query(f: Filter): util.Iterator[SimpleFeature] = {
    // TODO: implement filtering
    val reader = new SimpleFeatureParquetReader(root, new SimpleFeatureReadSupport(getSimpleFeatureType))
    new util.Iterator[SimpleFeature] {
      var staged: SimpleFeature = _

      override def next(): SimpleFeature = staged

      override def hasNext: Boolean = {
        staged = reader.read()
        staged != null
      }
    }
  }
}
