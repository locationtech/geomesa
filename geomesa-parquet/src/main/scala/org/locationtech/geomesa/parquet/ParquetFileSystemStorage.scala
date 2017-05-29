package org.locationtech.geomesa.parquet

import java.{io, util}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.{FileSystemReader, FileSystemStorage, FileSystemStorageFactory, FileSystemWriter}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
  var featureTypes = {
    val iter = fs.listFiles(root, true)
    val result = new Iterator[Try[SimpleFeatureType]] {
      override def hasNext: Boolean = iter.hasNext

      override def next(): Try[SimpleFeatureType] = {
        val f = iter.next()
        if (f.isDirectory) Failure(null)
        else Try {
          val readFooter = ParquetFileReader.readFooter(new Configuration, f.getPath, ParquetMetadataConverter.NO_FILTER)
          val schema = readFooter.getFileMetaData.getSchema
          SFTSchemaConverter.inverse(schema)
        }
      }
    }.toList
    result.collect { case Success(c) => c }.map { sft => sft.getTypeName -> sft }.toMap
  }

  override def listFeatureTypes: util.List[SimpleFeatureType] = {
    import scala.collection.JavaConversions._
    featureTypes.values.toList
  }


  override def getReader(q: Query, part: String): FileSystemReader = {
    val typeName = q.getTypeName
    val sft = featureTypes(typeName)
    val path = new Path(root, new Path(q.getTypeName, part))
    val support = new SimpleFeatureReadSupport(sft)
    val reader = new SimpleFeatureParquetReader(path, support)
    new FileSystemReader {
      override def filterFeatures(q: Query): util.Iterator[SimpleFeature] = {
        new util.Iterator[SimpleFeature] {
          // TODO: push down predicates and partition pruning
          var staged: SimpleFeature = _

          override def next(): SimpleFeature = staged

          override def hasNext: Boolean = {
            staged = null
            var cont = true
            while(staged == null && cont) {
              val f = reader.read()
              if(f == null) {
                cont = false
              } else if(q.getFilter.evaluate(f)) {
                staged = f
              }
            }
            staged != null
          }
        }
      }
    }
  }

  override def getWriter(featureType: String, part: String): FileSystemWriter = new FileSystemWriter {
    private val sft = featureTypes(featureType)
    private val writer = new SimpleFeatureParquetWriter(new Path(root, part), new SimpleFeatureWriteSupport(sft))

    override def writeFeature(f: SimpleFeature): Unit = writer.write(f)

    override def flush(): Unit = {}

    override def close(): Unit = writer.close()
  }

  override def createNewFeatureType(sft: SimpleFeatureType): Unit = {
    // TODO: should we create a single file in here with the SFT
    fs.mkdirs(new Path(root, sft.getTypeName))
  }
}
