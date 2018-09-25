/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common
import java.util.{Optional, ServiceLoader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Abstract implementation that uses FileMetadata for configuration
  */
abstract class FileSystemStorageFactory extends org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory {

  override def load(fc: FileContext,
                    conf: Configuration,
                    root: Path): Optional[FileSystemStorage] = {
    import org.locationtech.geomesa.utils.conversions.JavaConverters._

    StorageMetadata.load(fc, root) // note: this is a cached operation
        .filter(_.getEncoding == getEncoding)
        .map(load(conf, _))
        .asJava
  }

  override def create(fc: FileContext,
                      conf: Configuration,
                      root: Path,
                      sft: SimpleFeatureType): FileSystemStorage = {
    val scheme = PartitionScheme.extractFromSft(sft).getOrElse {
      throw new IllegalArgumentException("SimpleFeatureType does not have partition scheme specified")
    }
    Encodings.getEncoding(sft).filterNot(_.equalsIgnoreCase(getEncoding)).foreach { e =>
      throw new IllegalArgumentException(s"This factory can't create storage with encoding '$e'")
    }
    load(conf, StorageMetadata.create(fc, root, sft, getEncoding, scheme))
  }

  protected def load(conf: Configuration, metadata: StorageMetadata): FileSystemStorage
}

object FileSystemStorageFactory {

  import scala.collection.JavaConverters._

  def factories(): Iterator[org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory] =
    ServiceLoader.load(classOf[org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory]).iterator().asScala

  def factory(encoding: String): org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory =
    factories().find(_.getEncoding.equalsIgnoreCase(encoding)).getOrElse {
      throw new IllegalArgumentException(s"Could not find a factory class for encoding '$encoding'. " +
          s"Factories are available for: ${factories().map(_.getEncoding).mkString(", ")}")
    }

}
