#GeoMesa GDALFileHandler
The GDALFileHandler uses the GDAL 2.0.0 JNI wrapper to allow any raster format with a GDAL driver to be 
ingested to the blob store provided it has sufficient metadata to satisfy the blob store schema.

To use the GDALFileHandler the user must first successfully install GDAL 2.0.0 (instructions available here: [http://gdal.org/](http://gdal.org/)).  
Once GDAL 2.0.0 is installed the user must update the `LD_LIBRARY_PATH` system variable to include the GDAL shared libraries in the path.

Logging within the GDALFileHandler should be helpful when diagnosing deployment issues relating to this process.
