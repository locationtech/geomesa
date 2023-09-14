s/import org\.geotools\.data\._/\0\nimport org.geotools.data.api._/
s/org\.opengis\.geometry\.BoundingBox/org.geotools.api.geometry.BoundingBox/g
s/org\.opengis\.geometry\.Geometry/org.locationtech.jts.geom.Geometry/g
s/FilterFactory2/FilterFactory/g
s/ReferencedEnvelope\.create/ReferencedEnvelope.envelope/g
s/org\.geotools\.data\.DataAccessFactory\.Param/org.geotools.api.data.DataAccessFactory.Param/g

s/org\.geotools\.data\.DataStore/org.geotools.api.data.DataStore/g
s/import org\.geotools\.data\.DataStore/import org.geotools.api.data.DataStore/
s/import org\.geotools\.data\.\{DataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStore\}/import org.geotools.api.data.DataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DataStore\}/import org.geotools.api.data.DataStore/

s/import org\.geotools\.data\.DataStoreFactorySpi/import org.geotools.api.data.DataStoreFactorySpi/
s/import org\.geotools\.data\.\{DataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFactorySpi\}/import org.geotools.api.data.DataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStoreFactorySpi\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DataStoreFactorySpi\}/import org.geotools.api.data.DataStoreFactorySpi/
s/import org\.geotools\.data\.DataStoreFinder/import org.geotools.api.data.DataStoreFinder/
s/import org\.geotools\.data\.\{DataStoreFinder, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStoreFinder\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFinder\}/import org.geotools.api.data.DataStoreFinder\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFinder, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStoreFinder\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DataStoreFinder\}/import org.geotools.api.data.DataStoreFinder/
s/import org\.geotools\.data\.DelegatingFeatureReader/import org.geotools.api.data.DelegatingFeatureReader/
s/import org\.geotools\.data\.\{DelegatingFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DelegatingFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DelegatingFeatureReader\}/import org.geotools.api.data.DelegatingFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DelegatingFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DelegatingFeatureReader\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DelegatingFeatureReader\}/import org.geotools.api.data.DelegatingFeatureReader/
s/import org\.geotools\.data\.FeatureEvent/import org.geotools.api.data.FeatureEvent/
s/import org\.geotools\.data\.\{FeatureEvent, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureEvent\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureEvent\}/import org.geotools.api.data.FeatureEvent\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureEvent, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureEvent\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureEvent\}/import org.geotools.api.data.FeatureEvent/
s/import org\.geotools\.data\.FeatureListener/import org.geotools.api.data.FeatureListener/
s/import org\.geotools\.data\.\{FeatureListener, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureListener\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureListener\}/import org.geotools.api.data.FeatureListener\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureListener, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureListener\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureListener\}/import org.geotools.api.data.FeatureListener/
s/import org\.geotools\.data\.FeatureLock/import org.geotools.api.data.FeatureLock/
s/import org\.geotools\.data\.\{FeatureLock, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureLock\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLock\}/import org.geotools.api.data.FeatureLock\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLock, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureLock\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureLock\}/import org.geotools.api.data.FeatureLock/
s/import org\.geotools\.data\.FeatureLocking/import org.geotools.api.data.FeatureLocking/
s/import org\.geotools\.data\.\{FeatureLocking, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureLocking\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLocking\}/import org.geotools.api.data.FeatureLocking\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLocking, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureLocking\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureLocking\}/import org.geotools.api.data.FeatureLocking/
s/import org\.geotools\.data\.FeatureReader/import org.geotools.api.data.FeatureReader/
s/import org\.geotools\.data\.\{FeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureReader\}/import org.geotools.api.data.FeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureReader\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureReader\}/import org.geotools.api.data.FeatureReader/
s/import org\.geotools\.data\.FeatureSource/import org.geotools.api.data.FeatureSource/
s/import org\.geotools\.data\.\{FeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureSource\}/import org.geotools.api.data.FeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureSource\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureSource\}/import org.geotools.api.data.FeatureSource/
s/import org\.geotools\.data\.FeatureStore/import org.geotools.api.data.FeatureStore/
s/import org\.geotools\.data\.\{FeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureStore\}/import org.geotools.api.data.FeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureStore\}/import org.geotools.api.data.FeatureStore/
s/import org\.geotools\.data\.FeatureWriter/import org.geotools.api.data.FeatureWriter/
s/import org\.geotools\.data\.\{FeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureWriter\}/import org.geotools.api.data.FeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureWriter\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureWriter\}/import org.geotools.api.data.FeatureWriter/
s/import org\.geotools\.data\.FileDataStore/import org.geotools.api.data.FileDataStore/
s/import org\.geotools\.data\.\{FileDataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileDataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStore\}/import org.geotools.api.data.FileDataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileDataStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FileDataStore\}/import org.geotools.api.data.FileDataStore/
s/import org\.geotools\.data\.FileDataStoreFactorySpi/import org.geotools.api.data.FileDataStoreFactorySpi/
s/import org\.geotools\.data\.\{FileDataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileDataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStoreFactorySpi\}/import org.geotools.api.data.FileDataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileDataStoreFactorySpi\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FileDataStoreFactorySpi\}/import org.geotools.api.data.FileDataStoreFactorySpi/
s/import org\.geotools\.data\.FileStoreFactory/import org.geotools.api.data.FileStoreFactory/
s/import org\.geotools\.data\.\{FileStoreFactory, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileStoreFactory\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileStoreFactory\}/import org.geotools.api.data.FileStoreFactory\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileStoreFactory, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileStoreFactory\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FileStoreFactory\}/import org.geotools.api.data.FileStoreFactory/
s/import org\.geotools\.data\.Join/import org.geotools.api.data.Join/
s/import org\.geotools\.data\.\{Join, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Join\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Join\}/import org.geotools.api.data.Join\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Join, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Join\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Join\}/import org.geotools.api.data.Join/
s/import org\.geotools\.data\.LockingManager/import org.geotools.api.data.LockingManager/
s/import org\.geotools\.data\.\{LockingManager, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.LockingManager\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), LockingManager\}/import org.geotools.api.data.LockingManager\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), LockingManager, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.LockingManager\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{LockingManager\}/import org.geotools.api.data.LockingManager/
s/import org\.geotools\.data\.Parameter/import org.geotools.api.data.Parameter/
s/import org\.geotools\.data\.\{Parameter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Parameter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Parameter\}/import org.geotools.api.data.Parameter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Parameter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Parameter\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Parameter\}/import org.geotools.api.data.Parameter/
s/import org\.geotools\.data\.Query/import org.geotools.api.data.Query/
s/import org\.geotools\.data\.\{Query, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Query\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Query\}/import org.geotools.api.data.Query\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Query, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Query\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Query\}/import org.geotools.api.data.Query/
s/import org\.geotools\.data\.QueryCapabilities/import org.geotools.api.data.QueryCapabilities/
s/import org\.geotools\.data\.\{QueryCapabilities, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.QueryCapabilities\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), QueryCapabilities\}/import org.geotools.api.data.QueryCapabilities\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), QueryCapabilities, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.QueryCapabilities\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{QueryCapabilities\}/import org.geotools.api.data.QueryCapabilities/
s/import org\.geotools\.data\.Repository/import org.geotools.api.data.Repository/
s/import org\.geotools\.data\.\{Repository, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Repository\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Repository\}/import org.geotools.api.data.Repository\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Repository, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Repository\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Repository\}/import org.geotools.api.data.Repository/
s/import org\.geotools\.data\.ResourceInfo/import org.geotools.api.data.ResourceInfo/
s/import org\.geotools\.data\.\{ResourceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.ResourceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ResourceInfo\}/import org.geotools.api.data.ResourceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ResourceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.ResourceInfo\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{ResourceInfo\}/import org.geotools.api.data.ResourceInfo/
s/import org\.geotools\.data\.ServiceInfo/import org.geotools.api.data.ServiceInfo/
s/import org\.geotools\.data\.\{ServiceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.ServiceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ServiceInfo\}/import org.geotools.api.data.ServiceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ServiceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.ServiceInfo\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{ServiceInfo\}/import org.geotools.api.data.ServiceInfo/
s/import org\.geotools\.data\.Transaction/import org.geotools.api.data.Transaction/
s/import org\.geotools\.data\.\{Transaction, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Transaction\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Transaction\}/import org.geotools.api.data.Transaction\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Transaction, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Transaction\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Transaction\}/import org.geotools.api.data.Transaction/

s/import org\.geotools\.data\.simple\.SimpleFeatureReader/import org.geotools.api.data.SimpleFeatureReader/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureReader\}/import org.geotools.api.data.SimpleFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureReader\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureReader\}/import org.geotools.api.data.SimpleFeatureReader/
s/import org\.geotools\.data\.simple\.SimpleFeatureSource/import org.geotools.api.data.SimpleFeatureSource/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureSource\}/import org.geotools.api.data.SimpleFeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureSource\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureSource\}/import org.geotools.api.data.SimpleFeatureSource/
s/import org\.geotools\.data\.simple\.SimpleFeatureStore/import org.geotools.api.data.SimpleFeatureStore/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureStore\}/import org.geotools.api.data.SimpleFeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureStore\}/import org.geotools.api.data.SimpleFeatureStore/
s/import org\.geotools\.data\.simple\.SimpleFeatureWriter/import org.geotools.api.data.SimpleFeatureWriter/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureWriter\}/import org.geotools.api.data.SimpleFeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureWriter\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureWriter\}/import org.geotools.api.data.SimpleFeatureWriter/

s/org.opengis/org.geotools.api/g
