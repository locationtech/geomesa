/import org.geotools.data.api._/d
s/import org\.geotools\.data\._/\0\nimport org.geotools.data.api._/
s/org\.opengis\.geometry\.BoundingBox/org.geotools.api.geometry.BoundingBox/g
s/org\.opengis\.geometry\.Geometry/org.locationtech.jts.geom.Geometry/g
s/FilterFactory2/FilterFactory/g
s/ReferencedEnvelope\.create/ReferencedEnvelope.envelope/g
s/org\.geotools\.data\.DataAccessFactory\.Param/org.geotools.data.api.DataAccessFactory.Param/g
s/DirectPosition/Position/g

s/org\.geotools\.data\.DataStore/org.geotools.data.api.DataStore/g
s/import org\.geotools\.data\.\{DataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.DataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStore\}/import org.geotools.data.api.DataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.DataStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DataStore\}/import org.geotools.data.api.DataStore/

s/org\.geotools\.data\.Parameter/org.geotools.data.api.Parameter/g
s/import org\.geotools\.data\.\{Parameter, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Parameter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Parameter\}/import org.geotools.data.api.Parameter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Parameter, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Parameter\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Parameter\}/import org.geotools.data.api.Parameter/

s/org.geotools.data.DataStoreFactorySpi/org.geotools.data.api.DataStoreFactorySpi/g
s/import org\.geotools\.data\.\{DataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.DataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFactorySpi\}/import org.geotools.data.api.DataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.DataStoreFactorySpi\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DataStoreFactorySpi\}/import org.geotools.data.api.DataStoreFactorySpi/

s/org.geotools.data.DataStoreFinder/org.geotools.data.api.DataStoreFinder/g
s/import org\.geotools\.data\.\{DataStoreFinder, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.DataStoreFinder\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFinder\}/import org.geotools.data.api.DataStoreFinder\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFinder, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.DataStoreFinder\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DataStoreFinder\}/import org.geotools.data.api.DataStoreFinder/

s/org.geotools.data.DelegatingFeatureReader/org.geotools.data.api.DelegatingFeatureReader/g
s/import org\.geotools\.data\.\{DelegatingFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.DelegatingFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DelegatingFeatureReader\}/import org.geotools.data.api.DelegatingFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DelegatingFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.DelegatingFeatureReader\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DelegatingFeatureReader\}/import org.geotools.data.api.DelegatingFeatureReader/

s/org.geotools.data.FeatureEvent/org.geotools.data.api.FeatureEvent/g
s/import org\.geotools\.data\.\{FeatureEvent, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureEvent\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureEvent\}/import org.geotools.data.api.FeatureEvent\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureEvent, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureEvent\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureEvent\}/import org.geotools.data.api.FeatureEvent/

s/org.geotools.data.FeatureListener/org.geotools.data.api.FeatureListener/g
s/import org\.geotools\.data\.\{FeatureListener, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureListener\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureListener\}/import org.geotools.data.api.FeatureListener\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureListener, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureListener\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureListener\}/import org.geotools.data.api.FeatureListener/

s/org.geotools.data.FeatureLock/org.geotools.data.api.FeatureLock/g
s/import org\.geotools\.data\.\{FeatureLock, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureLock\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLock\}/import org.geotools.data.api.FeatureLock\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLock, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureLock\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureLock\}/import org.geotools.data.api.FeatureLock/

s/org.geotools.data.FeatureLocking/org.geotools.data.api.FeatureLocking/g
s/import org\.geotools\.data\.\{FeatureLocking, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureLocking\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLocking\}/import org.geotools.data.api.FeatureLocking\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLocking, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureLocking\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureLocking\}/import org.geotools.data.api.FeatureLocking/

s/org.geotools.data.FeatureReader/org.geotools.data.api.FeatureReader/g
s/import org\.geotools\.data\.\{FeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureReader\}/import org.geotools.data.api.FeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureReader\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureReader\}/import org.geotools.data.api.FeatureReader/

s/org.geotools.data.FeatureSource/org.geotools.data.api.FeatureSource/g
s/import org\.geotools\.data\.\{FeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureSource\}/import org.geotools.data.api.FeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureSource\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureSource\}/import org.geotools.data.api.FeatureSource/

s/org.geotools.data.FeatureStore/org.geotools.data.api.FeatureStore/g
s/import org\.geotools\.data\.\{FeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureStore\}/import org.geotools.data.api.FeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureStore\}/import org.geotools.data.api.FeatureStore/

s/org.geotools.data.FeatureWriter/org.geotools.data.api.FeatureWriter/g
s/import org\.geotools\.data\.\{FeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureWriter\}/import org.geotools.data.api.FeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FeatureWriter\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureWriter\}/import org.geotools.data.api.FeatureWriter/

s/org.geotools.data.FileDataStore/org.geotools.data.api.FileDataStore/g
s/import org\.geotools\.data\.\{FileDataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FileDataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStore\}/import org.geotools.data.api.FileDataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FileDataStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FileDataStore\}/import org.geotools.data.api.FileDataStore/

s/org.geotools.data.FileDataStoreFactorySpi/org.geotools.data.api.FileDataStoreFactorySpi/g
s/import org\.geotools\.data\.\{FileDataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FileDataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStoreFactorySpi\}/import org.geotools.data.api.FileDataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FileDataStoreFactorySpi\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FileDataStoreFactorySpi\}/import org.geotools.data.api.FileDataStoreFactorySpi/

s/org.geotools.data.FileStoreFactory/org.geotools.data.api.FileStoreFactory/g
s/import org\.geotools\.data\.\{FileStoreFactory, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FileStoreFactory\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileStoreFactory\}/import org.geotools.data.api.FileStoreFactory\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileStoreFactory, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.FileStoreFactory\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FileStoreFactory\}/import org.geotools.data.api.FileStoreFactory/

s/org.geotools.data.Join/org.geotools.data.api.Join/g
s/import org\.geotools\.data\.\{Join, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Join\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Join\}/import org.geotools.data.api.Join\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Join, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Join\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Join\}/import org.geotools.data.api.Join/

s/org.geotools.data.LockingManager/org.geotools.data.api.LockingManager/g
s/import org\.geotools\.data\.\{LockingManager, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.LockingManager\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), LockingManager\}/import org.geotools.data.api.LockingManager\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), LockingManager, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.LockingManager\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{LockingManager\}/import org.geotools.data.api.LockingManager/

s/org.geotools.data.Parameter/org.geotools.data.api.Parameter/g
s/import org\.geotools\.data\.\{Parameter, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Parameter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Parameter\}/import org.geotools.data.api.Parameter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Parameter, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Parameter\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Parameter\}/import org.geotools.data.api.Parameter/

s/org.geotools.data.Query/org.geotools.data.api.Query/g
s/import org\.geotools\.data\.\{Query, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Query\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Query\}/import org.geotools.data.api.Query\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Query, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Query\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Query\}/import org.geotools.data.api.Query/

s/org.geotools.data.QueryCapabilities/org.geotools.data.api.QueryCapabilities/g
s/import org\.geotools\.data\.\{QueryCapabilities, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.QueryCapabilities\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), QueryCapabilities\}/import org.geotools.data.api.QueryCapabilities\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), QueryCapabilities, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.QueryCapabilities\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{QueryCapabilities\}/import org.geotools.data.api.QueryCapabilities/

s/org.geotools.data.Repository/org.geotools.data.api.Repository/g
s/import org\.geotools\.data\.\{Repository, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Repository\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Repository\}/import org.geotools.data.api.Repository\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Repository, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Repository\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Repository\}/import org.geotools.data.api.Repository/

s/org.geotools.data.ResourceInfo/org.geotools.data.api.ResourceInfo/g
s/import org\.geotools\.data\.\{ResourceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.ResourceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ResourceInfo\}/import org.geotools.data.api.ResourceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ResourceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.ResourceInfo\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{ResourceInfo\}/import org.geotools.data.api.ResourceInfo/

s/org.geotools.data.ServiceInfo/org.geotools.data.api.ServiceInfo/g
s/import org\.geotools\.data\.\{ServiceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.ServiceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ServiceInfo\}/import org.geotools.data.api.ServiceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ServiceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.ServiceInfo\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{ServiceInfo\}/import org.geotools.data.api.ServiceInfo/

s/org.geotools.data.Transaction/org.geotools.data.api.Transaction/g
s/import org\.geotools\.data\.\{Transaction, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Transaction\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Transaction\}/import org.geotools.data.api.Transaction\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Transaction, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.Transaction\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Transaction\}/import org.geotools.data.api.Transaction/

s/org.geotools.data.simple.SimpleFeatureReader/org.geotools.data.api.SimpleFeatureReader/g
s/import org\.geotools\.data\.simple\.\{SimpleFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.SimpleFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureReader\}/import org.geotools.data.api.SimpleFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.SimpleFeatureReader\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureReader\}/import org.geotools.data.api.SimpleFeatureReader/

s/org.geotools.data.simple.SimpleFeatureSource/org.geotools.data.api.SimpleFeatureSource/g
s/import org\.geotools\.data\.simple\.\{SimpleFeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.SimpleFeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureSource\}/import org.geotools.data.api.SimpleFeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.SimpleFeatureSource\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureSource\}/import org.geotools.data.api.SimpleFeatureSource/

s/org.geotools.data.simple.SimpleFeatureStore/org.geotools.data.api.SimpleFeatureStore/g
s/import org\.geotools\.data\.simple\.\{SimpleFeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.SimpleFeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureStore\}/import org.geotools.data.api.SimpleFeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.SimpleFeatureStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureStore\}/import org.geotools.data.api.SimpleFeatureStore/

s/org.geotools.data.simple.SimpleFeatureWriter/org.geotools.data.api.SimpleFeatureWriter/g
s/import org\.geotools\.data\.simple\.\{SimpleFeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.SimpleFeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureWriter\}/import org.geotools.data.api.SimpleFeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.data.api.SimpleFeatureWriter\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureWriter\}/import org.geotools.data.api.SimpleFeatureWriter/

s/org.opengis/org.geotools.api/g
