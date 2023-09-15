/import org.geotools.api.data._/d
s/import org\.geotools\.data\._/\0\nimport org.geotools.api.data._/
s/org\.opengis\.geometry\.BoundingBox/org.geotools.api.geometry.BoundingBox/g
s/org\.opengis\.geometry\.Geometry/org.locationtech.jts.geom.Geometry/g
s/FilterFactory2/FilterFactory/g
s/ReferencedEnvelope\.create/ReferencedEnvelope.envelope/g
s/org\.geotools\.data\.DataAccessFactory\.Param/org.geotools.api.data.DataAccessFactory.Param/g
s/DirectPosition/Position/g

s/org\.geotools\.data\.DataStore/org.geotools.api.data.DataStore/g
s/import org\.geotools\.data\.\{DataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStore\}/import org.geotools.api.data.DataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DataStore\}/import org.geotools.api.data.DataStore/

s/org\.geotools\.data\.Parameter/org.geotools.api.data.Parameter/g
s/import org\.geotools\.data\.\{Parameter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Parameter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Parameter\}/import org.geotools.api.data.Parameter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Parameter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Parameter\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Parameter\}/import org.geotools.api.data.Parameter/

s/org.geotools.data.DataStoreFactorySpi/org.geotools.api.data.DataStoreFactorySpi/g
s/import org\.geotools\.data\.\{DataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFactorySpi\}/import org.geotools.api.data.DataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStoreFactorySpi\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DataStoreFactorySpi\}/import org.geotools.api.data.DataStoreFactorySpi/

s/org.geotools.data.DataStoreFinder/org.geotools.api.data.DataStoreFinder/g
s/import org\.geotools\.data\.\{DataStoreFinder, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStoreFinder\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFinder\}/import org.geotools.api.data.DataStoreFinder\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DataStoreFinder, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DataStoreFinder\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DataStoreFinder\}/import org.geotools.api.data.DataStoreFinder/

s/org.geotools.data.DelegatingFeatureReader/org.geotools.api.data.DelegatingFeatureReader/g
s/import org\.geotools\.data\.\{DelegatingFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DelegatingFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DelegatingFeatureReader\}/import org.geotools.api.data.DelegatingFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), DelegatingFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.DelegatingFeatureReader\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{DelegatingFeatureReader\}/import org.geotools.api.data.DelegatingFeatureReader/

s/org.geotools.data.FeatureEvent/org.geotools.api.data.FeatureEvent/g
s/import org\.geotools\.data\.\{FeatureEvent, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureEvent\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureEvent\}/import org.geotools.api.data.FeatureEvent\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureEvent, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureEvent\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureEvent\}/import org.geotools.api.data.FeatureEvent/

s/org.geotools.data.FeatureListener/org.geotools.api.data.FeatureListener/g
s/import org\.geotools\.data\.\{FeatureListener, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureListener\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureListener\}/import org.geotools.api.data.FeatureListener\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureListener, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureListener\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureListener\}/import org.geotools.api.data.FeatureListener/

s/org.geotools.data.FeatureLock/org.geotools.api.data.FeatureLock/g
s/import org\.geotools\.data\.\{FeatureLock, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureLock\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLock\}/import org.geotools.api.data.FeatureLock\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLock, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureLock\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureLock\}/import org.geotools.api.data.FeatureLock/

s/org.geotools.data.FeatureLocking/org.geotools.api.data.FeatureLocking/g
s/import org\.geotools\.data\.\{FeatureLocking, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureLocking\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLocking\}/import org.geotools.api.data.FeatureLocking\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureLocking, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureLocking\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureLocking\}/import org.geotools.api.data.FeatureLocking/

s/org.geotools.data.FeatureReader/org.geotools.api.data.FeatureReader/g
s/import org\.geotools\.data\.\{FeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureReader\}/import org.geotools.api.data.FeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureReader\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureReader\}/import org.geotools.api.data.FeatureReader/

s/org.geotools.data.FeatureSource/org.geotools.api.data.FeatureSource/g
s/import org\.geotools\.data\.\{FeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureSource\}/import org.geotools.api.data.FeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureSource\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureSource\}/import org.geotools.api.data.FeatureSource/

s/org.geotools.data.FeatureStore/org.geotools.api.data.FeatureStore/g
s/import org\.geotools\.data\.\{FeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureStore\}/import org.geotools.api.data.FeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureStore\}/import org.geotools.api.data.FeatureStore/

s/org.geotools.data.FeatureWriter/org.geotools.api.data.FeatureWriter/g
s/import org\.geotools\.data\.\{FeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureWriter\}/import org.geotools.api.data.FeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FeatureWriter\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FeatureWriter\}/import org.geotools.api.data.FeatureWriter/

s/org.geotools.data.FileDataStore/org.geotools.api.data.FileDataStore/g
s/import org\.geotools\.data\.\{FileDataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileDataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStore\}/import org.geotools.api.data.FileDataStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileDataStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FileDataStore\}/import org.geotools.api.data.FileDataStore/

s/org.geotools.data.FileDataStoreFactorySpi/org.geotools.api.data.FileDataStoreFactorySpi/g
s/import org\.geotools\.data\.\{FileDataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileDataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStoreFactorySpi\}/import org.geotools.api.data.FileDataStoreFactorySpi\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileDataStoreFactorySpi, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileDataStoreFactorySpi\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FileDataStoreFactorySpi\}/import org.geotools.api.data.FileDataStoreFactorySpi/

s/org.geotools.data.FileStoreFactory/org.geotools.api.data.FileStoreFactory/g
s/import org\.geotools\.data\.\{FileStoreFactory, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileStoreFactory\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileStoreFactory\}/import org.geotools.api.data.FileStoreFactory\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), FileStoreFactory, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.FileStoreFactory\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{FileStoreFactory\}/import org.geotools.api.data.FileStoreFactory/

s/org.geotools.data.Join/org.geotools.api.data.Join/g
s/import org\.geotools\.data\.\{Join, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Join\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Join\}/import org.geotools.api.data.Join\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Join, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Join\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Join\}/import org.geotools.api.data.Join/

s/org.geotools.data.LockingManager/org.geotools.api.data.LockingManager/g
s/import org\.geotools\.data\.\{LockingManager, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.LockingManager\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), LockingManager\}/import org.geotools.api.data.LockingManager\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), LockingManager, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.LockingManager\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{LockingManager\}/import org.geotools.api.data.LockingManager/

s/org.geotools.data.Parameter/org.geotools.api.data.Parameter/g
s/import org\.geotools\.data\.\{Parameter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Parameter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Parameter\}/import org.geotools.api.data.Parameter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Parameter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Parameter\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Parameter\}/import org.geotools.api.data.Parameter/

s/org.geotools.data.Query/org.geotools.api.data.Query/g
s/import org\.geotools\.data\.\{Query, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Query\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Query\}/import org.geotools.api.data.Query\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Query, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Query\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Query\}/import org.geotools.api.data.Query/

s/org.geotools.data.QueryCapabilities/org.geotools.api.data.QueryCapabilities/g
s/import org\.geotools\.data\.\{QueryCapabilities, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.QueryCapabilities\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), QueryCapabilities\}/import org.geotools.api.data.QueryCapabilities\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), QueryCapabilities, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.QueryCapabilities\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{QueryCapabilities\}/import org.geotools.api.data.QueryCapabilities/

s/org.geotools.data.Repository/org.geotools.api.data.Repository/g
s/import org\.geotools\.data\.\{Repository, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Repository\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Repository\}/import org.geotools.api.data.Repository\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Repository, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Repository\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Repository\}/import org.geotools.api.data.Repository/

s/org.geotools.data.ResourceInfo/org.geotools.api.data.ResourceInfo/g
s/import org\.geotools\.data\.\{ResourceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.ResourceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ResourceInfo\}/import org.geotools.api.data.ResourceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ResourceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.ResourceInfo\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{ResourceInfo\}/import org.geotools.api.data.ResourceInfo/

s/org.geotools.data.ServiceInfo/org.geotools.api.data.ServiceInfo/g
s/import org\.geotools\.data\.\{ServiceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.ServiceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ServiceInfo\}/import org.geotools.api.data.ServiceInfo\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), ServiceInfo, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.ServiceInfo\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{ServiceInfo\}/import org.geotools.api.data.ServiceInfo/

s/org.geotools.data.Transaction/org.geotools.api.data.Transaction/g
s/import org\.geotools\.data\.\{Transaction, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Transaction\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Transaction\}/import org.geotools.api.data.Transaction\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.\{([a-zA-Z0-9, ]*), Transaction, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.Transaction\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.\{Transaction\}/import org.geotools.api.data.Transaction/

s/org.geotools.data.simple.SimpleFeatureReader/org.geotools.api.data.SimpleFeatureReader/g
s/import org\.geotools\.data\.simple\.\{SimpleFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureReader\}/import org.geotools.api.data.SimpleFeatureReader\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureReader, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureReader\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureReader\}/import org.geotools.api.data.SimpleFeatureReader/

s/org.geotools.data.simple.SimpleFeatureSource/org.geotools.api.data.SimpleFeatureSource/g
s/import org\.geotools\.data\.simple\.\{SimpleFeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureSource\}/import org.geotools.api.data.SimpleFeatureSource\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureSource, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureSource\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureSource\}/import org.geotools.api.data.SimpleFeatureSource/

s/org.geotools.data.simple.SimpleFeatureStore/org.geotools.api.data.SimpleFeatureStore/g
s/import org\.geotools\.data\.simple\.\{SimpleFeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureStore\}/import org.geotools.api.data.SimpleFeatureStore\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureStore, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureStore\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureStore\}/import org.geotools.api.data.SimpleFeatureStore/

s/org.geotools.data.simple.SimpleFeatureWriter/org.geotools.api.data.SimpleFeatureWriter/g
s/import org\.geotools\.data\.simple\.\{SimpleFeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureWriter\}/import org.geotools.api.data.SimpleFeatureWriter\nimport org.geotools.data.{\1}/
s/import org\.geotools\.data\.simple\.\{([a-zA-Z0-9, ]*), SimpleFeatureWriter, ([a-zA-Z0-9, ]*)\}/import org.geotools.api.data.SimpleFeatureWriter\nimport org.geotools.data.{\1, \2}/
s/import org\.geotools\.data\.simple\.\{SimpleFeatureWriter\}/import org.geotools.api.data.SimpleFeatureWriter/

s/org.opengis/org.geotools.api/g
