# run with: find . -name "*.scala" -not -exec grep -q "import org.geotools.api.data._" {} \; -exec sed -E -i -f gt-30-api-changes.sed {} \;

s/org\.geotools\.data\.DataStore/org.geotools.api.data.DataStore/g
s/(import org\.geotools\.data\.)\{DataStore, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.DataStore\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), DataStore\}/import org.geotools.api.data.DataStore\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), DataStore, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.DataStore\n\1{\2, \3}/

s/org\.geotools\.data\.Parameter/org.geotools.api.data.Parameter/g
s/(import org\.geotools\.data\.)\{Parameter, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Parameter\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Parameter\}/import org.geotools.api.data.Parameter\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Parameter, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Parameter\n\1{\2, \3}/

s/org.geotools.data.DataStoreFactorySpi/org.geotools.api.data.DataStoreFactorySpi/g
s/(import org\.geotools\.data\.)\{DataStoreFactorySpi, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.DataStoreFactorySpi\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), DataStoreFactorySpi\}/import org.geotools.api.data.DataStoreFactorySpi\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), DataStoreFactorySpi, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.DataStoreFactorySpi\n\1{\2, \3}/

s/org.geotools.data.DataStoreFinder/org.geotools.api.data.DataStoreFinder/g
s/(import org\.geotools\.data\.)\{DataStoreFinder, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.DataStoreFinder\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), DataStoreFinder\}/import org.geotools.api.data.DataStoreFinder\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), DataStoreFinder, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.DataStoreFinder\n\1{\2, \3}/

s/org.geotools.data.DelegatingFeatureReader/org.geotools.api.data.DelegatingFeatureReader/g
s/(import org\.geotools\.data\.)\{DelegatingFeatureReader, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.DelegatingFeatureReader\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), DelegatingFeatureReader\}/import org.geotools.api.data.DelegatingFeatureReader\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), DelegatingFeatureReader, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.DelegatingFeatureReader\n\1{\2, \3}/

s/org.geotools.data.FeatureEvent/org.geotools.api.data.FeatureEvent/g
s/(import org\.geotools\.data\.)\{FeatureEvent, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureEvent\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureEvent\}/import org.geotools.api.data.FeatureEvent\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureEvent, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureEvent\n\1{\2, \3}/

s/org.geotools.data.FeatureListener/org.geotools.api.data.FeatureListener/g
s/(import org\.geotools\.data\.)\{FeatureListener, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureListener\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureListener\}/import org.geotools.api.data.FeatureListener\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureListener, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureListener\n\1{\2, \3}/

s/org.geotools.data.FeatureLock/org.geotools.api.data.FeatureLock/g
s/(import org\.geotools\.data\.)\{FeatureLock, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureLock\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureLock\}/import org.geotools.api.data.FeatureLock\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureLock, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureLock\n\1{\2, \3}/

s/org.geotools.data.FeatureLocking/org.geotools.api.data.FeatureLocking/g
s/(import org\.geotools\.data\.)\{FeatureLocking, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureLocking\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureLocking\}/import org.geotools.api.data.FeatureLocking\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureLocking, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureLocking\n\1{\2, \3}/

s/org.geotools.data.FeatureReader/org.geotools.api.data.FeatureReader/g
s/(import org\.geotools\.data\.)\{FeatureReader, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureReader\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureReader\}/import org.geotools.api.data.FeatureReader\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureReader, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureReader\n\1{\2, \3}/

s/org.geotools.data.FeatureSource/org.geotools.api.data.FeatureSource/g
s/(import org\.geotools\.data\.)\{FeatureSource, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureSource\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureSource\}/import org.geotools.api.data.FeatureSource\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureSource, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureSource\n\1{\2, \3}/

s/org.geotools.data.FeatureStore/org.geotools.api.data.FeatureStore/g
s/(import org\.geotools\.data\.)\{FeatureStore, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureStore\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureStore\}/import org.geotools.api.data.FeatureStore\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureStore, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureStore\n\1{\2, \3}/

s/org.geotools.data.FeatureWriter/org.geotools.api.data.FeatureWriter/g
s/(import org\.geotools\.data\.)\{FeatureWriter, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureWriter\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureWriter\}/import org.geotools.api.data.FeatureWriter\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FeatureWriter, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FeatureWriter\n\1{\2, \3}/

s/org.geotools.data.FileDataStore/org.geotools.api.data.FileDataStore/g
s/(import org\.geotools\.data\.)\{FileDataStore, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FileDataStore\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FileDataStore\}/import org.geotools.api.data.FileDataStore\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FileDataStore, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FileDataStore\n\1{\2, \3}/

s/org.geotools.data.FileDataStoreFactorySpi/org.geotools.api.data.FileDataStoreFactorySpi/g
s/(import org\.geotools\.data\.)\{FileDataStoreFactorySpi, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FileDataStoreFactorySpi\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FileDataStoreFactorySpi\}/import org.geotools.api.data.FileDataStoreFactorySpi\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FileDataStoreFactorySpi, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FileDataStoreFactorySpi\n\1{\2, \3}/

s/org.geotools.data.FileStoreFactory/org.geotools.api.data.FileStoreFactory/g
s/(import org\.geotools\.data\.)\{FileStoreFactory, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FileStoreFactory\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FileStoreFactory\}/import org.geotools.api.data.FileStoreFactory\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), FileStoreFactory, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.FileStoreFactory\n\1{\2, \3}/

s/org.geotools.data.Join/org.geotools.api.data.Join/g
s/(import org\.geotools\.data\.)\{Join, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Join\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Join\}/import org.geotools.api.data.Join\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Join, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Join\n\1{\2, \3}/

s/org.geotools.data.LockingManager/org.geotools.api.data.LockingManager/g
s/(import org\.geotools\.data\.)\{LockingManager, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.LockingManager\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), LockingManager\}/import org.geotools.api.data.LockingManager\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), LockingManager, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.LockingManager\n\1{\2, \3}/

s/org.geotools.data.Parameter/org.geotools.api.data.Parameter/g
s/(import org\.geotools\.data\.)\{Parameter, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Parameter\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Parameter\}/import org.geotools.api.data.Parameter\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Parameter, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Parameter\n\1{\2, \3}/

s/org.geotools.data.Query/org.geotools.api.data.Query/g
s/(import org\.geotools\.data\.)\{Query, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Query\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Query\}/import org.geotools.api.data.Query\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Query, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Query\n\1{\2, \3}/

s/org.geotools.data.QueryCapabilities/org.geotools.api.data.QueryCapabilities/g
s/(import org\.geotools\.data\.)\{QueryCapabilities, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.QueryCapabilities\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), QueryCapabilities\}/import org.geotools.api.data.QueryCapabilities\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), QueryCapabilities, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.QueryCapabilities\n\1{\2, \3}/

s/org.geotools.data.Repository/org.geotools.api.data.Repository/g
s/(import org\.geotools\.data\.)\{Repository, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Repository\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Repository\}/import org.geotools.api.data.Repository\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Repository, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Repository\n\1{\2, \3}/

s/org.geotools.data.ResourceInfo/org.geotools.api.data.ResourceInfo/g
s/(import org\.geotools\.data\.)\{ResourceInfo, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.ResourceInfo\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), ResourceInfo\}/import org.geotools.api.data.ResourceInfo\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), ResourceInfo, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.ResourceInfo\n\1{\2, \3}/

s/org.geotools.data.ServiceInfo/org.geotools.api.data.ServiceInfo/g
s/(import org\.geotools\.data\.)\{ServiceInfo, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.ServiceInfo\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), ServiceInfo\}/import org.geotools.api.data.ServiceInfo\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), ServiceInfo, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.ServiceInfo\n\1{\2, \3}/

s/org.geotools.data.Transaction/org.geotools.api.data.Transaction/g
s/(import org\.geotools\.data\.)\{Transaction, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Transaction\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Transaction\}/import org.geotools.api.data.Transaction\n\1{\2}/
s/(import org\.geotools\.data\.)\{([a-zA-Z0-9, _]*), Transaction, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.Transaction\n\1{\2, \3}/

s/org.geotools.data.simple.SimpleFeatureReader/org.geotools.api.data.SimpleFeatureReader/g
s/(import org\.geotools\.data\.simple\.)\{SimpleFeatureReader, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.SimpleFeatureReader\n\1{\2}/
s/(import org\.geotools\.data\.simple\.)\{([a-zA-Z0-9, _]*), SimpleFeatureReader\}/import org.geotools.api.data.SimpleFeatureReader\n\1{\2}/
s/(import org\.geotools\.data\.simple\.)\{([a-zA-Z0-9, _]*), SimpleFeatureReader, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.SimpleFeatureReader\n\1{\2, \3}/

s/org.geotools.data.simple.SimpleFeatureSource/org.geotools.api.data.SimpleFeatureSource/g
s/(import org\.geotools\.data\.simple\.)\{SimpleFeatureSource, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.SimpleFeatureSource\n\1{\2}/
s/(import org\.geotools\.data\.simple\.)\{([a-zA-Z0-9, _]*), SimpleFeatureSource\}/import org.geotools.api.data.SimpleFeatureSource\n\1{\2}/
s/(import org\.geotools\.data\.simple\.)\{([a-zA-Z0-9, _]*), SimpleFeatureSource, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.SimpleFeatureSource\n\1{\2, \3}/

s/org.geotools.data.simple.SimpleFeatureStore/org.geotools.api.data.SimpleFeatureStore/g
s/(import org\.geotools\.data\.simple\.)\{SimpleFeatureStore, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.SimpleFeatureStore\n\1{\2}/
s/(import org\.geotools\.data\.simple\.)\{([a-zA-Z0-9, _]*), SimpleFeatureStore\}/import org.geotools.api.data.SimpleFeatureStore\n\1{\2}/
s/(import org\.geotools\.data\.simple\.)\{([a-zA-Z0-9, _]*), SimpleFeatureStore, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.SimpleFeatureStore\n\1{\2, \3}/

s/org.geotools.data.simple.SimpleFeatureWriter/org.geotools.api.data.SimpleFeatureWriter/g
s/(import org\.geotools\.data\.simple\.)\{SimpleFeatureWriter, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.SimpleFeatureWriter\n\1{\2}/
s/(import org\.geotools\.data\.simple\.)\{([a-zA-Z0-9, _]*), SimpleFeatureWriter\}/import org.geotools.api.data.SimpleFeatureWriter\n\1{\2}/
s/(import org\.geotools\.data\.simple\.)\{([a-zA-Z0-9, _]*), SimpleFeatureWriter, ([a-zA-Z0-9, _]*)\}/import org.geotools.api.data.SimpleFeatureWriter\n\1{\2, \3}/

s/(import org\.geotools\.data\.)\{DataStore\}/import org.geotools.api.data.DataStore/
s/(import org\.geotools\.data\.)\{FeatureLock\}/import org.geotools.api.data.FeatureLock/
s/(import org\.geotools\.data\.)\{FeatureLocking\}/import org.geotools.api.data.FeatureLocking/
s/(import org\.geotools\.data\.)\{FeatureReader\}/import org.geotools.api.data.FeatureReader/
s/(import org\.geotools\.data\.)\{FeatureSource\}/import org.geotools.api.data.FeatureSource/
s/(import org\.geotools\.data\.)\{FeatureStore\}/import org.geotools.api.data.FeatureStore/
s/(import org\.geotools\.data\.)\{FeatureWriter\}/import org.geotools.api.data.FeatureWriter/
s/(import org\.geotools\.data\.)\{FileDataStore\}/import org.geotools.api.data.FileDataStore/
s/(import org\.geotools\.data\.)\{FileDataStoreFactorySpi\}/import org.geotools.api.data.FileDataStoreFactorySpi/
s/(import org\.geotools\.data\.)\{FileStoreFactory\}/import org.geotools.api.data.FileStoreFactory/
s/(import org\.geotools\.data\.)\{Parameter\}/import org.geotools.api.data.Parameter/
s/(import org\.geotools\.data\.)\{DataStoreFactorySpi\}/import org.geotools.api.data.DataStoreFactorySpi/
s/(import org\.geotools\.data\.)\{DataStoreFinder\}/import org.geotools.api.data.DataStoreFinder/
s/(import org\.geotools\.data\.)\{DelegatingFeatureReader\}/import org.geotools.api.data.DelegatingFeatureReader/
s/(import org\.geotools\.data\.)\{FeatureEvent\}/import org.geotools.api.data.FeatureEvent/
s/(import org\.geotools\.data\.)\{FeatureListener\}/import org.geotools.api.data.FeatureListener/
s/(import org\.geotools\.data\.)\{Join\}/import org.geotools.api.data.Join/
s/(import org\.geotools\.data\.)\{LockingManager\}/import org.geotools.api.data.LockingManager/
s/(import org\.geotools\.data\.)\{Parameter\}/import org.geotools.api.data.Parameter/
s/(import org\.geotools\.data\.)\{Query\}/import org.geotools.api.data.Query/
s/(import org\.geotools\.data\.)\{QueryCapabilities\}/import org.geotools.api.data.QueryCapabilities/
s/(import org\.geotools\.data\.)\{Repository\}/import org.geotools.api.data.Repository/
s/(import org\.geotools\.data\.)\{ResourceInfo\}/import org.geotools.api.data.ResourceInfo/
s/(import org\.geotools\.data\.)\{ServiceInfo\}/import org.geotools.api.data.ServiceInfo/
s/(import org\.geotools\.data\.)\{Transaction\}/import org.geotools.api.data.Transaction/
s/(import org\.geotools\.data\.simple\.)\{SimpleFeatureReader\}/import org.geotools.api.data.SimpleFeatureReader/
s/(import org\.geotools\.data\.simple\.)\{SimpleFeatureSource\}/import org.geotools.api.data.SimpleFeatureSource/
s/(import org\.geotools\.data\.simple\.)\{SimpleFeatureStore\}/import org.geotools.api.data.SimpleFeatureStore/
s/(import org\.geotools\.data\.simple\.)\{SimpleFeatureWriter\}/import org.geotools.api.data.SimpleFeatureWriter/

s/import org\.geotools\.data\.\{_\}/import org.geotools.data._/
s/import org\.geotools\.data\._/\0\nimport org.geotools.api.data._/

s/org\.opengis\.geometry\.BoundingBox/org.geotools.api.geometry.BoundingBox/g
s/org\.opengis\.geometry\.Geometry/org.locationtech.jts.geom.Geometry/g
s/FilterFactory2/FilterFactory/g
s/ReferencedEnvelope\.create/ReferencedEnvelope.envelope/g
s/org\.geotools\.data\.DataAccessFactory\.Param/org.geotools.api.data.DataAccessFactory.Param/g
s/DirectPosition/Position/g

s/org.opengis/org.geotools.api/g
