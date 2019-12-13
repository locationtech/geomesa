import glob
import os.path
import pkgutil
import re
import sys
import tempfile
import zipfile


__version__ = '${python.version}'

PACKAGE_EXTENSIONS = {'.zip', '.egg', '.jar'}
PACKAGE_DEV = re.compile("[.]dev[0-9]*$")


def configure(jars=[], packages=[], files=[], spark_home=None, spark_master='yarn', tmp_path=None):

    os.environ['PYSPARK_PYTHON'] = sys.executable

    spark_home = process_spark_home(spark_home)

    pyspark_dir = os.path.join(spark_home, 'python')
    pyspark_lib_dir = os.path.join(pyspark_dir, 'lib')
    pyspark_lib_zips = glob.glob(os.path.join(pyspark_lib_dir, '*.zip'))
    sys_path_set = {path for path in sys.path}
    for pyspark_lib_zip in pyspark_lib_zips:
        if pyspark_lib_zip not in sys_path_set and os.path.basename(pyspark_lib_zip) != 'pyspark.zip':
            sys.path.insert(1, pyspark_lib_zip)
    if pyspark_dir not in sys_path_set:
        sys.path.insert(1, pyspark_dir)
    
    py_files = pyspark_lib_zips + process_executor_packages(packages, tmp_path)

    assert spark_master is 'yarn', 'only yarn master is supported with this release'

    import pyspark
    import geomesa_pyspark.types

    # Need differential behavior based for <= Spark 2.0.x, Spark 2.1.0
    #  is the fist release to provide the module __version__ attribute
    pyspark_pre21 = getattr(pyspark, '__version__', None) is None

    if pyspark_pre21 and len(jars) > 0:
        os.environ['PYSPARK_SUBMIT_ARGS'] = ' '.join(['--driver-class-path', ','.join(jars), 'pyspark-shell'])

    conf = (
        pyspark.SparkConf()
        .setMaster(spark_master)
        .set('spark.yarn.dist.jars', ','.join(jars))
        .set('spark.yarn.dist.files', ','.join(py_files + files))
        .setExecutorEnv('PYTHONPATH', ":".join(map(os.path.basename, py_files)))
        .setExecutorEnv('PYSPARK_PYTHON', sys.executable)
    )

    if not pyspark_pre21 and len(jars):
        conf.set('spark.driver.extraClassPath', ','.join(jars))

    return conf

        
def process_spark_home(spark_home):
    if spark_home is None:
        spark_home = os.environ.get('SPARK_HOME', None)
    assert spark_home is not None, 'unable to resolve SPARK_HOME'
    assert os.path.isdir(spark_home), '%s is not a directory' % spark_home
    os.environ['SPARK_HOME'] = spark_home
    return spark_home


def process_executor_packages(executor_packages, tmp_path=None):
    if tmp_path is None:
        version_info = sys.version_info
        tmp_path = os.path.join(tempfile.gettempdir(), 'spark-python-%s.%s' % (version_info.major, version_info.minor))
    if not os.path.isdir(tmp_path):
        os.makedirs(tmp_path)
    driver_packages = {module for _, module, package in pkgutil.iter_modules() if package is True}
    executor_files = []
    for executor_package in executor_packages:
        
        if executor_package not in driver_packages:
            raise ImportError('unable to locate ' + executor_package + ' installed in driver')

        package = sys.modules.get(executor_package, None)
        if package is None:
            package = pkgutil.get_loader(executor_package).load_module(executor_package)

        package_path = os.path.dirname(package.__file__)
        package_root = os.path.dirname(package_path)

        if package_root[-4:].lower() in PACKAGE_EXTENSIONS:
            executor_files.append(package_root)
        elif os.path.isdir(package_root):
            package_version = getattr(package, '__version__', getattr(package, 'VERSION', None))
            zip_name = "%s.zip" % executor_package if package_version is None\
                else "%s-%s.zip" % (executor_package, package_version) 
            zip_path = os.path.join(tmp_path, zip_name)
            if (not os.path.isfile(zip_path)) or ((package_version and PACKAGE_DEV.search(package_version)) is not None):
                zip_package(package_path, zip_path)
            executor_files.append(zip_path)
                
    return executor_files

    
def zip_package(package_path, zip_path):
    path_offset = len(os.path.dirname(package_path)) + 1
    with zipfile.PyZipFile(zip_path, 'w') as writer:
        for root, _, files in os.walk(package_path):
            for file in files:
                full_path = os.path.join(root, file)
                archive_path = full_path[path_offset:]
                writer.write(full_path, archive_path)


def init_sql(spark):
    spark._jvm.org.apache.spark.sql.SQLTypes.init(spark._jwrapped)
