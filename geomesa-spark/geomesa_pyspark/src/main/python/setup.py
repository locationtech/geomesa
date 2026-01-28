from setuptools import setup, find_packages

setup(
    name='geomesa_pyspark',
    version='${python.version}',
    url='https://www.geomesa.org/',
    packages=find_packages(),
    install_requires=['pytz', 'shapely']
)
