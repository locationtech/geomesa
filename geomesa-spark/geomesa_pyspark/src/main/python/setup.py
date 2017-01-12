from setuptools import setup, find_packages

setup(
    name='geomesa_pyspark',
    author='Tom Kunicki, Gerard C. Briones',
    version='${python.version}',
    author_email='tkunicki@ccri.com, gbriones@ccri.com',
    packages=find_packages(),
    install_requires=['pytz']
)
