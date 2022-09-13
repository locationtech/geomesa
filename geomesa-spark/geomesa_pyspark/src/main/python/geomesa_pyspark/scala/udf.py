"""
Utility functions for creating Pyspark UDFs (from Scala UDFs) that run on the JVM.
"""
from functools import partial
from pyspark.context import SparkContext
from pyspark.sql.column import Column, _to_java_column, _to_seq
from py4j.java_gateway import JavaMember, JavaObject
from typing import Union

ColumnOrName = Union[Column, str]

def scala_udf(
        sc: SparkContext,
        udf: JavaObject,
        *cols: ColumnOrName) -> Column:
    """
    Create Column for applying the Scala UDF.
    Parameters
    ----------
    sc : SparkContext
        The Spark Context object.
    udf : JavaObject
        The UDF as a Java Object.
    *cols : ColumnOrName
        The Columns for the UDF to operate on.
    Returns
    -------
    Column
        A new Column with the UDF applied to the supplied Columns.
    """
    return Column(udf.apply(_to_seq(sc, cols, _to_java_column)))

def build_scala_udf(
        sc: SparkContext,
        udf: JavaMember) -> partial:
    """
    Build a Scala UDF for PySpark by partially applying the scala_udf function.
    Parameters
    ----------
    sc : SparkContext
        The Spark Context object.
    udf : JavaMember
        The UDF as a Java Member.
    Returns
    -------
    partial
        A partially applied Scala UDF that accepts ColumnOrNames.
    """
    return partial(scala_udf, sc, udf())
