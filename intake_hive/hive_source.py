import threading
from typing import Dict, List
from urllib.parse import ParseResult, parse_qs, urlparse  # noqa: F401

import numpy as np
import pandas as pd
import pkg_resources
from intake import DataSource, Schema
from pyspark import SparkContext, sql
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    TimestampType,
)


class SparkHolder(object):
    session: SparkSession = None
    lock = threading.Lock()

    def set_session(self, session=None) -> SparkSession:
        """Set global SQL SparkSession
        Args:
            session: Explicitly provide a session object, if you have one
        """
        if self.session is not None:
            return self.session

        with self.lock:
            if session is None:
                self.session = sql.SparkSession.builder.enableHiveSupport().getOrCreate()
            else:
                self.session = session

        return self.session

    def setup(self) -> SparkSession:
        return self.set_session()

    def __enter__(self):
        return self.setup()

    def __exit__(self, *exc):
        sc: SparkContext = self.session.sparkContext
        sc.stop()
        self.session.stop()


class HiveSource(DataSource):
    """
    Interfaces with Online FS
    https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
    """

    container = "dataframe"
    partition_access = True
    name = "hive"
    version = pkg_resources.get_distribution("intake-hive").version

    def __init__(self, urlpath: str, metadata=None):
        """
            Example where the Hive table is user_events_hive without partitions:
                user_events_hive:
                  driver: hive
                  args:
                    urlpath: 'user_events_yaml_catalog'

            >>> spark_df = catalog.entity.user.user_events_partitioned().to_spark()

            Example where the Hive table is user_events_hive partitioned by userid:
                user_events_hive:
                  driver: hive
                  args:
                    urlpath: 'user_events_yaml_catalog?userid={{userid}}'

            >>> # Reads partition userid=42
            >>> pandas_df: pd.DataFrame = catalog.entity.user.user_events_partitioned(userid="42").read()
        Args:
            urlpath: the Hive table name, and partition keys and values
            metadata: Used by Intake
        """
        self._urlpath: str = urlpath
        parse_result: ParseResult = urlparse(urlpath)
        self._table_name: str = parse_result.netloc if parse_result.scheme != "" else parse_result.path
        self._partitions: Dict[str, str] = parse_qs(parse_result.query)
        self._holder = SparkHolder()
        self._session = None

        super().__init__(metadata=metadata)

    def write(self, df: pd.DataFrame):
        raise NotImplementedError("Not yet implemented")

    def _get_schema(self) -> Schema:
        extra_metadata = None
        if "canonical_name" in self.metadata:
            self._canonical_name = self.metadata["canonical_name"]
            self._avro_schema = self.metadata["avro_schema"] if "avro_schema" in self.metadata else None
            self.dtype = self.metadata["dtypes"] if "dtypes" in self.metadata else None
            extra_metadata = {"canonical_name": self._canonical_name, "avro_schema": self._avro_schema}

        if self._session is None:
            self._holder.setup()
            self._session = self._holder.session
            self.spark_df = self._setup_spark_df()
            self.npartitions = self.spark_df.rdd.getNumPartitions()

        if self.dtype is None:
            # this means that the _avro_schema is also none

            rows = self.spark_df.take(10)
            self.dtype = pandas_dtypes(self.spark_df.schema, rows)
            self.shape = (None, len(self.dtype))

        return Schema(
            datashape=None,
            dtype=self.dtype,
            shape=(None, len(self.dtype)),
            npartitions=1,  # This data is not partitioned, so there is only one partition
            extra_metadata=extra_metadata,
        )

    def _create_hive_query(self) -> str:
        if len(self._partitions) > 0:
            conditions = " AND ".join([f"{k}='{v[0]}'" for k, v in self._partitions.items()])
            return f"SELECT * FROM {self._table_name} WHERE {conditions}"
        else:
            return f"SELECT * FROM {self._table_name}"

    def _setup_spark_df(self) -> sql.DataFrame:
        query = self._create_hive_query()
        return self._session.sql(query)

    def read_partition(self, i):
        """Returns one partition of the data as a pandas data-frame"""
        import pandas as pd

        self._get_schema()
        # TODO(talebz): checkout arrow
        out = self._session.sparkContext.runJob(self.spark_df.rdd, lambda x: x, partitions=[i])
        df = pd.DataFrame.from_records(out, columns=list(self.dtype))
        df = df.astype(self.dtype)
        return df

    def to_spark(self):
        """Return the Spark object for this data, a DataFrame"""
        self._get_schema()
        return self.spark_df

    def read(self):
        """Read all of the data into an in-memory Pandas data-frame"""
        self._get_schema()
        return self.spark_df.toPandas()

    def _close(self):
        self.spark_df = None


def pandas_dtypes(schema: sql.types.StructType, rows: List[sql.types.Row]) -> Dict[str, str]:
    """
    copied from intake-spark
    Rough dtype for the given pyspark schema
    """
    from pyspark.sql.types import IntegralType, BooleanType

    # copied from toPandas() method
    pdf = pd.DataFrame.from_records(rows)
    pdf.columns = [s.name for s in schema]
    for field in schema:
        pandas_type = _to_corrected_pandas_type(field.dataType)
        # if pandas_type is not None and not(
        #         isinstance(field.dataType, IntegralType) and field.nullable):
        #     df[field.name] = df[field.name].astype(pandas_type)
        # SPARK-21766: if an integer field is nullable and has null values, it can be
        # inferred by pandas as float column. Once we convert the column with NaN back
        # to integer type e.g., np.int16, we will hit exception. So we use the inferred
        # float type, not the corrected type from the schema in this case.
        if pandas_type is not None and not (
            isinstance(field.dataType, IntegralType) and field.nullable and pdf[field.name].isnull().any()
        ):
            pdf[field.name] = pdf[field.name].astype(pandas_type)
        # Ensure we fall back to nullable numpy types, even when whole column is null:
        if isinstance(field.dataType, IntegralType) and pdf[field.name].isnull().any():
            pdf[field.name] = pdf[field.name].astype(np.float64)
        if isinstance(field.dataType, BooleanType) and pdf[field.name].isnull().any():
            pdf[field.name] = pdf[field.name].astype(np.object)
    return {k: str(v) for k, v in pdf.dtypes.to_dict().items()}


def _to_corrected_pandas_type(dt: sql.types.DataType):  # noqa: C901  too complex
    """
    copied from 3.0
    When converting Spark SQL records to Pandas :class:`DataFrame`, the inferred data type may be
    wrong. This method gets the corrected data type for Pandas if that type may be inferred
    uncorrectly.
    """
    if type(dt) == ByteType:
        return np.int8
    elif type(dt) == ShortType:
        return np.int16
    elif type(dt) == IntegerType:
        return np.int32
    elif type(dt) == LongType:
        return np.int64
    elif type(dt) == FloatType:
        return np.float32
    elif type(dt) == DoubleType:
        return np.float64
    elif type(dt) == BooleanType:
        return np.bool
    elif type(dt) == TimestampType:
        return np.datetime64
    else:
        return None
