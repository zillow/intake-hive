import datetime
import shutil

import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from intake_hive.hive_source import HiveSource, SparkHolder


@pytest.fixture
def user_events_df():
    return pd.DataFrame(
        {
            "userid": [100, 101],
            "home_id": [3, 4],
            "action": ["click", "click"],
            "duration": [1.0, 0.5],
            "timestamp": [datetime.datetime(2012, 5, 1, 0, 0), datetime.datetime(2012, 5, 2, 0, 0)],
        }
    )


@pytest.fixture
def session() -> SparkSession:
    holder = SparkHolder()
    return holder.setup()


def test_user_events_hive(user_events_df: pd.DataFrame, session: SparkSession):
    spark_df = session.createDataFrame(user_events_df)

    spark_df.registerTempTable("user_events_temp")

    ds = HiveSource("user_events_temp")
    assert_frame_equal(user_events_df, ds.read())


def test_not_exist(user_events_df: pd.DataFrame, session: SparkSession):
    ds = HiveSource("not_exist")
    with pytest.raises(AnalysisException) as e:
        ds.read()

    assert "Table or view not found: not_exist" in str(e.value)


def test_catalog(user_events_df: pd.DataFrame, session: SparkSession):
    # Catalog()
    pass


def test_user_events_hive_partitioned(session: SparkSession):
    try:
        session = SparkHolder().setup()

        session.sql(
            f"""
            CREATE TABLE IF NOT EXISTS hive_df (col1 INT, col2 STRING, partition_bin INT)
            USING HIVE OPTIONS(fileFormat 'TextFile')
            PARTITIONED BY (partition_bin)
            LOCATION 'hive_df'
        """
        )

        session.sql(
            """
            INSERT INTO hive_df PARTITION (partition_bin = 0)
            VALUES (0, 'init_record')
            WHeRE
        """
        )
        session.sql(
            """
            INSERT INTO hive_df PARTITION (partition_bin = 1)
            VALUES (1, 'new_record')
        """
        )

        import numpy as np

        dtypes = {"col1": np.int32, "col2": "object", "partition_bin": np.int32}

        # test all
        assert_frame_equal(
            pd.DataFrame(
                [
                    {"col1": 0, "col2": "init_record", "partition_bin": 0},
                    {"col1": 1, "col2": "new_record", "partition_bin": 1},
                ]
            ).astype(dtypes),
            HiveSource("hive_df").read(),
        )

        # test partition_bin=0
        assert_frame_equal(
            pd.DataFrame([{"col1": 0, "col2": "init_record", "partition_bin": 0}]).astype(dtypes),
            HiveSource("hive_df?partition_bin=0").read_partition(0),
        )

        # test partition_bin=1
        assert_frame_equal(
            pd.DataFrame([{"col1": 1, "col2": "new_record", "partition_bin": 1}]).astype(dtypes),
            HiveSource("hive_df?partition_bin=1").read_partition(0),
        )
    finally:
        shutil.rmtree("metastore_db")
        shutil.rmtree("hive_df")
