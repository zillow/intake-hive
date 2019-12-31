.. image:: https://travis-ci.org/zillow/intake-hive.svg?branch=master
    :target: https://travis-ci.org/zillow/intake-hive

.. image:: https://coveralls.io/repos/github/zillow/intake-hive/badge.svg?branch=master
    :target: https://coveralls.io/github/zillow/intake-hive?branch=master


Welcome to the Intake Hive plugin
==================================================
This `Intake <https://intake.readthedocs.io/en/latest/quickstart.html>`_ plugin 
:

Example where the Hive table is user_events_hive partitioned by userid:

.. code-block:: yaml

    sources:
      user_events_hive:
        driver: hive
        args:
          urlpath: 'user_events_yaml_catalog?userid={{userid}}'



.. code-block:: python

  import pandas as pd
  import intake

  catalog = intake.open_catalog(catalog_path)

  # Reads partition userid=42
  pandas_df: pd.DataFrame = catalog.entity.user.user_events_partitioned(userid="42").read()
