.. intake-hive documentation master file
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

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



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
