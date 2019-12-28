.. image:: https://travis-ci.org/zillow/intake-hive.svg?branch=master
    :target: https://travis-ci.org/zillow/intake-hive

.. image:: https://coveralls.io/repos/github/zillow/intake-hive/badge.svg?branch=master
    :target: https://coveralls.io/github/zillow/intake-hive?branch=master

.. image:: https://readthedocs.org/projects/intake-hive/badge/?version=latest
    :target: https://intake-hive.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status


Welcome to Intake Hive  plugin
==================================================
This `Intake <https://intake.readthedocs.io/en/latest/quickstart.html>`_ plugin 
:

Sample Catalog source entry:

.. code-block:: yaml

    sources:
      user_events_hive:
        driver: hive
        args:
          # Assumes the table already exists in Hive
          urlpath: 'user_events'


Example code using sample catalog:

.. code-block:: python

  # TODO
