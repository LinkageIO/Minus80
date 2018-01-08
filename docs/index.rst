.. Minus80 documentation master file, created by
   sphinx-quickstart on Thu Jan  4 12:38:30 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _overview:
.. currentmodule:: minus80 

###############################################################
minus80: A library for freezing and integrating biological data
###############################################################

**minus80** is a `Python <http://www.python.org>`__ package for storing and
analyzing biological datasets. Minus80 has a motto of *build once, import many
times*.  For example, a dataset can be imported from either a raw dataset or
from another file format, then be queried or accessed almost instantly using
the python API. Much like a real -80C Freezer, samples (data) are prepped and
then stored long term for easy access.

.. ipython:: python

    import minus80 as m80

Minus80 is two main components: a front end and a back end. The front end are
two python objects (Accession and Cohort) that represent datasets that are
stored by minus80. The back end is an abstract base class that can be extended
to make other datasets :ref:`freezable`. It is easier to explain what minus80
does with a use case. After mastering how Accession and Cohort behave, see the
Freezable docs to learn how to extend the storage functionality of minus80 to
other datasets and how to access the databases powering minus80 directly.


Table Of Contents
=================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   Overview <index>
   Accessions and Cohorts <accessions_and_cohorts>
   Freezing <freezable>
   config
   Cloud <cloud>
   api


Indices and tables
==================

* :ref:`genindex`
* :ref:`search`

