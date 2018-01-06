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
two python objects that represent datasets that are stored by minus80. The back
end is an abstract base class that can be extended to make datasets
*freezable*. It is easier to explain what minus80 does with a use case.


Accessions
----------
Consider an instance where you want to characterize data generated from an
experiment. This can be accomplished by using an :class:`Accession` object.
An accession is a simple structure that houses an identifier along with 
associated data files and accomponying metadata.

.. autoclass:: Accession
   :noindex:

The minimal amount of information needed is an identifier:

.. ipython:: python

   x = m80.Accession('Sample1')
   x

Files and metadata can be associated with the accession upon initialization
using the `files` kwarg and any number of additional kwargs for metadata:

.. ipython:: python
   
    x = m80.Accession('Sample2',
        files=['reads1.fastq','reads2.fastq'],
        gender='male',
        age=45
    )
    x

Once created, data can be accessed from the object in the usual, pythonic way:

.. ipython:: python

   'reads1.fastq' in x.files
   x['gender']
   x['age']


However usefuls, most experiments do not consist of a single accession. Accessions
become powerful when they are analyzed together in some sort of experimental context.
A group of accessions is called a :class:`Cohort`.

Cohorts
-------
A Cohort represent a **named** set of accessions. A Cohort is a *build once, utilize many*
data structure and is backed by a database on disk. Revisiting the -80C Freezer analogy, 
Accessions are the units which are *frozen* and *unfrozen* from the database but only when
they are added to a Cohort. Think of Accessions that are added to Cohorts as from a 
**master mix**, they can be *unfrozen* and used multiple times through multiple analyses and
are only changed when they are updated in the Cohort. This is an **important** concent as 
multiple instances of the same accession can be generated from the same Cohort (see example 
below). 

You can interact with Accessions within
a cohort in a variety of ways.

.. autoclass:: Cohort
   :noindex:

A Cohort is initialized by name:

.. ipython:: python

    c = m80.Cohort('experiment1')
    a1 = m80.Accession('acc1',pval=0.05)  
    a2 = m80.Accession('acc2')  
    a3 = m80.Accession('acc3')  
    c.add_accessions([a1,a2,a3])

.. note::

    When an Accession is added to a Cohort, it is assigned an internal integer
    ID (called AID). These can be useful when short identifiers are needed but
    are not guaranteed to be the same across different cohorts. E.g. `a1` might
    have an AID of 1 in `c1` but an AID of 12 in `c2`. The AID is assigned based
    on the order it was added to the Cohort. 

Interacting with Cohorts is pythonic:

.. ipython:: python

    len(c)
    a1 in c
    for x in c:
        print(x.name)

Accessions are accessed from the Cohort by identifier:

.. ipython:: python

    a1dup = c['acc1']
    a1dup 

Changing the values of an accession do not influence the *frozen* version
from the Cohort. Another instance of the same Accessions will have the 
orgininal value.

.. ipython:: python

    instance1 = c['acc1']
    instance1['pval'] = 0.04
    instance2 = c['acc1']
    instance1['pval']
    instance2['pval']


Some other convenience functions are useful for randomized analyses:

.. ipython:: python

   random_accession = c.random_accession()
   random_accession
   random_accessions = list(c.random_accessions(n=2))
   random_accessions


.. autoclass:: Freezable


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   Overview <index>
   config
   Cloud <cloud>
   api


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

