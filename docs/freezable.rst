.. _freezable:
.. currentmodule:: minus80

#########
Freezable
#########

How minus80 works behind the scenes
-----------------------------------
As detailed in the :ref:`overview <index>`, minus80 comes with two objects that make it easy to 
store and access data about experimental *samples* or more broadly, experimental *accessions*.
Accession objects can be created, but are not persistent across python sessions unless they are
stored within a Cohort. 

Also, changes to the underlying data does not happen when accessions are
changed, only when they are changed through the Cohort methods. Think of taking DNA samples out of the
freezer and using them in an assay. A small aliquot is taken from the frozen sample to perform the 
analysis on, changes to this aliquot (the non-persistent accession here) does not change the underlying
DNA stored in the freezer (the cohort).

In the same vein, duplicate Accessions can stored (with sometimes different metadata, etc) in multiple
Cohorts in minus80. It is the context of the Cohort name differentiates what data goes along with each 
accession contained within it. Think of 10 individuals. You could have a cohort called "genomicDNA" that
contains all 10 samples data. You could have the **same 10 accessions** under a different context, perhaps
"liverSamples". 

Freezable datastructures
------------------------
Cohorts persist across python sessions because the Cohort class inherits from the Freezable class. Accessions
do not inherit these properties. The freezable class is an abstract one, meaning that you would most likely not
create a *freezable* instance on its own. This is much like how lists inherit from the *iterable* abstract class,
you would never create just an *iterable* but rather create objects that *are iterable*. In the same way, you can
create objects that *are freezable*.

Here is the signature for the Freezable class:

.. autoclass:: Freezable
    :noindex:


A Freezable object needs a ``name`` attribute and a discernible ``dtype`` in order to be frozen.
For instance, since Cohorts are freezable, assume the following::
    
    >>> import minus80 as m80
    >>> x = m80.Cohort('Sample1Liver')

Here the name is ``Sample1Liver`` and the dtype is ``Cohort``. Since Cohorts are freezable, and the 
freezable ``__init__`` function is called when the object is made, the object inherits some attributes.
Part of these attributes are links to several centralized databases. These databases are stored in the
directory dictated by the ``basedir`` options in the ``~/.minus80.conf`` file. 

Three different databases are supported, each serving a slightly different purpose. You can read the full
details in the :ref:`api`, or read a summary here.

Relational Data
~~~~~~~~~~~~~~~
A link to a ``sqlite`` database is provided using the ``apsw`` package internally. When the object is 
created, a database file linked to its ``name`` and ``dtype`` is created.

Also, upon creation, an open connections to the db is made. This can be accessed as an internal 
attribute::

    >>> x = Cohort('Sample1Liver')
    # Get the internal sqlite connection
    >>> con = x._db
    # Get a cursor
    >>> cur = x._db.cursor()
    # Execute some SQL 
    >>> cur.execute('SELECT * FROM ...')


Additionally, tables from other minus80 databases can be cross accessed.

.. automethod:: Freezable._open_db
    :noindex:


Or the path to the object database can be accessed:

.. automethod:: Freezable._dbfilename
    :noindex:


Columnar Data
-------------
Columnar data structures, such as numpy arrays or pandas dataframes, are not as well suited to
be stored and accessed quickly in an SQL database. Minus80 stores columnar data internally on disk
as bcolz tables. These are *very* fast to load from disk and can even be accessed out of memory using
things like blaze.

If you want to store a pandas dataframe, use the ``_bcolz`` method::

    >>> x = Cohort('Sample2Liver')
    # Create a dataframe
    >>> data_frame = pandas.DataFrame(...)
    # Store the dataframe in the Cohort
    >>> x._bcolz('tblData',df=data_frame)
    # Retrieve the dataframe from the database
    >>> data_frame2 = x.bcolz('tblData') 
    # Note, to retrieve, provide a name but no df option

If you want to store a numpy array instead of an entire dataframe, you can do that with the similar
``_bcolz_array`` method::

    >>> import numpy as np
    >>> x = Cohort('Sample3Liver')
    # Say, you have an array
    >>> data_array = np.array([0,1,2,3,4,5])
    # Store the array as part of the Cohort object
    # Here we give it the  name 'data'
    >>> x._bcolz_array('data', array=data_array)
    # To retrieve the array from the database, use the
    # same method, but do not provide the array option
    >>> data_array2 = x._bcolz_array('data')


Key/Value Store
---------------
A simple key/value store is included with Freezable object for things like object attributes. This
is backed internally by sqlite3. This is mainly for a small number of object attributes or the like.
The internal method is smart enough to detect three different types of values: ``int``, ``float``, and
``str``. More complex values are not supported by this method.


.. Note:: This is not optimized for massive datasets and does not compete with specialized key/value stores.

The key/value store can be accessed using the internal ``_dict`` method::

    >>> x = Cohort('Sample4Liver')
    # Store a value in the dict
    >>> x._dict('foo',val='bar')
    # Retrieve the value
    >>> val = x._dict('foo')


Temporary Files
---------------
Temporary files can be accessed using the ``_tmpfile`` method. This simple method
just wraps the ``tempfile`` module and creates the tmpfile in the minus80 basedir 
as to consolidate everything.
