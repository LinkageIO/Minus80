
.. _cloud:
.. currentmodule:: minus80



###############################
Connecting Minus80 to the Cloud
###############################

.. Note:: This is currently an experimental feature and still under development! Please direct any questions or bugs to `our github <https://github.com/LinkageIO/Minus80/issues>`__.

**minus80** has an experimental feature where it can store and access data that
is stored in an `S3 <https://docs.aws.amazon.com/AmazonS3/latest/dev/Welcome.html>`__ 
bucket in the *cloud*.  First, the proper credentials need to be added to the
:ref:`config <config>` file which is stored in your home directory:
``~/.minus80.conf``. You will need to know your s3 ``endpoint`` which is either
an amazon server or an enterprise s3 server, your ``access_key`` and your
``secret_key``.


Once this is set up, the :class:`CloudData` object allows you to interact with minus80
datasets.

.. autoclass:: CloudData


Accessing minus80 data in the cloud
-----------------------------------

Initialization is easy as credentials are taken from ``~/.minus80.conf`` so no arguments are needed::

    >>> import minus80 as m80
    >>> x = m80.CloudData()


CloudData objects have a simple interface to interact with data stored in the cloud. List your stored
datasets::

    >>> x.list()
    Nothing here yet!

Store a dataset by its name and its dtype (e.g. Cohort)::

    >>> x.put('TestCohort', 'Cohort')
    >>> x.list()
    -----Cohort------
    TestCohort

Retrieve a dataset using the same interface::

    >>> x.get('TestCohort', 'Cohort')


Storing raw data in the cloud
-----------------------------
Sometimes you have raw data that needs to be shared or stored. The cloud is a perfect place for this.
All of the CloudData methods have a ``raw`` keyword argument that indicates raw data. If ``True``, 
the ``name`` argument changes to a filename and the ``dtype`` argument changes to a string indicating
the raw datatype that the file contains. In practice this can be anything, it just categorizes the raw
data. Often the file extension suffices (e.g. a fastq file). See the :ref:`api` for full details.


.. Warning:: These are convenience methods are are not substitutes for properly backing up your data. There are not guarantees on integrity.


