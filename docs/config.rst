
.. _config:
.. currentmodule:: minus80


###################
Configuration Files
###################


**minus80** uses a configuration file that is generated and placed in
your home directory: ``~/.minus80.conf``. If a config file does not exist,
it will be created the first time you import minus80.


A default config file is a YAML file and looks like:

.. ipython:: python
    
    from minus80.Config import default_config
    print(default_config)


Basic Options
-------------
The basic minus80 options are specified in the ``options`` section. The ``basedir`` is 
directory that contains all the internal databases and minus80 data.


Cloud Options
-------------
Minus80 comes with the ability to store and retrieve data from the :ref:`cloud <cloud>`. To use this 
feature, add in `S3 <https://docs.aws.amazon.com/AmazonS3/latest/dev/Welcome.html>`__ credentials
and refer to the minus80 :ref:`cloud <cloud>` documentation.

