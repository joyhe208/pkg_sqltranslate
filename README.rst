============
SQLTranslate
============

SQL Translate stores database info and generates complex sql queries so people
who don't know sql can access the data they need in a user friendly way.

Features
--------
- Store database info in jsons --> DatabaseInfo class will use JSON data to cache a DatabaseInfo object
example jsons:

tableInfo = 
{
    'climatic':                                       
    {                                                 
        'temporal': True,
        'dateCol': 'day',                          
        'classifier': 'staid',
        'columns': ['tmmx','tmmn','pr','pet','vpd',
        'rmax','rmin','th','vs','day','staid']
    },
    'streamflow':                                     
    {                                                 
        'temporal': True,
        'dateCol': 'date',                             
        'classifier':'staid',
        'columns':['streamflow','date','staid']
    },
    'stations':                                       
    {
        'temporal': False,
        'classifier':'staid',
        'columns':['staid','lat','lng']
    }
}

dataCategories = 
{
    'Streamflow': {
        'streamflow':['streamflow']
    },
    Temperature':{
        'climatic': ['tmmx','tmmn']
    },
    'Precipitation':{
        'climatic': ['pr']
    },
    'Humidity': {
        'climatic': ['pet', 'vpd','rmax','rmin']
    },
    'Wind':{
        'climatic': ['th','vs']
    }
}

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
