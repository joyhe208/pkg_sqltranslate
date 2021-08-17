SQLTranslate
============

SQL Translate stores database info and generates complex sql queries with simple input parameters to make customized data packages accessible.

Features
--------
- A DatabaseInfo object is created when there is a tableInfo.json file in the working directory. There may also be a dataCategories.json, which allows people to access data from categories instead of explicit column names. This DatabaseInfo object is cached and unless tableInfo.json or dataCategories.json is modified, SQLTranslate will use the cached object to general SQL queries.

DatabaseInfo required dictionaries:

tableInfo = 
```json
{
    "climatic":                                       
    {                                                 
        "temporal": 1,
        "dateCol": "day",                          
        "classifier": "staid",
        "columns": ["tmmx","tmmn","pr","pet","vpd",
        "rmax","rmin","th","vs","day","staid"]
    },
    "streamflow":                                     
    {                                                 
        "temporal": 1,
        "dateCol": "date",                             
        "classifier":"staid",
        "columns":["streamflow","date","staid"]
    },
    "stations":                                       
    {
        "temporal": 0,
        "classifier":"staid",
        "columns":["staid","lat","lng"]
    }
}
```

dataCategories = 
```json
{
    "Streamflow": {
        "streamflow":["streamflow"]
    },
   "Temperature":{
        "climatic": ["tmmx","tmmn"]
    },
    "Precipitation":{
        "climatic": ["pr"]
    },
    "Humidity": {
        "climatic": ["pet", "vpd","rmax","rmin"]
    },
    "Wind":{
        "climatic": ["th","vs"]
    }
}
```
- For internal use, SQLTranslate can be run on command line by running "python sqltranslate." The program will prompt for inputs to generate parameters to construct a SQLTranslate object – the resulting query is the return value of SQLTranslate.command().
- For external use (in apps), the run(params) function in the sqltranslate module will cache a DatabaseInfo object and use this object to generate SQL queries which can then be used to access data from a database. 

params formatting:

params = 
```
{
    "categorical": True or False,
    "categories" or "columnList": [...],
    "filters" (if categorical==false): {'columnName': {'type': range or equal, 'values':[lower bound, upper bound] or [list of values]}...} #leave empty if none,
    "aggregate":{'columnName':aggregation method ("sum", "avg ", "med")...} #leave empty if none,
    "aggregateBy" (if there is temporal data): "year", "month", or "day",
    "dateRange" (if there is temporal data): [lower bound, upper bound] #leave out if none
}
```
Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
