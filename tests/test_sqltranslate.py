#!/usr/bin/env python

"""Tests for `sqltranslate` package."""

import pytest
from click.testing import CliRunner
from sqltranslate.sqltranslate import *
from sqltranslate import cli
from sqltranslate.DatabaseInfo import *

@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string

HDDatabase = None

def test_databaseinfo_constructor():
    tableInfo = {
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
    dataCategories = {
        'Streamflow': {
           'streamflow':['streamflow']
        },
        'Temperature':{
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
    global HDDatabase
    HDDatabase = DatabaseInfo(tableInfo,dataCategories)

def test_databaseinfo_categories():
    global HDDatabase
    result = HDDatabase.getColumnsAndTablesFromCategories(['Temperature','Streamflow'])
    
def test_databaseinfo_columnlist():
    global HDDatabase
    result = HDDatabase.getColumnsAndTablesFromColumnList(['tmmx','tmmn','streamflow','pet'])

def test_databaseinfo_rowclassifiers():
    global HDDatabase
    result = HDDatabase.getRowClassifierColumns(['climatic','streamflow'])

def test_databaseinfo_datetime():
    global HDDatabase
    result = HDDatabase.getDateTimeColumns(['climatic','streamflow','stations'])

def test_sqltranslate():
    global HDDatabase
    param1 = {
        'categorical':False,
        'columnList':['tmmx','tmmn','streamflow','pet']
    }
    SQLTran1 = SQLTranslate(HDDatabase,param1)
    param2 = {
        'categorical':True,
        'categories':["Temperature","Streamflow"],
    }
    SQLTran2 = SQLTranslate(HDDatabase,param2)

def test_sqltranslate_agg():
    global HDDatabase
    param1 = {'categorical':False,
    'columnList':['tmmx','tmmn','streamflow','pet'],
    'aggregate':['avg','avg','sum','avg']
    }
    SQLTran1 = SQLTranslateAggregate(HDDatabase,param1)
    param2 =  { 
    'categorical':True,
    'categories':['Temperature','Streamflow','Humidity'],
    'aggregate':['avg','sum','avg']
    }
    SQLTran2 = SQLTranslateAggregate(HDDatabase,param2)
    