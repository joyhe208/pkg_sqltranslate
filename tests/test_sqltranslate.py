#!/usr/bin/env python

"""Tests for `sqltranslate` package."""

import pytest
from click.testing import CliRunner
from sqltranslate import sqltranslate
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
    print(result)

def test_databaseinfo_datetime():
    global HDDatabase
    result = HDDatabase.getDateTimeColumns(['climatic','streamflow','stations'])
