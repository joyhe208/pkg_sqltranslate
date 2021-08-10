import re
import numpy as np

class DatabaseTable:
    def __init__(name,params):
        self.name = name
        self.params = params
    
    def getColumns(self):
        return self.params['columns']

    def getRowClassifier(self):
        return self.params['classifier'].upper()
    
    def __hash__(self):
        return hash(name)
    
    def __eq__(self,other):
        return self.name == other.name

class DatabaseTemporalTable(DatabaseTable):
    def __init__(name,params):
        super().__init__(self,params)

    def getDateCol(self):
        return self.params['dateCol']

class TablePair:
    def __init__(table1, table2):
        self.tablePair = (table1,table2)

    def __getitem__(self,key):
        self.tablePair[key]

    def __hash__(self):
        return hash(self.tablePair)
        
    def __eq__(self,other):
        return self.tablePair == other.tablePair

#this class keeps track of database metadata to make SQL queries built from the SQLTranslate module more specific
#also will reduce processing time for SQL Translate as it can access from a cache
class DatabaseInfo:
    ############CONSTRUCTOR PARAMETERS#######################
    #table info consists of table metadata                  #
    #add columns too
    # ex: 
    # {
    #     'climatic':                                       #
    #     {                                                 #
    #         'temporal': True,
    #         'dateCol': 'day'                              #
    #         'classifier': ['staid'],
    #         'columns': ['tmmx','tmmn','pr','pet','vpd',
    #         'rmax','rmin','th','vs','day','staid'],
    #         'commonColumns':{
    #             'streamflow':[('staid','staid'),          #
    #             ('day','date')],
    #             'stations':[('staid','staid')]
    #         }
    #     }
    #     'streamflow':                                     #
    #     {                                                 #
    #         'temporal': True,
    #         'dateCol': 'date'                             #
    #         'classifier':['staid'],
    #         'columns':['streamflow','date','staid']
    #         'commonColumns':{
    #             'climatic':[('staid','staid'),            #
    #             ('day','date')],
    #             'stations': [('staid','staid')]
    #         }       
    #     }
    #     'stations':                                       #
    #     {
    #         'temporal': False,                            #
    #         'classifiers':['staid']                       #
    #         'columns':['staid','lat','lng']
    #         "commonColumns":{
    #             'climatic':[('staid','staid')],
    #             'streamflow':[('staid','staid')]
    #         }
    #     }                                                 #
    # }
    #data categories to limit manual column entry 
    #ex:
    # {
    #     'Streamflow': {
    #        'streamflow':['streamflow']
    #     },
    #     'Temperature':{
    #            'climatic': ['tmmx','tmmn']
    #     },
    #     'Precipitation':{
    #         'climatic': ['pr']
    #     },
    #     'Humidity': {
    #         'climatic': ['pet', 'vpd','rmax','rmin']
    #     },
    #     'Wind':{
    #        'climatic': ['th','vs']
    #     }
    # }
    #common columns lists any common-valued columns that    #
    # may or may not have different names across tables     #
    #ex:
    # {
       
    # }                                                   #
    ########################################################

    def __init__(self, tableInfo, dataCategories=None):
        #add letter classifier
        self.tables = {}
        for table in tableInfo:
            self.tables[table] = DatabaseTemporalTable(table,tableInfo[table]) if tableInfo[table]['temporal'] else DatabaseTable(table,tableInfo[table])
        self.dataCategories = dataCategories
        self.columnChoices = {}
        self.commonColumns = {}
        for table in tableInfo:
            self.generateColumnChoices(table)
        self.temporalDataTables = [table for table in tableInfo if table['temporal']]
    
    def generateColumnChoices(self,table):
        for column in self.tables[table].getColumns():
            if(column in self.columnChoices):
                self.commonColumns[TablePair(table,self.columnChoices[column])] = column
            else:
                self.columnChoices[column] = table

    def getColumnsandTablesFromCategories(self,categories):
        columnList = []
        tableList = []
        for category in categories:
            for table, items in self.dataCategories[category]:
                tableList.append(table)
                columnList.extend(list(map(lambda columnName: table+"." + columnName.upper(), items)))
        tablePairs = [tablePair for tablePair in self.commonColumns.keys() if (tablePair[0] in tableList) and (tablePair[1] in tableList)]
        commonColumns =  map(lambda tablePair: (tablePair[0] + "." + self.commonColumns[tablePair], tablePair[1] + + "." + self.commonColumns[tablePair]), tablePairs)
        return {'columns': columnList,
                'tables': tableList,
                'commonColumns':commonColumns}

    def getColumnsandTablesFromColumnList(self,columns):
        #deal with common columns here
        columnList = map(lambda columnName: self.columnChoices[columnName]+"."+columnName.upper(),columns)
        tableList = set([self.columnChoices[columnName] for columnName in columns])
        tablePairs = [tablePair for tablePair in self.commonColumns.keys() if (tablePair[0] in tableList) and (tablePair[1] in tableList)]
        commonColumns =  map(lambda tablePair: (tablePair[0] + "." + self.commonColumns[tablePair], tablePair[1] + + "." + self.commonColumns[tablePair]), tablePairs)
        return {'columns': columnList,
                'tables': tableList,
                'commonColumns':commonColumns}

    def getRowClassifierColumns(self,tables):
        return map(lambda table: table+"."+ self.tables[table].getRowClassifier().upper(),tables)

    def getDateTimeColumns(self,tables):
        return map(lambda table: table+ "."+ self.tables[table].getDateCol.upper(),[t for t in self.tables if t in self.temporalDataTables])
    

