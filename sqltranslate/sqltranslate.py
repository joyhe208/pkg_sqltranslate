"""Main module."""
from itertools import repeat
# params:
# {
#     'categorical':False,
#     'columnList':['tmmx','tmmn','streamflow','pet'],
# }
# {
#     'categorical':True,
#     'categories':["Temperature","Streamflow"],
# }
# {
#     'categorical':False,
#     'columnList':['tmmx','tmmn','streamflow','pet'],
#     'aggregate':
#     {
#         'tmmx': 'avg',
#         'tmmn': 'avg',
#         'streamflow':'sum',
#         'pet':'avg'
#     }
# }
#     'categorical':True,
#     'categories':['Temperature','Streamflow'],
#     'aggregate':['avg','sum']
# }
#     'categorical':False,
#     'columnList':['tmmx','tmmn','streamflow','pet'],
#     'aggregate':['avg','avg','sum','avg']
#     'dateRange':['01-01-2004','01-01-2006']
#     'aggregateBy':'day'
# }
#     'categorical':False,
#     'columnList':['tmmx','tmmn','streamflow','pet'],
#     'aggregate':['avg','avg','sum','avg'],
#     'dateRange':['01-01-2004','01-01-2006'],
#     'aggregateBy':'year'
# }

class SQLTranslate:
    def __init__(self,databaseInfo,params):
        self.params = params
        self.databaseInfo = databaseInfo
        self.selected = databaseInfo.getColumnsAndTablesFromCategories(self.params['categories']) if self.params['categorical'] else databaseInfo.getColumnsAndTablesFromColumnList(self.params['columnList'])
        self.selectedColumns = self.selected['columns']
        self.selectedTables = self.selected['tables']
        self.selectedColumns.extend(self.databaseInfo.getRowClassifierColumns(self.selectedTables))
        self.join = self.selected['commonColumns']
        self.command = self.makeCommand()

    def select(self):
        return "SELECT " + ', '.join(self.selectedColumns)
    
    def fromTables(self):
        return "FROM " + ' inner join '.join(list(map(lambda t: "\"" + t.upper() + "\" as " + t, self.selectedTables)))
    
    def on(self):
        return "on " + ' and '.join(list(map(lambda pair: pair[0] + "=" + pair[1],self.join)))
    
    def where(self):
        return 
    
    def makeCommand(self):
        return self.select() + "\n" + self.fromTables() + ("\n" + self.on() if len(self.join) else "")

    def __str__(self):
        return self.command

class SQLTranslateAggregate(SQLTranslate):
    def __init__(self,databaseInfo,params):
        self.params = params
        self.databaseInfo = databaseInfo
        self.selected = databaseInfo.getColumnsAndTablesFromCategories(self.params['categories']) if self.params['categorical'] else databaseInfo.getColumnsAndTablesFromColumnList(self.params['columnList'])
        self.selectedColumns = []
        self.groupByCols = []
        if(self.params['categorical']):
            self.aggregate = []
            for i in range(len(self.params['categories'])):
                category = self.params['categories'][i]
                categoryDict = self.databaseInfo.dataCategories[category]
                self.aggregate.extend([self.params['aggregate'][i]]*len(list(categoryDict.values())[0]))
        else:
            self.aggregate = self.params['aggregate']

        for i in range(len(self.selected['columns'])):
            currCol = self.selected['columns'][i]
            if(self.aggregate[i]!=None):
                self.selectedColumns.append(self.aggregate[i].upper() + "(" + currCol + ")")
            else:
                self.selectedColumns.append(currCol)
                self.groupByCols.append(currCol)

        self.selectedTables = self.selected['tables']
        self.selectedColumns.extend(self.databaseInfo.getRowClassifierColumns(self.selectedTables))
        self.groupByCols.extend(self.databaseInfo.getRowClassifierColumns(self.selectedTables))
        self.join = self.selected['commonColumns']
        self.command = self.makeCommand()
    
    def groupBy(self):
        return "GROUP BY " + ", ".join(self.groupByCols) 

    def makeCommand(self):
        return (super().makeCommand() + "\n" + self.groupBy())
    

    
# class SQLTranslateTemporal(SQLTranslateAggregate):


