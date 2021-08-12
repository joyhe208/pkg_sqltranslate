"""Main module."""
from itertools import repeat, combinations
# params:
# {
#     'categorical':False,
#     'columnList':['tmmx','tmmn','streamflow','pet'],
#     'filters': {
#         'tmmx': {
#             'type':'range',
#             'values': [150,160]
#         },
#         'staid': {
#             'type':'equal',
#             'values':[1,2]
#         }
#     }
# }
# {
#     'categorical':False,
#     'columnList':['tmmx','tmmn','streamflow','pet'],
#     'filters':None
#     'id': None
# }
# {
#     'categorical':True,
#     'categories':["Temperature","Streamflow"],
# }
#{
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
# {    'categorical':False,
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
        self.filters = []
        if(not self.params['categorical'] and 'filters' in self.params):
            colSet = set()
            for i in range(len(self.selectedColumns)):
                currCol = self.selectedColumns[i].split('.')[1].lower()
                if(currCol not in self.params['filters'] or currCol in colSet):
                    self.filters.append(None)
                else:
                    colSet.add(currCol)
                    currFilt = self.params['filters'][currCol]
                    if(currFilt['type']=='range'):
                        self.filters.append(("(" + self.selectedColumns[i] + " BETWEEN " + "\"" + currFilt['values'][0] + "\" AND " + "\"" + currFilt['values'][1] + "\")") if isinstance(currFilt['values'][0],str) else self.filters.append("(" + self.selectedColumns[i] + " BETWEEN " +  str(currFilt['values'][0]) + " AND " + str(currFilt['values'][1]) + ")"))
                    else:
                        self.filters.extend(list(map(lambda filt: "(" + selectedColumns[i] + "=" + "\"" + filt + "\")", currFilt['values']))) if isinstance(currFilt['values'][0],str) else self.filters.extend(list(map(lambda filt: "(" + self.selectedColumns[i] + "=" +  str(filt) + ")", currFilt['values'])))
        print(self.filters)
        self.command = self.makeCommand()

    def select(self):
        return "SELECT " + ', '.join(self.selectedColumns)
    
    def fromTables(self):
        return "FROM " + ' inner join '.join(list(map(lambda t: "\"" + t.upper() + "\" as " + t, self.selectedTables)))
    
    def on(self):
        if(len(self.join)):
            return "on " + ' and '.join(list(map(lambda pair: pair[0] + "=" + pair[1],self.join)))
        return ""

    def where(self):
        if(len(self.filters)):
            return "WHERE " + ' and '.join([filter for filter in self.filters if filter!=None])
        return ""

    def makeCommand(self):
        return self.select() + "\n" + self.fromTables() + "\n" + self.on()  + "\n" + self.where()

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
        self.filters = []
        if(not self.params['categorical'] and 'filters' in self.params):
            colSet = set()
            for i in range(len(self.selectedColumns)):
                currCol = self.selectedColumns[i].split('.')[1].lower()
                if(currCol not in self.params['filters'] or currCol in colSet):
                    self.filters.append(None)
                else:
                    colSet.add(currCol)
                    currFilt = self.params['filters'][currCol]
                    if(currFilt['type']=='range'):
                        self.filters.append(("(" + self.selectedColumns[i] + " BETWEEN " + "\"" + currFilt['values'][0] + "\" AND " + "\"" + currFilt['values'][1] + "\")") if isinstance(currFilt['values'][0],str) else self.filters.append("(" + self.selectedColumns[i] + " BETWEEN " +  str(currFilt['values'][0]) + " AND " + str(currFilt['values'][1]) + ")"))
                    else:
                        self.filters.extend(list(map(lambda filt: "(" + selectedColumns[i] + "=" + "\"" + filt + "\")", currFilt['values']))) if isinstance(currFilt['values'][0],str) else self.filters.extend(list(map(lambda filt: "(" + self.selectedColumns[i] + "=" +  str(filt) + ")", currFilt['values'])))
        self.command = self.makeCommand()
    
    def groupBy(self):
        return "GROUP BY " + ", ".join(self.groupByCols) 

    def makeCommand(self):
        return (super().makeCommand() + "\n" + self.groupBy())

class SQLTranslateTemporal(SQLTranslateAggregate):
    def __init__(self,databaseInfo,params):
        self.aggregateBy = params['aggregateBy']
        self.dateRange = list(map(lambda date: "\"" + date + "\"", params['dateRange'])) if self.aggregateBy == 'day' else list(map(lambda date: self.aggregateBy.upper() + "(\"" + date +"\")",params['dateRange']))
        super().__init__(databaseInfo,params)
    
    def on(self):
        dateCols = self.databaseInfo.getDateTimeColumns(self.selectedTables)
        datePairs = list(combinations(dateCols,2))
        self.join.extend(datePairs)
        return super().on()

    def where(self):
        dateCol = self.databaseInfo.getDateTimeColumns(self.selectedTables)[0]
        self.filters.append(f"({dateCol} BETWEEN DATE(\"{self.params['dateRange'][0]}\") AND DATE(\"{self.params['dateRange'][1]}\"))")
        return super().where()

    def groupBy(self):
        dateCol = self.databaseInfo.getDateTimeColumns(self.selectedTables)[0]
        if(self.aggregateBy=='month'):
            self.groupByCols.extend(["MONTH("+dateCol+")","YEAR("+dateCol+")"])
        elif(self.aggregateBy=='year'):
            self.groupByCols.append("YEAR("+dateCol+")")
        else:
            self.groupByCols.append(dateCol)
        return super().groupBy()

