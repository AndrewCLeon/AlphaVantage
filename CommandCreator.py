
from enum import Enum
import numpy as np

class CrudType(Enum):
    INSERT = 0,
    UPDATE = 1,
    DELETE = 2,
    READ = 3

class AlterationType(Enum):
    Add = 'ADD',
    Alter = 'ALTER',
    Drop = 'DROP'

class CommandCreator:
    
    def __init__(self):
        print ("Command Creator initialized...")
        
    #CREATE DATABASE
    #DROP DATABASE
    
    
    #CREATE TABLE
    #ALTER TABLE
    #DROP TABLE
    
    #BACKUP DATABASE
        #TO LOCATION
    
    #ADD CONSTRAINT
        #UNIQUE
        #INDEX
        
    #EXECUTE PROCEDURE
        #WITH PARAMETERS
    
    #ALTER AUTHORIZATION
    def AlterAuthorization(self, objectName:str, newOwner:str, objectType:str = 'database'):
       return "ALTER AUTHORIZATION ON {objectType}::{objectName} TO {newOwner}".format(objectType = objectType, objectName = objectName, newOwner = newOwner)
    
    def DropTable(self, table):
        return "DROP TABLE {table}".format(table = table)
   
    def AlterTable(self, table, target, targetName, dataType = "", alteration:AlterationType = AlterationType.Add):
        
        #Scrub for impossible scenarios
        if target == "COLUMN" and alteration != AlterationType.Drop and dataType == "":
            return 0
        
        command = "ALTER TABLE {table}\r\n".format(table = table)
        command += "{alteration} {target} {targetName} {dataType}".format(table = table, alteration = alteration, target = target, targetName = targetName, dataType = dataType)
        return command
    
    def ExecuteStoredProcedure(self, procedureName, parameters:dict = None):
        command = "EXEC {procedure}\r\n".format(procedure = procedureName)
        
        if parameters is not None:
            command += self.ZipParams(parameters)
        
        return command
    
    def ZipParams(self, parameters:dict, isFilter:bool = False, isDisjunction:bool = False):
        
        if parameters is None:
            return 0
        
        zippedParams = "" 
        
        separator = ","
        if isFilter:
            if isDisjunction:
                separator = "OR"
            else:
                separator = "AND"
        
        keys = list(parameters.keys())
        
        for key in keys:
            value = parameters[key]
            
            if isinstance(value, str):
                zippedParams += "{column} = '{value}'".format(column = str(key), value = str(parameters[key]))
            else:
                zippedParams += "{column} = {value}".format(column = str(key), value = str(parameters[key]))
            
            if(key != keys[len(keys) - 1]):
                if isFilter:
                    zippedParams += "\r\n{separator} ".format(separator = separator)
                else:
                    zippedParams += "{separator}\r\n".format(separator = separator)
            else:
                zippedParams += "\r\n"
        return zippedParams
        
    def CrudCommand(self, typeOf:CrudType, table, commandValues:dict = None, queryFilter:dict = None, dataSet:list = None):
        
        command = None
        if typeOf == CrudType.INSERT:
            command = self.BuildInsertCommand(table, commandValues, dataSet)
        elif typeOf == CrudType.UPDATE:
            command = self.BuildUpdateCommand(table, commandValues, queryFilter)
        elif typeOf == CrudType.DELETE:
            command = self.BuildDeleteCommand(table, queryFilter)
        else:
            command = self.BuildReadCommand(table, commandValues, queryFilter)
        return command
    
    def BuildInsertCommand(self, table, commandValues:dict, dataSet:list):
        
        if commandValues is None or dataSet is None:
            print("Unable to insert with no columns or values specified")
            return None
        
        
        command = "INSERT INTO [dbo].[{table}] (".format(table=table)
        keys = list(commandValues.keys())
        for key in list(keys):
                command += "[{key}]".format(key = key)
                if(key != keys[len(keys) - 1]):
                    command += ","
        command += ")\r\nVALUES "       
        
        
        for row in np.arange(0, len(dataSet)):
            command += "("
            for value in np.arange(0, len(dataSet[row])):
                
                if isinstance(dataSet[row], str):
                    command += "'" + str(dataSet[row]) + "'"
                    break
                elif isinstance(dataSet[row][value], str):
                    command += "'" + str(dataSet[row][value]) + "'"
                elif isinstance(dataSet[row][value], float):
                    command += str(dataSet[row][value])
                elif isinstance(dataSet[row][value], int):
                    command += str(dataSet[row][value])
                
                if value != len(dataSet[row]) - 1:
                    command += ", "
            command += ")"
            
            if row != len(dataSet) - 1:
                command += ",\r\n"
            
        return command
        
    def BuildUpdateCommand(self, table, commandValues:dict, queryFilter:dict):
        if commandValues is None or queryFilter is None:
                print ('Unable to build update command for filter-less query, please provide a filter')
                return 0
            
        command = "UPDATE {table}\r\nSET {zippedAssignments}WHERE {zippedFilter}".format(table=table, zippedAssignments = self.ZipParams(commandValues), zippedFilter = self.ZipParams(queryFilter, isFilter = True))

        return command
        
    def BuildDeleteCommand(self, table, queryFilter:dict):
        
        if queryFilter is None:
            return 0
        
        return "DELETE FROM {table}\r\nWHERE {zippedFiler}".format(table = table, zippedFilter = self.ZipParams(queryFilter, isFilter = True))
    
    def BuildSelectCommand(self, table, commandValues:dict, queryFilter:dict):
        
        if commandValues is None:
            return 0
        
        command = "SELECT "
        
        keys = list(commandValues.keys())
        
        for col in keys:
            command += "{column}"
            if col != keys[len(keys) - 1]:
                command += ","
            command += "\r\n"
        
        command += "FROM {table}\r\nWHERE {zippedFilter}".format(table = table, zippedFilter = self.ZipParams(queryFilter, isFilter = True))
                
        return command
    
    
#    def BuildCommand(self, table, commandType = "INSERT", commandValues:dict = None, queryFilter:dict = None):
#        command = None
#        if commandType == "INSERT":
#            command = "INSERT INTO {table} (".format(table=table)
#            keys = list(commandValues.keys())
#            values = list(commandValues.values())
#            for key in list(keys):
#                    command += key
#                    if(key != keys[len(keys) - 1]):
#                        command += ","
#            
#            command += ")\r\nVALUES ("       
#            for value in list(values):                
#                if isinstance(value, str):
#                    command += "'" + str(value) + "'"
#                elif isinstance(value, int):
#                    command += str(value)
#                    
#                if(value != values[len(values) - 1]):
#                    command += ', '
#            
#            command += ")"
#        elif commandType == "UPDATE":
#            
#            if queryFilter is None:
#                return 0
#            
#            command = "UPDATE {table}\r\n".format(table=table)
#            command += "SET "
#            
#            keys = list(commandValues.keys())
#            filterKeys = list(queryFilter.keys())
#            
#            for key in keys:
#                value = commandValues[key]
#                
#                if isinstance(value, str):
#                    command += "{column} = '{value}'".format(column = key, value = commandValues[key])
#                else:
#                    command += "{column} = {value}".format(column = key, value = commandValues[key])
#                
#                if(key != keys[len(keys) - 1]):
#                    command += ",\r\n"
#                else:
#                    command += "\r\n"
#            command += "WHERE "
#            
#            for col in filterKeys:
#                value = queryFilter[col]
#                
#                if isinstance(value, str):
#                    command += "{column} = '{value}'".format(column=col, value=queryFilter[col])
#                else:
#                    command += "{column} = {value}".format(column=col, value=queryFilter[col])
#                    
#                if col != filterKeys[len(filterKeys) - 1]:
#                    command += " AND "
#        return command 