import pyodbc as odbc
from CommandCreator import CrudType, CommandCreator

class SQLServer:
    
    ConnectionString = None
    Connection = None
    Cursor = None
    
    CommandCreator = None
    
    def __init__(self, Server="", Database="", Username="", Password="", trustedConnection = False):
        self.CommandCreator = CommandCreator()
        
        self.ConnectionString = "Driver=SQL Server Native Client 11.0;server={server};database={database};".format(server=Server, database = Database)
        
        if trustedConnection == False:
            self.ConnectionString += "username={username};password={password}".format(username = Username, password = Password)
        else:
            self.ConnectionString += "Trusted_Connection=yes;"
        
        #print("Connecting using this connection string \r\n {connectionString}".format(connectionString = self.ConnectionString))

        try:
            self.Connection, self.Cursor = self.Connect()
            result = self.Cursor.execute('SELECT GETDATE()').fetchall()
            
            print ("Found {result} rows".format(result = len(result)))
            
            if len(result) >= 1:
                print("Connection successful")
            else:
                print("Unable to create connection")
                return None
        except Exception as ex:
            print("Unable to create connection, failed with exception: {exception}".format(exception=type(ex).__name__))
        finally:
            self.Disconnect()
        
    def ExecuteCommand(self, command):
        
        try:
            self.Connection, self.Cursor = self.Connect()
            self.Cursor.execute(command)
            #for row in res:
             #   print('row = %r' % (row,))
            
        except Exception as ex:
            print("NonQuery failed with exception: {exception}".format(exception=type(ex).__name__))
        finally:
            self.Disconnect(commit=True)
        
    def ExecuteNonQuery(self, table, commandType:CrudType, commandValues:dict = None, queryFilter:dict = None, dataSet:list = None):
        
        try:
            command = self.CommandBuilder.CrudCommand(commandType, table, commandValues, queryFilter, dataSet)
            #command = self.BuildCommand(table,commandType, commandValues, queryFilter)
            self.Connection, self.Cursor = self.Connect()
            self.Cursor.execute(command)
            self.Disconnect(commit=True)
        except Exception as ex:
            print("NonQuery failed with exception: {exception}".format(exception=type(ex).__name__))
        finally:
            self.Disconnect(commit=True)
        
    def ExecuteQuery(self, table, commandValues:dict):
        try:
            command = self.CommandBuilder.CrudCommand(CrudType.SELECT, table, commandValues)
            #command = self.BuildCommand(table, "SELECT", commandValues)
            self.Connection, self.Cursor = self.Connect()
            res = self.Cursor.execute(command)
            for row in res:
                print('row = %r' % (row,))
                
        except Exception as ex:
            print("Query failed with Exception:{exception}".format(exception=type(ex).__name__))
        finally:
            self.Disconnect()
        
    def Connect(self):
        connection = odbc.connect(self.ConnectionString)
        return connection, connection.cursor()
    
    def Disconnect(self, commit = False):
        try:   
            if commit == True:
                self.Connection.commit()
        except Exception as ex:
            print("Query failed with Exception:{exception}".format(exception=type(ex).__name__))
        finally:
            self.Cursor.close()
            del self.Cursor
            self.Connection.close()
    
sql = SQLServer(Server="DESKTOP-T107VRL\SQLEXPRESS", Database="MarketAnalysis", trustedConnection=True)

#command = sql.ExecuteNonQuery(table = "AMEX", commandType = "UPDATE", commandValues = {'Symbol':'MSFT', 'Price':1200}, queryFilter = { 'id':500})
#print(command)