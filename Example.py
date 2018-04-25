#from APICaller import AlphaVantageAPI
#import matplotlib.pyplot as py
import pandas as pd

#import numpy as np
from SQLServer import SQLServer
from CommandCreator import CommandCreator
#import time

nasdaq = pd.read_csv("AMEX.csv").iloc[:, :-1]
nasdaq = nasdaq.sort_values(by=['Symbol'])

f = open('AMEX.csv', 'r')

contents = f.readline()
symbols = []
while contents is not None:
    ind = 0
    string = ""
    while ind < len(contents):
        string += contents[ind]
        if contents[ind + 1] == str(","):
            break
        
    symbols.append(string)
    contents = f.readline()
    


sql = SQLServer(Server="DESKTOP-T107VRL\SQLEXPRESS", Database="MarketAnalysis", trustedConnection=True)

commandBuilder = CommandCreator()
tempSet = []
epoch = 0
for index, row in nasdaq.iterrows():
    tempSet.insert(index - (epoch * 499), row['Symbol'])
    
    #The maximum insert size is 500 items, don't exceed that
    if index % 499 == 0:
        command = commandBuilder.BuildInsertCommand("AMEX", commandValues = { "Symbol":1 }, dataSet = tempSet)
        sql.ExecuteCommand(command)
        print(command)    
#        file = open("querries.sql", "a+")
#        file.write(command)
#        file.flush()
#        file.close()
        
        #sql.ExecuteCommand(command)
        tempSet = []
        #time.sleep(20)
        epoch = epoch + 1
        
updateSet = []
for index, row in nasdaq.iterrows():
    command = commandBuilder.BuildUpdateCommand("NYSE_Symbols", commandValues = { "CompanyName":row["Name"] }, queryFilter={ "Symbol":row["Symbol"]})
    sql.ExecuteCommand(command)
    print(command)


        
print(command)

from CommandCreator import CommandCreator, CrudType
        
#api = AlphaVantageAPI()
#msftSet = api.GetTimeSeries('MSFT')

com = CommandCreator()
command = com.CrudCommand(CrudType.INSERT, "AMEX", 
                          commandValues = { "TimeStamp":1, "Open":2, "High":3, "Low":4, "Close":5, "Volume":6}, 
                          dataSet = msftSet)


        
