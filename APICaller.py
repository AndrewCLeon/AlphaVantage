import json
import numpy
import requests
import Function
import Interval
class AlphaVantageAPI:
    
    APIKey = "FR8QKDRVDCPIC8BB"
    Function = None
    Symbol = None
    Interval = None
    Series = None
    def __init__(self):
        self.Function = Function.Function.TIME_SERIES_WEEKLY
        self.Interval = Interval.Interval.min_1
        self.Symbol = "MSFT"
    
    def GetTimeSeries(self, symbol = None):
        
        #SeriesName = "Time Series (1min)"
        #SeriesName = "Time Series (Daily)"
        SeriesName = "Weekly Time Series"
        
        if symbol is not None:
            self.Symbol = symbol
        
        parameters = { "function":self.Function, "symbol":self.Symbol, "apikey":"FR8QKDRVDCPIC8BB"}
        resp = requests.get('https://www.alphavantage.co/query', params = parameters)
        self.Series = resp.json()
        
        #SeriesName = "Weekly Time Series"
        
        intervalKeys = reversed(list(self.Series[SeriesName].keys()))
        
        dataSet = []
        dataSetIndex = 0
        for interval in intervalKeys:
            propKeys = list(self.Series[SeriesName][interval].keys())
            
            rowSet = []
            rowSet.insert(0, interval)
            rowIndex = 1
            for prop in propKeys:
                value = self.Series[SeriesName][interval][prop]
                rowSet.insert(rowIndex, value)
                rowIndex = rowIndex + 1
                
            dataSet.insert(dataSetIndex, rowSet)
            dataSetIndex = dataSetIndex + 1
        resp.close()
        return dataSet
    
    def TransformDataset(self, dataSet):
        x = []
        y = []
        
        startPrice = dataSet[0][1]
        
        for index in numpy.arange(0, len(dataSet)):
            x.insert(index, dataSet[index][0])
            y.insert(index, ((float(dataSet[index][1]) - float(startPrice))/float(startPrice)) * 100)
            
        return x, y
        
    