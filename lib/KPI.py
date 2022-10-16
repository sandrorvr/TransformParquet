from abc import ABC, abstractclassmethod

class KPI(ABC):
    def __init__(self, titleKPI, columnsUsed, columnsFixed=[]):
        self._df = None
        self.initColumns = None
        self.columnsFixed = columnsFixed
        self.columnsUsed = columnsUsed
        self.titleKPI = titleKPI
        self.labels = []
    
    def __str__(self):
      return 'CLASS_KPI'
    
    def getSelectColumnsToPin(self):
      return self.initColumns + self.columnsFixed + [self.titleKPI]
    
    def getDF(self):
      return self._df.select(*self.getSelectColumnsToPin())

    def setDF(self, df):
      self._df = df
      self.initColumns = df.columns
      return self

    def setLabels(self, lb):
        lb = lb.lower().strip().replace(' ', '_')
        self.labels.append(lb)
        return self
    
    def getLabels(self):
        return self.labels

    @abstractclassmethod
    def configKPI(self):
        pass


    def run(self):
      self.configKPI()
      return self
       
