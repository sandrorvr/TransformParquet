from abc import ABC, abstractclassmethod

class PRE_SAVE(ABC):
  def __init__(self):
    self._df = None

  def setDF(self, df):
    self._df = df
    return self
  
  def getDF(self):
    return self._df

  @abstractclassmethod
  def configTransformation(self):
    pass
  
  def run(self):
    self.configTransformation()
    return self