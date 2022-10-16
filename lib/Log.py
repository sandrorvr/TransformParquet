from json import dump
from datetime import datetime

class Log:
  def __init__(self, phase, isVolatiele=True, nameFile='logFile.json'):
    self.phase = phase
    self.nameFile = nameFile
    self.isVolatiele =isVolatiele
    self.dateTime = datetime.now().strftime('"%Y/%m/%d, %H:%M:%S"')

  def save(self,logPoint='corrent', data={}):
    if logPoint == 'start':
      if self.isVolatiele:
        with open(self.nameFile, 'w') as file:
          file.write(f'************** LOG {self.dateTime} PHASE: {self.phase} ***************\n')
          file.write('\n')
      else:
        with open(self.nameFile, 'a') as file:
          file.write(f'************** LOG {self.dateTime} PHASE: {self.phase} ***************\n')
          file.write('\n')
    elif logPoint == 'end':
      with open(self.nameFile, 'a') as file:
        file.write('\n\n')
        file.write(f'************** LOG {self.dateTime} PHASE: {self.phase} ***************')
    else:
      with open(self.nameFile, 'a') as file:
        if type(data) != dict:
          raise TypeError('The data type must be a dictionary [dict].')
        data['logPoint'] = logPoint
        dump(data, file)
        file.write('\n')

