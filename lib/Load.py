from pyspark.sql.window import Window
import pyspark.sql.functions as f

from datetime import datetime

class Load:
  def __init__(self, df, path, phase, folder, n_batch):
    self._df = df
    self.sizeDF = self._df.count()
    self.n_batch = n_batch
    self.listPercentile = None
    self.path = path
    self.phase = phase
    self.folder = folder
    self.log = None
    self.kpis = []
    self.transformations = []
  
  def getFullPath(self):
    return f'{self.path}/{self.phase}/transformzone/{self.folder}/'
  
  def setLog(self, log):
    self.log = log
    return self

  def setKpis(self, kpi):
    self.kpis.append(kpi)
    return self
  
  def setTransformations(self, transformation):
    self.transformations.append(transformation)
    return self
  
  def executeKpis(self, batch):
    for kpi in self.kpis:
      batch = kpi.setDF(batch).run().getDF()
      self.log.save(logPoint=f'execute_kpi {kpi.titleKPI}',data={'size_data':batch.count()})
    return batch
  
  def executeTransformations(self, batch):
    for transformation in self.transformations:
      batch = transformation.setDF(batch).run().getDF()
      self.log.save(logPoint=f'execute_kpi {transformation.__str__()}',data={'size_data':batch.count()})
    return batch
  
  def getDF(self):
    return self._df

  
  def generatePercentileList(self):
    self.listPercentile = [(b, b+self.n_batch) for b in range(1,self.sizeDF,self.n_batch)] + [(self.sizeDF, self.sizeDF+self.n_batch)]
    if self.listPercentile[-1][1] > self.sizeDF:
      self.listPercentile = self.listPercentile[0:-1]


  def transformBatch(self, batch_index):
    print(self.listPercentile[batch_index])
    batch = self._df.filter(
        (f.col('INDEX_LOOP') >= self.listPercentile[batch_index][0])&
        (f.col('INDEX_LOOP') < self.listPercentile[batch_index][1])
        )
    self.log.save(logPoint=f'filter_batch_{self.listPercentile[batch_index]}',data={'size_data':batch.count()})
    batch = self.executeKpis(batch)
    batch = self.executeTransformations(batch)
    return batch

  def save(self, batch, mode='overwrite'):
    chunk = datetime.now().strftime('%d_%H_%M_%S')
    path = self.getFullPath() + chunk
    batch.write.mode(mode).parquet(path)
    self.log.save(logPoint=f'save_chunk_batch_{chunk}',data={'size_data':batch.count()})

  def loop(self):
    self.log.save(logPoint=f'start',data={'size_data': self._df.count()})
    self.generatePercentileList()
    print(self.listPercentile)
    batch_index = 0
    while batch_index < len(self.listPercentile):
      bath = self.transformBatch(batch_index)
      self.save(batch=bath)
      #bath.show(n=50)
      batch_index += 1

