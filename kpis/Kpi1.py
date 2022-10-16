class kpi1(KPI):
  def __init__(self, titleKPI, columnsUsed, columnsFixed=[]):
    super().__init__(titleKPI,columnsUsed, columnsFixed)
  
  def configKPI(self):
    self._df = self._df.withColumn(\
                                   self.titleKPI, 
                                   f.when(
                                       f.col(self.columnsUsed[0])%2 == 0, 
                                       self.labels[0]
                                       ).otherwise(self.labels[1])
                                   )
    def __str__(self):
        return 'kpi1_remove_end_counts_duplicates'