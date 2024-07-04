from Collectors.DataCollectors.CA_DataCollector import CA_DataCollector
from Collectors.DataCollectors.IL_DataCollector import IL_DataCollector
from Collectors.DataCollectors.TN_DataCollector import TN_DataCollector
from Collectors.DataCollectors.test_UK_collector import UK_DataCollector
# from Collectors.DataCollectors.USA_DataCollector import USA_DataCollector


class CollectorManager:
    def __init__(self, batch_size):
        self.batch_size = batch_size
        
        self.collectors = [IL_DataCollector(batch_size)]


    def run_collectors(self):
        for collector in self.collectors:
            collector.get_debates()
