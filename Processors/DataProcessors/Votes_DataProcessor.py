from Processors.DataProcessors.DataProcessor import DataProcessor

class Votes_DataProcessor(DataProcessor):
    def __init__(self, batch_size):
        super(Votes_DataProcessor, self).__init__(batch_size)

        self.table = "bills"

    
    def to_csv(self):
        pass


    def process_data(self):
        pass
    

    def process_UK(self):
        pass


    def process_IL(self):
        pass


    def process_USA(self):
        pass


    def process_TN(self):
        pass


    def process_CA(self):
        pass