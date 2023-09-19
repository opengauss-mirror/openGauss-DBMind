import json

from transformers.utils import logging
from transformers import BartTokenizer


DATA_PATH = r"/data1/yhzheng/Bart_Sql_Gen/resource/sql.json"
MODEL_PATH = r"/data1/yhzheng/Bart_Sql_Gen/resource/"

logging.set_verbosity_info()
logger = logging.get_logger("transformers")

class Dataset():
    def __init__(self):
        self.original_data = []
        op = open(DATA_PATH).readlines()
        for i in op:
            batch = json.loads(i)
            self.original_data.append(batch)
    
    def __call__(self):
        self.process2String()
        self.string2Token()
        all_char = self.check()
        return self.string_data
    
    def check(self):
        countUnknown = 0
        all_char = []
        for i in self.dataset:
            for j in i:
                if j == 3:
                    countUnknown += 1
                if j not in all_char:
                    all_char.append(j)
        print(countUnknown)
        print(len(all_char))
        print("end")
        return all_char

    def process2String(self):
        self.string_data = []
        for i in self.original_data:
            sql = self.substitueUnknownWord(i['sql'])
            index = self.substitueUnknownWord(i['indexes'])
            string_batch = "<s> " + sql + " </s></s> " + i['reward'] + " : " + index + " </s> " 
            self.string_data.append(string_batch)
            if  i == 1:
                logger.info("##########")
                logger.info(self.string_data)
    
    def substitueUnknownWord(self,sequence):
        while "_" in sequence and "." in sequence:
            sequence = sequence.split(".")[0] + " " + sequence.split("_")[1]
        return sequence

    def get_tok(self):
        return self.tok               
    
    def string2Token(self):
        self.tok = BartTokenizer.from_pretrained(MODEL_PATH)
        TokenDataset = []
        for i in self.string_data:
            input_ids = self.tok(i)["input_ids"]
            TokenDataset.append(input_ids)
            if i == 1: 
                logger.info("##########")
                logger.info(TokenDataset)
        self.dataset = TokenDataset


if __name__ == '__main__':
    data = Dataset()
    data()
