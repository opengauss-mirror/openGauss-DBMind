import json
import sys
import time

import torch
import numpy as np

from torch.utils.data import DataLoader
import logging
from transformers import BartTokenizer

CONFIGURATION_FILE = json.load(open(sys.argv[1]))

DATA_PATH = CONFIGURATION_FILE["experiments_root"] + '/workload_generation/BartSqlGen/resource/sql.json'
MODEL_PATH = CONFIGURATION_FILE["experiments_root"] + '/workload_generation/BartSqlGen/resource/'



class DataProcessing():
    def __init__(self):
        self.original_data = []
        op = open(DATA_PATH).readlines()
        for i in op:
            batch = json.loads(i)
            self.original_data.append(batch)
        self.table = []

    def __call__(self):
        self.string_data = self.process2String()
        self.train_data, self.test_data = self.DivideTrainTest(self.string_data)
        self.train_data_token = self.string2Token(self.train_data)
        self.test_data_token = self.string2Token(self.test_data)
        self.train_loader = self.string2Dataloader(self.train_data_token)
        self.test_loader = self.string2Dataloader(self.test_data_token)
        all_char = self.check()
        return self.train_loader, self.test_loader

    def DivideTrainTest(self, data):
        count = 0
        train_data = []
        test_data = []
        train_batch = []
        test_batch = []

        for i in data:
            if "tmp" in i or "(select" in i:
                continue
            if count % 10 != 9 and count % 10 != 8:
                train_data.append(i)
            else:
                test_data.append(i)
            count += 1

        return train_data, test_data

    def check(self):
        countUnknown = 0
        all_char = []
        for i in self.train_data_token:
            for j in i:
                j = j["input_ids"]
                for k in j:
                    if k == 3:
                        countUnknown += 1
                    if k not in all_char:
                        all_char.append(k)
        logging.info("Count Unknown Word:" + str(countUnknown))
        logging.info("The length of vocab: " + str(len(all_char)))
        return all_char

    def process2String(self):
        string_data = []
        for i in self.original_data:
            sql = self.substitueUnknownWord(i['sql'])
            index = self.substitueUnknownWord(i['indexes'])
            select = sql.split("from")[0]
            _from = "from" + (sql.split("from")[1]).split(";")[0]
            if "order by" in _from:
                orderby = "order by" + _from.split("order by")[1]
                before_orderby = _from.split("order by")[0]
                sql = before_orderby + select + orderby
            else:
                sql = "from" + (sql.split("from")[1]).split(";")[0] + " " + select

            # string_batch = "<s> " + sql + " </s></s> " + i['reward'] + " : " + index + " </s> "
            string_batch = sql + " </s> " + "The improvement of " + index + " is " + i["reward"]

            # 舍去不优质的数据
            if len(index) > 50 and float(i["reward"]) < 20:
                continue
            if len(sql) > 200 and float(i["reward"]) < 20:
                continue
            if float(i["reward"]) < 10:
                continue
            string_data.append(string_batch)
        return string_data

    def string2Dataloader(self, data_token):
        data_it = DatasetIterater(data_token)
        train_loader = data_it()
        return train_loader

    def substitueUnknownWord(self, sequence):
        # while "_" in sequence and "." in sequence:
        #     sequence = sequence.split(".")[0] + " " + sequence.split("_")[1]
        return sequence

    def get_tok(self):
        return self.tok

    def string2Token(self, string_data):
        self.tok = BartTokenizer.from_pretrained(MODEL_PATH)
        TokenDatasetLabel = []
        TokenDatasetWordMask = []
        TokenDatasetIndexMask = []
        TokenDatasetSqlMask = []
        for i in string_data:
            input_ids = self.tok(i)

            s = ""
            t = i.split(" ")
            a = np.random.randint(0, len(t) - 1)
            t[a] = "<mask>"
            s = " ".join(t)
            WordMask = self.tok(s)

            i2 = i.split("of")[0] + "of  <mask> is" + i.rsplit(" is", 1)[1]
            indexmask = self.tok(i2)

            i3 = "<mask> </s> " + i.split("</s>", 1)[1]
            sqlmask = self.tok(i3)

            TokenDatasetLabel.append(input_ids)
            TokenDatasetWordMask.append(WordMask)
            TokenDatasetIndexMask.append(indexmask)
            TokenDatasetSqlMask.append(sqlmask)
        TokenDataset = [TokenDatasetLabel, TokenDatasetWordMask, TokenDatasetIndexMask, TokenDatasetSqlMask]
        # if i == 1:
        #     logging.info("##########")
        #     logging.info(TokenDataset)
        return TokenDataset


class DatasetIterater(torch.utils.data.Dataset):
    def __init__(self, text):
        self.texta = [batch["input_ids"] for batch in text[0]]
        self.textb = [batch["input_ids"] for batch in text[1]]
        self.textc = [batch["input_ids"] for batch in text[2]]
        self.textd = [batch["input_ids"] for batch in text[3]]

    def __getitem__(self, item):
        return self.texta[item], self.textb[item], self.textc[item], self.textd[item]

    def __len__(self):
        return len(self.texta)

    def collate_fn(self, batch_data, pad=1):
        texta, textb, textc, textd = list(zip(*batch_data))
        max_len = max(len(seq) for seq in texta)
        texta = [seq + [pad] * (max_len - len(seq)) for seq in texta]
        textb = [seq + [pad] * (max_len - len(seq)) for seq in textb]
        textc = [seq + [pad] * (max_len - len(seq)) for seq in textc]
        textd = [seq + [pad] * (max_len - len(seq)) for seq in textd]
        texta = torch.LongTensor(texta)
        textb = torch.LongTensor(textb)
        textc = torch.LongTensor(textc)
        textd = torch.LongTensor(textd)
        return (texta, textb, textc, textd)

    def __call__(self):
        train_loader = DataLoader(dataset=self, batch_size=1, shuffle=True, collate_fn=self.collate_fn)
        return train_loader


if __name__ == '__main__':
    data = DataProcessing()
    data()
    pass
