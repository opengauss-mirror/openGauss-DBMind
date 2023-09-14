import torch
import torch.nn as nn
import numpy as np
import logging
import matplotlib.pyplot as plt

from transformers import DataCollatorWithPadding, AdamW, get_scheduler

from modeling_bart import BartTokenizer, BartModel, BartConfig, BartPretrainedModel, BartForConditionalGeneration
from processing import Dataset
from tqdm.auto import tqdm
from processing import Dataset


MODEL_PATH = r"/data1/yhzheng/Bart_Sql_Gen/resource/"
device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
logging.basicConfig(
        filename='./Bart_Sql_Gen/result/experiment.log',
        format = '%(filename)s %(levelname)s %(message)s',
        level=logging.INFO)

class LoadPretrainedBartModel():
    def __init__(self):
        self.model_config = BartConfig.from_pretrained(MODEL_PATH)

        # self.model_config.output_hidden_states = True
        # self.model_config.output_attentions = True

        self.model = BartModel.from_pretrained(MODEL_PATH, config=self.model_config)
        self.task = BartForConditionalGeneration.from_pretrained(MODEL_PATH, config=self.model_config)
        self.tok = BartTokenizer.from_pretrained(MODEL_PATH)

    def get_model(self):
        return self.model

    def get_config(self):
        return self.model_config

    def get_task(self):
        return self.task
    
    def get_tok(self):
        return self.tok

class BartSqlGenModel(BartPretrainedModel):
    def __init__(self, config: BartConfig, tok: BartTokenizer, model: BartModel, task: BartForConditionalGeneration, data: Dataset):
        super().__init__(config)
        self.batchsize = 1
        self.train_data,self.test_data = self.dataLoader(data)
        self.tok = tok
        self.BartConditionalGeneration = task
        self.model = model

    
    def train(self):
        optimizer = AdamW(self.BartConditionalGeneration.parameters(), lr=5e-5)
        self.num_epochs = 10
        num_training_steps = self.num_epochs * len(self.train_data)
        lr_scheduler = get_scheduler(
            "linear",
            optimizer=optimizer,
            num_warmup_steps=0,
            num_training_steps=num_training_steps,
        )
        progress_bar = tqdm(range(num_training_steps))
        
        total_epoch_loss = []
        for epoch in range(self.num_epochs):   
            batch_loss = []
            total_loss = 0        
            for batch in self.train_data:                
                labels = self.tok(batch, return_tensors="pt")["input_ids"]
                input_ids = self.mask_data(labels,epoch)
                
                
                output = self.BartConditionalGeneration(input_ids = input_ids,labels= labels)
                logits = output.logits
                masked_index = (input_ids[0] == self.tok.mask_token_id)
                masked_index = torch.nonzero(masked_index).item()
                probs = logits[0, masked_index].softmax(dim=0)
                # values, predictions = probs.topk(5)
                # print(values)
                # print(predictions)
                # print(self.tok.decode(predictions).split())

               
                loss = output.loss
                total_loss += loss
                batch_loss.append(loss)

                loss.backward()

                optimizer.step()
                lr_scheduler.step()
                optimizer.zero_grad()
                progress_bar.update(1)
            if epoch == 1:
                self.draw_pictures(batch_loss)
            total_epoch_loss.append(total_loss)
        logging.info("total_epoch_loss:" + str(total_epoch_loss))
    
    def evaluate(self):
        for batch in self.test_data:
            for i in batch: 
                labels = i
                input = "<s> <mask> : " + i.split(":")[1]

                # labels = self.tok(labels, return_tensors="pt")["input_ids"]
                input_ids = self.tok(input, return_tensors="pt")["input_ids"]

                logits = self.BartConditionalGeneration.generate(input_ids)
                final = self.tok.batch_decode(logits, skip_special_tokens= True)
                logging.info("input: " + str(input))
                logging.info("labels: " + str(labels))
                logging.info("predict：" + str(final))
                logging.info("###############")

    def draw_pictures(self,batch_loss):
        loss = []
        for i in batch_loss:
            loss.append(i.detach().numpy())
        plt.figure(figsize=(30,10), dpi=300)
        plt.plot(loss)
        f = plt.gcf()
        f.savefig(r"./pictures.png")
        f.clear()

    
    def mask_data(self,batch,epoch):
        if epoch <= self.num_epochs:
            for i in batch:
                choose_mask_method = np.random.rand()
                if choose_mask_method < 1:
                    a = np.random.randint(0,len(i)-1)
                    i[a] = self.tok.mask_token_id
                # elif choose_mask_method < 0.9:
                #     a = np.random.randint(0,len(batch)-1)
                #     batch[a] = batch[np.random.randint(0,len(batch)-1)]
        return batch

    def dataLoader(self,data):
        count = 0
        train_data = []
        test_data = []
        train_batch = []
        test_batch = []

        for i in data:
            if count%10 != 9 and count%10 != 8:
                if len(train_batch) < self.batchsize:
                    train_batch.append(i)
                else:
                    train_data.append(train_batch)
                    train_batch = []
            else:
                if len(test_batch) < self.batchsize:
                    test_batch.append(i)
                else:
                    test_data.append(test_batch)
                    test_batch = []
            count +=1
        

        return train_data, test_data

if __name__ == '__main__':
    data = Dataset()
    dataset = data()

    PretrainModel = LoadPretrainedBartModel()
    config = PretrainModel.get_config()
    model = PretrainModel.get_model()
    task = PretrainModel.get_task()
    tok = PretrainModel.get_tok()

    GenModel = BartSqlGenModel(config,tok,model,task,data)
    GenModel.train()
    GenModel.evaluate()





    
    
