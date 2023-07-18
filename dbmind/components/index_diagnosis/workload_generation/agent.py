# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import os
import logging
from tqdm import tqdm

import torch

from .environ import DBEnviron
from .generation_utils import gen_com
from .model import Actor, SelfCriticCriterion, SingleGRUModel


class WorkloadGeneration:
    def __init__(self, args):
        self.args = args
        self.env = DBEnviron(args)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
    def pg_train(self, train_loader, valid_loader,
                 word2idx, idx2word, col_info, word_info):
        if os.path.exists(self.args.model_load):
            model_source = torch.load(self.args.model_load,
                                      map_location=lambda storage, loc: storage)
            if self.args.model_struct == "Seq2Seq":
                model = Actor(self.args)
            elif self.args.model_struct == "SingleRNN":
                model = SingleGRUModel(self.args)

            model_dict = model.state_dict()
            pretrained_dict = {k: v for k, v in model_source["model"].items()
                               if k in model_dict and "encoder" in k}
            model_dict.update(pretrained_dict)
            model.load_state_dict(model_dict)

            logging.disable(logging.DEBUG)
            logging.info(f"Load the pretrained model from `{self.args.model_load}`.")
            logging.info(f"The set of the pretrained parameters loaded is {list(pretrained_dict.keys())}.")
            logging.disable(logging.INFO)
        else:
            if self.args.model_struct == "Seq2Seq":
                model = Actor(self.args)
            elif self.args.model_struct == "SingleRNN":
                model = SingleGRUModel(self.args)
        logging.disable(logging.DEBUG)
        logging.info(f"The type of the model structure is `{self.args.model_struct}`.")
        logging.disable(logging.INFO)

        criterion = SelfCriticCriterion()
        optimizer = torch.optim.Adam(model.parameters(), self.args.rein_lr)

        model = model.to(self.device)
        criterion = criterion.to(self.device)

        for epoch in tqdm(range(1, self.args.rein_epoch + 1)):
            logging.disable(logging.DEBUG)
            logging.info(f"The `lr` of EP{epoch} is `{optimizer.param_groups[0]['lr']}`.")
            logging.disable(logging.INFO)

            model.train()
            total_loss, total_reward, total_base = 0, 0, 0
            pro_bar = tqdm(enumerate(train_loader))
            for bi, batch in pro_bar:
                pro_bar.set_description(f"Epoch [{epoch}/{self.args.rein_epoch}]")
                optimizer.zero_grad()

                tensor_src, _, sql_tokens = batch
                tensor_src = tensor_src.to(self.device)
                batch_size = tensor_src.size(0)

                greedy_words = model(tensor_src, self.device,
                                     word2idx, idx2word, word_info, col_info,
                                     sql_tokens, True, max_diff=self.args.max_diff)
                sample_words, samlog_props = model(tensor_src, self.device,
                                                   word2idx, idx2word, word_info, col_info,
                                                   sql_tokens, False, max_diff=self.args.max_diff)

                rewards, baseline = list(), list()
                for qi in range(batch_size):
                    rewards.append(torch.tensor(
                        self.env.get_index_reward(sql_tokens[qi],
                                                  sample_words[qi].cpu().numpy(),
                                                  idx2word, col_info)))
                    baseline.append(torch.tensor(
                        self.env.get_index_reward(sql_tokens[qi],
                                                  greedy_words[qi].cpu().numpy(),
                                                  idx2word, col_info)))

                rewards = torch.stack(rewards, 0).to(self.device)
                baseline = torch.stack(baseline, 0).to(self.device)
                advantage = rewards - baseline

                loss = criterion(samlog_props, sample_words,
                                 tensor_src, advantage, self.device)

                total_loss += loss.item()
                pro_bar.set_postfix(rein_loss=total_loss / (bi + 1))

                total_reward += rewards.mean().item()
                pro_bar.set_postfix(reward=total_reward / (bi + 1))

                total_base += baseline.mean().item()
                pro_bar.set_postfix(baseline=total_base / (bi + 1))

                loss.backward()
                optimizer.step()

                gen_com.add_summary_value("train reinforce loss", loss.item())
                gen_com.add_summary_value("train reinforce advantage", advantage.mean().item())
                gen_com.add_summary_value("train reinforce baseline", baseline.mean().item())
                gen_com.add_summary_value("train reinforce reward", rewards.mean().item())
                gen_com.tf_step += 1

                if gen_com.tf_step % 100 == 0:
                    gen_com.summary_writer.flush()

            gen_com.add_summary_value("epoch train reinforce loss", total_loss / (bi + 1), epoch)
            gen_com.add_summary_value("epoch train reinforce reward", total_reward / (bi + 1), epoch)
            gen_com.add_summary_value("epoch train reinforce baseline", total_base / (bi + 1), epoch)

            logging.disable(logging.DEBUG)
            logging.info(f"The final train loss / reward / baseline of EP{epoch} "
                         f"is: {total_loss / (bi + 1)} / {total_reward / (bi + 1)} / {total_base / (bi + 1)}.")
            logging.disable(logging.INFO)

            model.eval()
            total_loss, total_reward, total_base = 0, 0, 0
            pro_bar = tqdm(enumerate(valid_loader))
            for bi, batch in pro_bar:
                pro_bar.set_description(f"Epoch [{epoch}/{self.args.rein_epoch}]")

                tensor_src, _, sql_tokens = batch
                tensor_src = tensor_src.to(self.device)
                batch_size = tensor_src.size(0)

                greedy_words = model(tensor_src, self.device,
                                     word2idx, idx2word, word_info, col_info,
                                     sql_tokens, True, max_diff=self.args.max_diff)
                sample_words, samlog_props = model(tensor_src, self.device,
                                                   word2idx, idx2word, word_info, col_info,
                                                   sql_tokens, False, max_diff=self.args.max_diff)

                rewards, baseline = list(), list()
                for qi in range(batch_size):
                    rewards.append(torch.tensor(
                        self.env.get_index_reward(sql_tokens[qi],
                                                  sample_words[qi].cpu().numpy(),
                                                  idx2word, col_info)))
                    baseline.append(torch.tensor(
                        self.env.get_index_reward(sql_tokens[qi],
                                                  greedy_words[qi].cpu().numpy(),
                                                  idx2word, col_info)))

                rewards = torch.stack(rewards, 0).to(self.device)
                baseline = torch.stack(baseline, 0).to(self.device)
                advantage = rewards - baseline

                loss = criterion(samlog_props, sample_words,
                                 tensor_src, advantage, self.device)

                total_loss += loss.item()
                pro_bar.set_postfix(rein_loss=total_loss / (bi + 1))

                total_reward += rewards.mean().item()
                pro_bar.set_postfix(reward=total_reward / (bi + 1))

                total_base += baseline.mean().item()
                pro_bar.set_postfix(baseline=total_base / (bi + 1))

                gen_com.add_summary_value("valid reinforce loss", loss.item())
                gen_com.add_summary_value("valid reinforce advantage", advantage.mean().item())
                gen_com.add_summary_value("valid reinforce baseline", baseline.mean().item())
                gen_com.add_summary_value("valid reinforce reward", rewards.mean().item())
                gen_com.tf_step += 1

                if gen_com.tf_step % 100 == 0:
                    gen_com.summary_writer.flush()

            gen_com.add_summary_value("epoch valid reinforce loss", total_loss / (bi + 1), epoch)
            gen_com.add_summary_value("epoch valid reinforce reward", total_reward / (bi + 1), epoch)
            gen_com.add_summary_value("epoch valid reinforce baseline", total_base / (bi + 1), epoch)

            logging.disable(logging.DEBUG)
            logging.info(f"The final valid loss / reward / baseline of EP{epoch} "
                         f"is: {total_loss / (bi + 1)} / {total_reward / (bi + 1)} / {total_base / (bi + 1)}.")
            logging.disable(logging.INFO)

            model_state_dict = model.state_dict()
            model_source = {
                "settings": self.args,
                "model": model_state_dict,
                "word2idx": word2idx,
                "idx2word": idx2word,
                "col_info": col_info
            }
            if epoch % self.args.model_save_gap == 0:
                torch.save(model_source, self.args.model_save.format(
                    self.args.exp_id, "PG-train_" + str(epoch)))
