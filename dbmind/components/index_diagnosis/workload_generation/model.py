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

import torch
import torch.nn as nn
import torch.nn.functional as F

import copy
import random
import numpy as np

from .generation_utils import constants, mod_sql
from .generation_utils.gen_com import dbmind_assert


class CrossEntropy(nn.Module):
    def __init__(self):
        super(CrossEntropy, self).__init__()

    def forward(self, props, tgt, device):
        props = torch.cat((props, torch.zeros(props.size(0), tgt.size(1)
                                              - props.size(1), props.size(2)).to(device)), 1)
        tgt_props = props.gather(2, tgt.unsqueeze(2)).squeeze()
        mask = (tgt > 2).float()
        return -(tgt_props * mask).sum() / mask.sum()


class SelfCriticCriterion(nn.Module):
    def __init__(self):
        super(SelfCriticCriterion, self).__init__()

    def forward(self, samlog_props, sample_words,
                tgt_seq, advantage, device):
        """

        :param samlog_props: torch.Size([16, 17, 9049]), the output of F.log_softmax()
        :param sample_words: torch.Size([16, 17])
        :param tgt_seq: torch.Size([16, 17])
        :param advantage: torch.Size([16])
        :param device:
        :return:
        """
        
        if advantage.size(0) > 1:
            advantage = (advantage - advantage.mean()) / advantage.std().clamp(min=1e-8)
        
        mask = (tgt_seq > 0).float()

        s_props = samlog_props.gather(2, sample_words.unsqueeze(2)).squeeze(-1)
        s_props = torch.cat((s_props, torch.zeros(s_props.size(0), mask.size(1)
                                                  - s_props.size(1)).to(device)), -1)

        advantage = advantage.unsqueeze(1).repeat(1, mask.size(1))
        advantage = advantage.detach()

        return - (s_props * mask * advantage).sum() / mask.sum()


class EncoderRNN(nn.Module):
    def __init__(self, input_size, emb_size, enc_hidden_size,
                 dec_hidden_size, is_bid, dropout, rnn_type):
        super(EncoderRNN, self).__init__()
        self.is_bid = is_bid
        self.rnn_type = rnn_type
        self.enc_hidden_size = enc_hidden_size
        
        self.embedding = nn.Embedding(input_size, emb_size, padding_idx=constants.PAD)
        self.dropout = nn.Dropout(dropout)
        if is_bid:
            if rnn_type == "GRU":
                self.gru = nn.GRU(emb_size, enc_hidden_size, batch_first=True, bidirectional=True)
            elif rnn_type == "LSTM":
                self.gru = nn.LSTM(emb_size, enc_hidden_size, batch_first=True, bidirectional=True)
            self.adapter = nn.Linear(enc_hidden_size * 2, dec_hidden_size)
        else:
            if rnn_type == "GRU":
                self.gru = nn.GRU(emb_size, enc_hidden_size, batch_first=True)
            elif rnn_type == "LSTM":
                self.gru = nn.LSTM(emb_size, enc_hidden_size, batch_first=True)
            self.adapter = nn.Linear(enc_hidden_size, dec_hidden_size)

    def init_hidden(self, batch_size, device):
        if self.is_bid:
            return torch.zeros(2 * 1, batch_size, self.enc_hidden_size, device=device)
        else:
            return torch.zeros(1 * 1, batch_size, self.enc_hidden_size, device=device)

    def forward(self, input_word, hidden):
        embedded = self.dropout(self.embedding(input_word))
        if self.rnn_type == "GRU":
            output, hidden = self.gru(embedded, hidden)
        elif self.rnn_type == "LSTM":
            output, (hidden, cell) = self.gru(embedded, (hidden, hidden))

        if self.is_bid:
            last_hidden = torch.cat((hidden[-2, :, :], hidden[-1, :, :]), dim=1)
            hidden = torch.tanh(self.adapter(last_hidden))
        else:
            hidden = hidden.squeeze(0)

        return output, hidden


class Attention(nn.Module):
    def __init__(self, enc_hidden_size, dec_hidden_size, is_bid):
        super(Attention, self).__init__()
        if is_bid:
            self.attn = nn.Linear((enc_hidden_size * 2) + dec_hidden_size, dec_hidden_size)
        else:
            self.attn = nn.Linear((enc_hidden_size * 1) + dec_hidden_size, dec_hidden_size)
        self.v = nn.Linear(dec_hidden_size, 1, bias=False)

    def forward(self, src_tensor, encoder_outputs, decoder_hidden, inf):
        """
        :param encoder_outputs: [batch size, src len, enc hid dim * 2]
        :param decoder_hidden: [batch size, dec hid dim]
        :return:
        """
        src_len = encoder_outputs.shape[1]

        decoder_hidden = decoder_hidden.unsqueeze(1).repeat(1, src_len, 1)

        energy = torch.tanh(self.attn(torch.cat((decoder_hidden, encoder_outputs), dim=-1)))

        attention = self.v(energy).squeeze(-1)
        attention = attention.masked_fill(src_tensor == 0, -inf)

        scores = F.softmax(attention, dim=1)

        return scores


class DecoderRNN(nn.Module):
    def __init__(self, output_size, emb_size, enc_hidden_size, dec_hidden_size,
                 dropout, is_bid, attention, is_ptr, rnn_type, with_critic=False):
        super(DecoderRNN, self).__init__()
        self.is_ptr = is_ptr
        self.attention = attention
        self.rnn_type = rnn_type
        self.with_critic = with_critic
        self.dec_hidden_size = dec_hidden_size

        self.embedding = nn.Embedding(output_size, emb_size)
        self.dropout = nn.Dropout(dropout)

        if is_bid and attention is not None:
            self.combined_size = (enc_hidden_size * 2) + dec_hidden_size + emb_size
        elif is_bid and attention is None:
            self.combined_size = dec_hidden_size + emb_size
        if not is_bid and attention is not None:
            self.combined_size = (enc_hidden_size * 1) + dec_hidden_size + emb_size
        elif not is_bid and attention is None:
            self.combined_size = dec_hidden_size + emb_size

        if is_ptr:
            self.ptr = nn.Linear(self.combined_size, 1)

        if rnn_type == "GRU":
            self.gru = nn.GRU(self.combined_size - dec_hidden_size, dec_hidden_size, batch_first=True)
        elif rnn_type == "LSTM":
            self.gru = nn.LSTM(self.combined_size - dec_hidden_size, dec_hidden_size, batch_first=True)
        self.fc = nn.Linear(self.combined_size, output_size)

        if with_critic:
            self.cri_fc = nn.Linear(self.combined_size, int(0.5 * self.combined_size))
            self.cri_out = nn.Linear(int(0.5 * self.combined_size), 1)

    def init_hidden(self, batch_size, device):
        return torch.zeros(1 * 1, batch_size, self.dec_hidden_size, device=device)

    def forward(self, word_input, decoder_hidden, device,
                encoder_outputs=None, src_tensor=None,
                step=None, valid_mask=None, inf=1e6):
        embedded = self.dropout(self.embedding(word_input))

        rnn_input = embedded
        if self.attention is not None:
            scores = self.attention(src_tensor, encoder_outputs, decoder_hidden, inf).unsqueeze(1)
            context = torch.bmm(scores, encoder_outputs)
            rnn_input = torch.cat((embedded, context), dim=-1)

        if self.rnn_type == "GRU":
            output, hidden = self.gru(rnn_input, decoder_hidden.unsqueeze(0))
        elif self.rnn_type == "LSTM":
            output, (hidden, cell) = self.gru(rnn_input, (decoder_hidden.unsqueeze(0), decoder_hidden.unsqueeze(0)))

        output = output.squeeze(1)
        embedded = embedded.squeeze(1)
        combined = torch.cat((output, embedded), dim=1)
        if self.attention is not None:
            context = context.squeeze(1)
            combined = torch.cat((output, context, embedded), dim=1)

        output = self.fc(combined.unsqueeze(1))
        if valid_mask is not None:
            output = torch.where(valid_mask.unsqueeze(1), torch.tensor(-inf).to(device), output)

        if self.with_critic:
            value = self.cri_out(combined.unsqueeze(1))
            if self.attention is not None:
                return value, output, hidden.squeeze(0), scores
            else:
                return value, output, hidden.squeeze(0)

        # action's probability distribution.
        if self.is_ptr:
            prob_gen = torch.sigmoid(self.ptr(combined))
            prob_ptr = 1 - prob_gen

            if valid_mask is not None:
                step_valid = torch.where(torch.gather(valid_mask, 1, src_tensor[:, step].unsqueeze(-1)), 0,
                                         torch.ones(word_input.shape, device=device, dtype=torch.int64))
                is_not_pad = ((src_tensor[:, step] != constants.PAD).int().unsqueeze(-1)).to(device)

                prob_ptr = prob_ptr * (step_valid * is_not_pad)
                prob_gen = 1 - prob_ptr

            # add generator probabilities to output
            gen_output = F.softmax(output.squeeze(1), dim=1)
            output = prob_gen * gen_output

            # add pointer probabilities (based on attention scores) to output
            # i) all encoder word input
            if valid_mask is None:
                ptr_output = scores.squeeze(1)
                ptr_output = prob_ptr * ptr_output
                output.scatter_add_(1, src_tensor, ptr_output)
                output = output.unsqueeze(1)

            # ii) only corresponding current step encoder word input
            else:
                ptr_output = scores.squeeze(1)
                ptr_output = prob_ptr * ptr_output
                output.scatter_add_(1, src_tensor, ptr_output)
                output = output.unsqueeze(1)

                output = torch.where(valid_mask.unsqueeze(1), torch.tensor(0.).to(device), output)

        if self.attention is not None:
            return output, hidden.squeeze(0), scores
        else:
            return output, hidden.squeeze(0)


class Seq2Seq(nn.Module):
    def __init__(self, args):
        super(Seq2Seq, self).__init__()
        self.args = args

        self.max_len = args.max_len

        attention = None
        self.encoder = EncoderRNN(args.src_vbs, args.emb_size, args.enc_hidden_size,
                                  args.dec_hidden_size, args.is_bid, args.dropout, args.rnn_type)
        if args.is_attn:
            attention = Attention(args.enc_hidden_size, args.dec_hidden_size, args.is_bid)
        self.decoder = DecoderRNN(args.tgt_vbs, args.emb_size, args.enc_hidden_size,
                                  args.dec_hidden_size, args.dropout, args.is_bid,
                                  attention, args.is_ptr, args.rnn_type, with_critic=False)

    def forward(self, src_tensor, tgt_tensor, device,
                word2idx, idx2word, word_info, col_info,
                sql_tokens=None, teacher_forcing_ratio=0.5, max_diff=5):
        batch_size = src_tensor.size(0)
        hidden = self.encoder.init_hidden(batch_size, device)
        encoder_outputs, encoder_hidden = self.encoder(src_tensor, hidden)

        decoder_input = torch.tensor([[constants.BOS]] * batch_size, device=device) 
        decoder_hidden = encoder_hidden  

        if sql_tokens is not None:
            for sql_token in sql_tokens:
                if "table_names" not in sql_token.keys():
                    table_names = [sql_token["pre_tokens"][i] for i, typ in
                                   enumerate(sql_token["pre_types"]) if "table" in typ]
                    sql_token["table_names"] = table_names
                if self.args.pert_mode == "column" \
                        and "pcolumn_vecs" not in sql_token.keys():
                    table_names = [sql_token["pre_tokens"][i] for i, typ in
                                   enumerate(sql_token["pre_types"]) if "table" in typ]
                    sql_token["table_names"] = table_names
                    pcolumn_vecs = [sql_token["pno_tokens"][i] for i, typ in
                                    enumerate(sql_token["pre_types"]) if
                                    typ == "select#column" or
                                    (typ == "select#aggregate_column" and
                                     idx2word[str(sql_token["pno_tokens"][i - 1])] in constants.aggregator[:2]) or
                                    typ == "where#column"]
                    sql_token["pcolumn_vecs"] = pcolumn_vecs

        if sql_tokens is not None and self.args.pert_mode == "column":
            pcolumn_vecs_list = [copy.deepcopy(token["pcolumn_vecs"]) for token in sql_tokens]

        decoder_outputs, decoded_words = list(), list()
        use_teacher_forcing = True if random.random() < teacher_forcing_ratio else False

        for di in range(self.max_len):
            valid_mask = None
            if sql_tokens is not None:
                batch_max_len = max([len(sql_token["pre_types"]) for sql_token in sql_tokens])
                if di >= batch_max_len:
                    break

                valid_padd = (di < torch.tensor([len(item["pre_tokens"]) for item in sql_tokens])).int().unsqueeze(-1)
                valid_padd = torch.cat((valid_padd, torch.zeros(batch_size, self.args.tgt_vbs - 1)), -1)
                word_cand = list()
                for bno, sql_token in enumerate(sql_tokens):
                    ptok_nos = list()
                    if decoded_words:
                        ptok_nos = torch.cat(decoded_words, 1)[bno].cpu().numpy()

                    if self.args.pert_mode == "all":
                        cand = mod_sql.valid_cand(sql_token, sql_token["table_names"], di, ptok_nos,
                                                  word2idx, idx2word, word_info, col_info,
                                                  max_diff=max_diff)
                    elif self.args.pert_mode == "column":
                        cand = mod_sql.valid_cand_col(sql_token, sql_token["table_names"], di,
                                                      ptok_nos, pcolumn_vecs_list[bno],
                                                      word2idx, idx2word, word_info, col_info,
                                                      max_diff=max_diff)
                    elif self.args.pert_mode == "value":
                        cand = mod_sql.valid_cand_val(sql_token, sql_token["table_names"], di, ptok_nos,
                                                      word2idx, idx2word, word_info, col_info,
                                                      max_diff=max_diff)
                    word_cand.append(cand)

                tensor_word_cand = [torch.from_numpy(np.array(cand)) for cand in word_cand]
                # pad all tensors to have same length
                max_len = max([cand.squeeze().numel() for cand in tensor_word_cand])
                index_data = [torch.nn.functional.pad(cand, pad=(0, max_len - cand.numel()),
                                                      mode="constant", value=0) for cand in tensor_word_cand]
                # stack them
                index_data = torch.stack(index_data) 
                valid_mask = torch.add(torch.ones(batch_size, self.args.tgt_vbs).
                                       scatter_(1, index_data, 0),
                                       valid_padd).type(torch.BoolTensor).to(device)

            if self.args.is_attn:
                decoder_output, decoder_hidden, decoder_attention = self.decoder(
                    decoder_input, decoder_hidden, device,
                    encoder_outputs, src_tensor, di,  
                    valid_mask=valid_mask, inf=self.args.inf)
            else:
                decoder_output, decoder_hidden = self.decoder(
                    decoder_input, decoder_hidden, device,
                    valid_mask=valid_mask, inf=self.args.inf)

            if self.args.is_ptr:
                decoder_output = torch.log(decoder_output + self.args.eps)
            else:
                decoder_output = F.log_softmax(decoder_output, dim=-1)

            if use_teacher_forcing:
                decoder_input = tgt_tensor[:, di].unsqueeze(-1)
            else:
                topv, topi = decoder_output.topk(1)
                decoder_input = topi.squeeze(-1).detach()

            decoder_outputs.append(decoder_output)

            if sql_tokens is not None and self.args.pert_mode == "column":
                for bno, (word, token) in enumerate(zip(decoder_input, sql_tokens)):
                    if di >= len(token["pre_types"]):
                        continue
                    if token["pre_types"][di] == "select#column" or \
                            (token["pre_types"][di] == "select#aggregate_column"
                             and idx2word[str(sql_token["pno_tokens"][di - 1])] in constants.aggregator[:2]) \
                            or token["pre_types"][di] == "where#column":

                        dbmind_assert(word in token["pno_tokens"], "Error! New column occurred during perturbation!")
                        
                        if word in pcolumn_vecs_list[bno]:
                            pcolumn_vecs_list[bno].remove(word)

            decoded_words.append(decoder_input)

        return torch.cat(decoder_outputs, 1), torch.cat(decoded_words, 1)


class SingleGRUModel(nn.Module):
    def __init__(self, args):
        super(SingleGRUModel, self).__init__()
        self.args = args
        self.dec_hidden_size = args.dec_hidden_size
        self.max_len = args.max_len

        self.embedding = nn.Embedding(args.tgt_vbs, args.emb_size, padding_idx=constants.PAD)
        self.dropout = nn.Dropout(args.dropout)

        if self.args.rnn_type == "GRU":
            self.gru = nn.GRU(args.emb_size, args.dec_hidden_size, batch_first=True)
        elif self.args.rnn_type == "LSTM":
            self.gru = nn.LSTM(args.emb_size, args.dec_hidden_size, batch_first=True)
        self.fc = nn.Linear(args.dec_hidden_size, args.tgt_vbs)

    def init_hidden(self, batch_size, device):
        return torch.zeros(1 * 1, batch_size, self.dec_hidden_size, device=device)

    def attention(self, output, hidden):
        hidden = hidden.view(output.shape[0], -1, 1)
        
        attn_weights = torch.bmm(output, hidden).squeeze(2)
        soft_attn_weights = F.softmax(attn_weights, dim=1)

        context = torch.bmm(output.transpose(1, 2), soft_attn_weights.unsqueeze(2)).squeeze(2)

        return context, soft_attn_weights.data.numpy()

    def forward(self, src_tensor, device,
                word2idx, idx2word, word_info, col_info,
                sql_tokens=None, max_props=True, max_diff=5):
        batch_size = src_tensor.size(0)
        rnn_input = torch.tensor([[constants.BOS]] * batch_size, device=device)
        rnn_input = self.dropout(self.embedding(rnn_input))

        hidden = self.init_hidden(batch_size, device)
        cell = self.init_hidden(batch_size, device)
        if sql_tokens is not None:
            for sql_token in sql_tokens:
                if "table_names" not in sql_token.keys():
                    table_names = [sql_token["pre_tokens"][i] for i, typ in
                                   enumerate(sql_token["pre_types"]) if "table" in typ]
                    sql_token["table_names"] = table_names
                if self.args.pert_mode == "column" \
                        and "pcolumn_vecs" not in sql_token.keys():
                    table_names = [sql_token["pre_tokens"][i] for i, typ in
                                   enumerate(sql_token["pre_types"]) if "table" in typ]
                    sql_token["table_names"] = table_names
                    pcolumn_vecs = [sql_token["pno_tokens"][i] for i, typ in
                                    enumerate(sql_token["pre_types"]) if
                                    typ == "select#column" or
                                    (typ == "select#aggregate_column" and
                                     idx2word[str(sql_token["pno_tokens"][i - 1])] in constants.aggregator[:2]) or
                                    typ == "where#column"]
                    sql_token["pcolumn_vecs"] = pcolumn_vecs

        if sql_tokens is not None and self.args.pert_mode == "column":
            pcolumn_vecs_list = [copy.deepcopy(token["pcolumn_vecs"]) for token in sql_tokens]

        rnn_outputs = list()
        decoded_outputs, decoded_words = list(), list()
        for di in range(self.max_len):
            valid_mask = None
            if sql_tokens is not None:
                batch_max_len = max([len(sql_token["pre_types"]) for sql_token in sql_tokens])
                if di >= batch_max_len:
                    break

                valid_padd = (di < torch.tensor([len(item["pre_tokens"]) for item in sql_tokens])).int().unsqueeze(-1)
                valid_padd = torch.cat((valid_padd, torch.zeros(batch_size, self.args.tgt_vbs - 1)), -1)
                word_cand = list()
                for bno, sql_token in enumerate(sql_tokens):
                    ptok_nos = list()
                    if decoded_words:
                        ptok_nos = torch.cat(decoded_words, 1)[bno].cpu().numpy()

                    if self.args.pert_mode == "all":
                        cand = mod_sql.valid_cand(sql_token, sql_token["table_names"], di, ptok_nos,
                                                  word2idx, idx2word, word_info, col_info,
                                                  max_diff=self.args.max_diff)
                    elif self.args.pert_mode == "column":
                        cand = mod_sql.valid_cand_col(sql_token, sql_token["table_names"], di,
                                                      ptok_nos, pcolumn_vecs_list[bno],
                                                      word2idx, idx2word, word_info, col_info,
                                                      max_diff=max_diff)
                    elif self.args.pert_mode == "value":
                        cand = mod_sql.valid_cand_val(sql_token, sql_token["table_names"], di, ptok_nos,
                                                      word2idx, idx2word, word_info, col_info,
                                                      max_diff=max_diff)
                    word_cand.append(cand)

                tensor_word_cand = [torch.from_numpy(np.array(cand)) for cand in word_cand]
                # pad all tensors to have same length
                max_len = max([cand.squeeze().numel() for cand in tensor_word_cand])
                index_data = [torch.nn.functional.pad(cand, pad=(0, max_len - cand.numel()),
                                                      mode="constant", value=0) for cand in tensor_word_cand]
                # stack them
                index_data = torch.stack(index_data)
                valid_mask = torch.add(torch.ones(batch_size, self.args.tgt_vbs).
                                       scatter_(1, index_data, 0),
                                       valid_padd).type(torch.BoolTensor).to(device)

            if self.args.rnn_type == "GRU":
                output, hidden = self.gru(rnn_input, hidden)
            elif self.args.rnn_type == "LSTM":
                output, (hidden, cell) = self.gru(rnn_input, (hidden, cell))

            rnn_outputs.append(output)
            if self.args.is_attn:
                output, _ = self.attention(torch.cat(rnn_outputs, 1), hidden)
                output = output.unsqueeze(1)
            output = self.fc(output)

            if valid_mask is not None:
                output = torch.where(valid_mask.unsqueeze(1), torch.tensor(-self.args.inf).to(device), output)

            output = F.log_softmax(output, dim=-1)
            if max_props:
                _, decoder_input = output.squeeze(1).max(-1, keepdim=True)
            else:
                _props = output.squeeze(1).data.clone().exp()
                decoder_input = _props.multinomial(1)

            decoded_outputs.append(output)

            if sql_tokens is not None and self.args.pert_mode == "column":
                for bno, (word, token) in enumerate(zip(decoder_input, sql_tokens)):
                    if di >= len(token["pre_types"]):
                        continue
                    if token["pre_types"][di] == "select#column" or \
                            (token["pre_types"][di] == "select#aggregate_column"
                             and idx2word[str(sql_token["pno_tokens"][di - 1])] in constants.aggregator[:2]) \
                            or token["pre_types"][di] == "where#column":

                        dbmind_assert(word in token["pno_tokens"], "Error! New column occurred during perturbation!")

                        if word in pcolumn_vecs_list[bno]:
                            pcolumn_vecs_list[bno].remove(word)

            decoded_words.append(decoder_input)

        if max_props:
            return torch.cat(decoded_words, 1)
        else:
            return torch.cat(decoded_words, 1), torch.cat(decoded_outputs, 1)


class Actor(nn.Module):
    def __init__(self, args):
        super(Actor, self).__init__()
        self.args = args

        self.max_len = args.max_len

        attention = None
        self.encoder = EncoderRNN(args.src_vbs, args.emb_size, args.enc_hidden_size,
                                  args.dec_hidden_size, args.is_bid, args.dropout, args.rnn_type)
        if args.is_attn:
            attention = Attention(args.enc_hidden_size, args.dec_hidden_size, args.is_bid)
        self.decoder = DecoderRNN(args.tgt_vbs, args.emb_size, args.enc_hidden_size,
                                  args.dec_hidden_size, args.dropout, args.is_bid,
                                  attention, args.is_ptr, args.rnn_type, with_critic=False)

    def forward(self, src_tensor, device,
                word2idx, idx2word, word_info, col_info,
                sql_tokens=None, max_props=True, max_diff=5):
        batch_size = src_tensor.size(0)
        hidden = self.encoder.init_hidden(batch_size, device)
        encoder_outputs, encoder_hidden = self.encoder(src_tensor, hidden)

        decoder_input = torch.tensor([[constants.BOS]] * batch_size, device=device)  # torch.Size([1, 1])
        decoder_hidden = encoder_hidden

        if sql_tokens is not None:
            for sql_token in sql_tokens:
                if "table_names" not in sql_token.keys():
                    table_names = [sql_token["pre_tokens"][i] for i, typ in
                                   enumerate(sql_token["pre_types"]) if "table" in typ]
                    sql_token["table_names"] = table_names
                if self.args.pert_mode == "column" \
                        and "pcolumn_vecs" not in sql_token.keys():
                    table_names = [sql_token["pre_tokens"][i] for i, typ in
                                   enumerate(sql_token["pre_types"]) if "table" in typ]
                    sql_token["table_names"] = table_names
                    pcolumn_vecs = [sql_token["pno_tokens"][i] for i, typ in
                                    enumerate(sql_token["pre_types"]) if
                                    typ == "select#column" or
                                    (typ == "select#aggregate_column" and
                                     idx2word[str(sql_token["pno_tokens"][i - 1])] in constants.aggregator[:2]) or
                                    typ == "where#column"]
                    sql_token["pcolumn_vecs"] = pcolumn_vecs

        if sql_tokens is not None and self.args.pert_mode == "column":
            pcolumn_vecs_list = [copy.deepcopy(token["pcolumn_vecs"]) for token in sql_tokens]

        decoder_outputs, decoded_words = list(), list()
        for di in range(self.max_len):
            valid_mask = None
            if sql_tokens is not None:
                batch_max_len = max([len(sql_token["pre_types"]) for sql_token in sql_tokens])
                if di >= batch_max_len:
                    break
                try:
                    valid_padd = (di < torch.tensor([len(item["pre_tokens"]) for item in
                                                     sql_tokens])).int().unsqueeze(-1)
                    valid_padd = torch.cat((valid_padd, torch.zeros(batch_size, self.args.tgt_vbs - 1)), -1)
                except:
                    print("valid_padd Error!")

                word_cand, ptok_nos = list(), list()
                for bno, sql_token in enumerate(sql_tokens):
                    if decoded_words:
                        ptok_nos = torch.cat(decoded_words, 1)[bno].cpu().numpy()

                    if self.args.pert_mode == "all":
                        cand = mod_sql.valid_cand(sql_token, sql_token["table_names"], di, ptok_nos,
                                                  word2idx, idx2word, word_info, col_info,
                                                  max_diff=max_diff)
                    elif self.args.pert_mode == "column":
                        cand = mod_sql.valid_cand_col(sql_token, sql_token["table_names"], di,
                                                      ptok_nos, pcolumn_vecs_list[bno],
                                                      word2idx, idx2word, word_info, col_info,
                                                      max_diff=max_diff)
                    elif self.args.pert_mode == "value":
                        cand = mod_sql.valid_cand_val(sql_token, sql_token["table_names"], di, ptok_nos,
                                                      word2idx, idx2word, word_info, col_info,
                                                      max_diff=max_diff)

                    word_cand.append(cand)

                tensor_word_cand = [torch.from_numpy(np.array(cand)) for cand in word_cand]

                max_len = max([cand.squeeze().numel() for cand in tensor_word_cand])
                index_data = [torch.nn.functional.pad(cand, pad=(0, max_len - cand.numel()),
                                                      mode="constant", value=0) for cand in tensor_word_cand]

                index_data = torch.stack(index_data)
                valid_mask = torch.add(torch.ones(batch_size, self.args.tgt_vbs).
                                       scatter_(1, index_data, 0),
                                       valid_padd).type(torch.BoolTensor).to(device)

            if self.args.is_attn:
                decoder_output, decoder_hidden, decoder_attention = self.decoder(
                    decoder_input, decoder_hidden, device,
                    encoder_outputs, src_tensor, di, 
                    valid_mask=valid_mask, inf=self.args.inf)
            else:
                decoder_output, decoder_hidden = self.decoder(
                    decoder_input, decoder_hidden, device,
                    valid_mask=valid_mask, inf=self.args.inf)

            if max_props:
                _, decoder_input = decoder_output.squeeze(1).max(-1, keepdim=True)
            else:
                if self.args.is_ptr:
                    _props = decoder_output.squeeze(1).data.clone()
                    decoder_input = _props.multinomial(1)
                    decoder_output = torch.log(decoder_output + self.args.eps)
                    decoder_outputs.append(decoder_output)
                else:
                    decoder_output = F.log_softmax(decoder_output, dim=-1)
                    _props = decoder_output.squeeze(1).data.clone().exp()
                    decoder_input = _props.multinomial(1)
                    decoder_outputs.append(decoder_output)

            if sql_tokens is not None and self.args.pert_mode == "column":
                for bno, (word, token) in enumerate(zip(decoder_input, sql_tokens)):
                    if di >= len(token["pre_types"]):
                        continue
                    if token["pre_types"][di] == "select#column" or \
                            (token["pre_types"][di] == "select#aggregate_column"
                             and idx2word[str(sql_token["pno_tokens"][di - 1])] in constants.aggregator[:2]) \
                            or token["pre_types"][di] == "where#column":

                        dbmind_assert(word in token["pno_tokens"], "Error! New column occurred during perturbation!")

                        if word in pcolumn_vecs_list[bno]:
                            pcolumn_vecs_list[bno].remove(word)

            decoded_words.append(decoder_input)

        if max_props:
            return torch.cat(decoded_words, 1)
        else:
            return torch.cat(decoded_words, 1), torch.cat(decoder_outputs, 1)
