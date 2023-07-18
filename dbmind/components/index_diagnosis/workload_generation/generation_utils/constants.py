PAD = 0
UNK = 1
EOS = 2
BOS = 3
SEP = 4

WORD = {
    PAD: "<pad>",
    UNK: "<unk>",
    BOS: "<s>",
    EOS: "</s>",
    SEP: "<sep>"
}

operator = ["=", "!=", ">", "<", "<=", ">=", "<>"]
operator_vocab = ["=", "!=", ">", "<", "<=", ">="]
aggregator = ["max", "min", "count", "avg", "sum"]
order_by_key = ["DESC", "ASC"]
conjunction = ["AND", "OR"]

null_predicate = ["is null", "is not null"]
in_predicate = ["in", "not in"]
exists_predicate = ["exists", "not exists"]
like_predicate = ["like", "not like"]

join = ["JOIN", "ON"]
punctuation = ["(", ")", ",", " ", ";"]
keyword = ["select", "from", "aggregate", "where", "group by", "having", "order by"]
