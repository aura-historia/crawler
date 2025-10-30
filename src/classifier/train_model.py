import pandas as pd
import torch
import torch.nn as nn
from datasets import Dataset
from transformers import (
    AutoConfig,
    AutoModelForMaskedLM,
    Trainer,
    TrainingArguments,
    BertTokenizer,
    DataCollatorWithPadding,
)
from sklearn.metrics import accuracy_score
import os

df = pd.read_csv("./training_data/training_data.csv")  # CSV columns: url,label

label_map = {"non-product": 0, "product": 1}
df["label"] = df["label"].map(label_map)

df = df.sample(frac=1, random_state=42).reset_index(drop=True)

train_frac = 0.8
train_size = int(len(df) * train_frac)
train_df = df[:train_size]
val_df = df[train_size:]

train_dataset = Dataset.from_pandas(train_df)
val_dataset = Dataset.from_pandas(val_df)

tokenizer = BertTokenizer(vocab_file="./bert_tokenizer/vocab.txt")


def tokenize(batch):
    return tokenizer(batch["url"], truncation=True, max_length=128)


train_dataset = train_dataset.map(tokenize, batched=True)
val_dataset = val_dataset.map(tokenize, batched=True)

train_dataset.set_format(type="torch", columns=["input_ids", "attention_mask", "label"])
val_dataset.set_format(type="torch", columns=["input_ids", "attention_mask", "label"])

config_kwargs = {"hidden_dropout_prob": 0.2, "vocab_size": 5000}
config = AutoConfig.from_pretrained("./bert_config/", **config_kwargs)

bert_model = AutoModelForMaskedLM.from_config(config)
bert_model.resize_token_embeddings(config_kwargs["vocab_size"])

bert_dict = torch.load("./bert_model/urlBERT.pt", map_location="cpu")
# Handle possible dict wrappers
if isinstance(bert_dict, dict) and "model_state_dict" in bert_dict:
    bert_dict = bert_dict["model_state_dict"]
bert_model.load_state_dict(bert_dict, strict=False)


class URLBERTForClassification(nn.Module):
    def __init__(self, bert_model, num_labels=2):
        super().__init__()
        self.bert = bert_model.bert  # Extract BERT encoder
        self.dropout = nn.Dropout(0.2)
        self.classifier = nn.Linear(config.hidden_size, num_labels)
        self.num_labels = num_labels

    def forward(self, input_ids, attention_mask=None, labels=None):
        outputs = self.bert(
            input_ids=input_ids, attention_mask=attention_mask, return_dict=False
        )
        last_hidden_state = outputs[0]

        cls_output = last_hidden_state[:, 0, :]
        pooled_output = self.dropout(cls_output)
        logits = self.classifier(pooled_output)

        loss = None
        if labels is not None:
            loss_fct = nn.CrossEntropyLoss()
            loss = loss_fct(logits.view(-1, self.num_labels), labels.view(-1))

        if loss is not None:
            return (loss, logits)
        return (logits,)


model = URLBERTForClassification(bert_model, num_labels=2)

for param in model.bert.encoder.layer[:8].parameters():
    param.requires_grad = False


def compute_metrics(pred):
    labels = pred.label_ids
    preds = pred.predictions.argmax(-1)
    acc = accuracy_score(labels, preds)
    return {"accuracy": acc}


training_args = TrainingArguments(
    output_dir="./results",
    num_train_epochs=3,
    per_device_train_batch_size=16,
    per_device_eval_batch_size=16,
    learning_rate=2e-5,
    eval_strategy="steps",
    eval_steps=500,
    save_steps=500,
    logging_steps=100,
    load_best_model_at_end=True,
    metric_for_best_model="accuracy",
    save_total_limit=2,
    seed=42,
    no_cuda=True,
    fp16=False,
)

data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    compute_metrics=compute_metrics,
    data_collator=data_collator,
)

trainer.train()

os.makedirs("./urlbert-product-finetuned", exist_ok=True)
torch.save(model.state_dict(), "./urlbert-product-finetuned/model.pt")
tokenizer.save_pretrained("./urlbert-product-finetuned")

print("✅ Fine-tuning completed and model saved!")
