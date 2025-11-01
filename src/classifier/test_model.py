import torch
import torch.nn as nn
from transformers import BertTokenizer, AutoConfig, AutoModelForMaskedLM


class URLBERTForClassification(nn.Module):
    def __init__(self, bert_model, num_labels=2, hidden_size=768):
        super().__init__()
        self.bert = bert_model.bert
        self.dropout = nn.Dropout(0.2)
        self.classifier = nn.Linear(hidden_size, num_labels)
        self.num_labels = num_labels

    def forward(self, input_ids, attention_mask=None, token_type_ids=None, **kwargs):
        outputs = self.bert(
            input_ids=input_ids,
            attention_mask=attention_mask,
            token_type_ids=token_type_ids,
            return_dict=False,
        )
        cls_output = outputs[0][:, 0, :]
        pooled_output = self.dropout(cls_output)
        logits = self.classifier(pooled_output)
        return logits


config = AutoConfig.from_pretrained(
    "./bert_config/", vocab_size=5000, hidden_dropout_prob=0.2
)

# Base Model
bert_model = AutoModelForMaskedLM.from_config(config)
bert_model.resize_token_embeddings(5000)

bert_dict = torch.load("./bert_model/urlBERT.pt", map_location="cpu")
if isinstance(bert_dict, dict) and "model_state_dict" in bert_dict:
    bert_dict = bert_dict["model_state_dict"]
bert_model.load_state_dict(bert_dict, strict=False)

model = URLBERTForClassification(
    bert_model, num_labels=2, hidden_size=config.hidden_size
)

checkpoint_path = "./urlbert-product-finetuned"
state_dict = torch.load(f"{checkpoint_path}/model.pt", map_location="cpu")

if isinstance(state_dict, dict) and "model_state_dict" in state_dict:
    state_dict = state_dict["model_state_dict"]
model.load_state_dict(state_dict, strict=False)
model.eval()

tokenizer = BertTokenizer.from_pretrained(checkpoint_path)

labels = {0: "non-product", 1: "product"}


def predict_url(url, debug=False):
    # Tokenize
    inputs = tokenizer(url, return_tensors="pt", truncation=True, max_length=128)

    # Predict
    with torch.no_grad():
        logits = model(**inputs)
        probabilities = torch.softmax(logits, dim=-1)
        prediction = torch.argmax(logits, dim=-1).item()
        confidence = probabilities.max().item()

    if debug:
        print(f"  Logits: {logits[0].tolist()}")
        print(
            f"  Probabilities: non-product={probabilities[0][0]:.4f}, product={probabilities[0][1]:.4f}"
        )

    return labels[prediction], confidence


if __name__ == "__main__":
    test_urls = [""]

    print("🧪 Testing Fine-tuned URLBERT Model\n")
    for i, url in enumerate(test_urls):
        pred, conf = predict_url(url, debug=(i < 2))  # Debug first 2 URLs
        print(f"URL: {url}")
        print(f"➜ Prediction: {pred} (Confidence: {conf:.2%})\n")
