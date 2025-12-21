from typing import Tuple, List
from pathlib import Path
import torch
import torch.nn.functional as f
from huggingface_hub import hf_hub_download
from transformers import BertTokenizer, AutoConfig, AutoModelForMaskedLM

from src.core.utils.logger import logger


class URLBertClassifier(torch.nn.Module):
    """
    URL classifier using fine-tuned BERT model to determine if a URL is a product page.
    """

    def __init__(
        self,
        model_path: str = None,
        tokenizer_path: str = None,
        config_path: str = None,
    ):
        """
        Initialize the URL classifier.

        Args:
            model_path: Path to the fine-tuned model file (.pth)
            tokenizer_path: Path to the tokenizer directory or vocab file
            config_path: Path to the BERT config file
        """
        super(URLBertClassifier, self).__init__()

        # Constants matching training configuration
        self.PAD_SIZE = 100
        self.VOCAB_SIZE = 5000

        root_dir = Path(__file__).resolve().parent

        if model_path is None:
            model_path = hf_hub_download(
                repo_id="abdefi/product_bert",
                filename="urlbert_products_best.pth",
            )

        if tokenizer_path is None:
            tokenizer_path = root_dir / "bert_tokenizer" / "vocab.txt"
        else:
            tokenizer_path = Path(tokenizer_path)

        if config_path is None:
            config_path = root_dir / "bert_config" / "config.json"
        else:
            config_path = Path(config_path)

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Using device: {self.device}")

        # Load tokenizer
        try:
            self.tokenizer = BertTokenizer(str(tokenizer_path))
            logger.info(f"Tokenizer loaded from {tokenizer_path}")
        except Exception as e:
            logger.error(f"Failed to load tokenizer from {tokenizer_path}: {e}")
            raise

        # Load model with correct architecture
        try:
            # Create config
            config_kwargs = {"vocab_size": self.VOCAB_SIZE, "hidden_dropout_prob": 0.1}
            config = AutoConfig.from_pretrained(str(config_path), **config_kwargs)

            # Create base BERT model
            self.bert = AutoModelForMaskedLM.from_config(config=config)
            self.bert.resize_token_embeddings(self.VOCAB_SIZE)

            # Remove MLM head to match training-time model
            self.bert.cls = torch.nn.Sequential()

            # Create classification layers
            self.dropout = torch.nn.Dropout(p=0.1)
            self.classifier = torch.nn.Linear(self.bert.config.hidden_size, 2)

            # Load trained weights
            state_dict = torch.load(
                str(model_path), map_location="cpu", weights_only=False
            )
            self.load_state_dict(state_dict)

            logger.info(f"Model loaded successfully with vocab_size={self.VOCAB_SIZE}")

        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise

        self.to(self.device)
        self.eval()

    def forward(self, x):
        """
        Forward pass through the model.

        Args:
            x: Tuple of (input_ids, token_type_ids, attention_mask)

        Returns:
            Logits tensor of shape (batch_size, 2)
        """
        input_ids, token_type_ids, attention_mask = x
        outputs = self.bert(
            input_ids,
            attention_mask=attention_mask,
            token_type_ids=token_type_ids,
            output_hidden_states=True,
        )
        hidden = outputs.hidden_states[-1][:, 0, :]
        out = self.dropout(hidden)
        out = self.classifier(out)
        return out

    def _preprocess_url(self, url: str) -> tuple:
        """
        Preprocess a URL for model input, matching training preprocessing.

        Args:
            url: The URL to preprocess

        Returns:
            Tuple of (input_ids, token_type_ids, attention_mask)
        """
        toks = self.tokenizer.tokenize(str(url))
        tokens = ["[CLS]"] + toks + ["[SEP]"]
        ids = self.tokenizer.convert_tokens_to_ids(tokens)

        if len(ids) < self.PAD_SIZE:
            # Mirror training preprocessing: types for actual tokens are 0, pad tokens set to 1
            types = [0] * len(ids)
            masks = [1] * len(ids)
            # Pad
            pad_len = self.PAD_SIZE - len(ids)
            ids = ids + [0] * pad_len
            masks = masks + [0] * pad_len
            types = types + [1] * pad_len
        else:
            ids = ids[: self.PAD_SIZE]
            masks = [1] * self.PAD_SIZE
            types = [0] * self.PAD_SIZE

        return ids, types, masks

    def classify_url(self, url: str) -> Tuple[bool, float]:
        """
        Classify a URL as product or non-product.

        Args:
            url: The URL to classify

        Returns:
            Tuple of (is_product: bool, confidence: float)
        """
        try:
            # Preprocess the URL
            ids, types, masks = self._preprocess_url(url)

            # Convert to tensors
            input_ids = torch.tensor([ids], dtype=torch.long).to(self.device)
            token_type_ids = torch.tensor([types], dtype=torch.long).to(self.device)
            attention_mask = torch.tensor([masks], dtype=torch.long).to(self.device)

            # Get prediction
            with torch.no_grad():
                logits = self.forward([input_ids, token_type_ids, attention_mask])
                probabilities = f.softmax(logits, dim=-1)
                prediction = torch.argmax(probabilities, dim=1).item()
                confidence = probabilities[0][prediction].item()

            is_product = bool(prediction == 1)

            return is_product, confidence

        except Exception as e:
            logger.error(f"Error classifying URL {url}: {e}")
            return False, 0.0

    def classify_urls_batch(
        self, urls: List[str], batch_size: int = 8
    ) -> List[Tuple[bool, float]]:
        """
        Classify multiple URLs in a batch.

        Args:
            urls: List of URLs to classify
            batch_size: Number of URLs to process at once

        Returns:
            List of tuples (is_product: bool, confidence: float)
        """
        try:
            # Preprocess all URLs
            examples = []
            for url in urls:
                ids, types, masks = self._preprocess_url(url)
                examples.append((ids, types, masks))

            results = []

            # Process in batches
            for i in range(0, len(examples), batch_size):
                batch = examples[i : i + batch_size]

                # Convert to tensors
                input_ids = torch.tensor([b[0] for b in batch], dtype=torch.long).to(
                    self.device
                )
                token_type_ids = torch.tensor(
                    [b[1] for b in batch], dtype=torch.long
                ).to(self.device)
                attention_mask = torch.tensor(
                    [b[2] for b in batch], dtype=torch.long
                ).to(self.device)

                # Get predictions
                with torch.no_grad():
                    logits = self.forward([input_ids, token_type_ids, attention_mask])
                    probabilities = f.softmax(logits, dim=-1)
                    predictions = torch.argmax(probabilities, dim=1)
                    confidences = torch.max(probabilities, dim=1).values

                    # Convert to list of tuples
                    for pred, conf in zip(predictions, confidences):
                        is_product = bool(pred.item() == 1)
                        confidence = conf.item()
                        results.append((is_product, confidence))

            return results

        except Exception as e:
            logger.error(f"Error classifying URLs batch: {e}")
            return [(False, 0.0)] * len(urls)


if __name__ == "__main__":
    # Example usage
    classifier = URLBertClassifier()
    test_urls = [
        "https://example.com/product/123",
        "https://example.com/about",
    ]

    # Test single URL classification
    print("Testing single URL classification:")
    for url in test_urls:
        is_product, confidence = classifier.classify_url(url)
        label = "product" if is_product else "non_product"
        print(f"URL: {url}")
        print(f" -> {label} (p={confidence:.4f})\n")

    # Test batch classification
    print("Testing batch classification:")
    results = classifier.classify_urls_batch(test_urls)
    for url, (is_product, confidence) in zip(test_urls, results):
        label = "product" if is_product else "non_product"
        print(f"URL: {url}")
        print(f" -> {label} (p={confidence:.4f})\n")
