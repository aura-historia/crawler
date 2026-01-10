import gc
import logging
import os
import re
import sys
import asyncio
import textwrap

import nest_asyncio
import streamlit as st
import json
import torch
from crawl4ai import BrowserConfig, CrawlerRunConfig, CacheMode, AsyncWebCrawler
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig

logger = logging.getLogger(__name__)

os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

# --- Windows asyncio fix ---
if sys.platform.startswith("win"):
    # Use ProactorEventLoopPolicy for subprocess support (required by Playwright)
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
nest_asyncio.apply()

st.set_page_config(page_title="🕸️ Web Data Extractor", page_icon="🕸️", layout="wide")
st.title("🕸️ Universal Web Data Extractor")

with st.container(border=True):
    url = st.text_input(
        "🌐 **Enter a URL**",
        placeholder="https://example.com/product/123",
        key="url_input",
    )
    strategy = st.radio(
        "🔍 Choose extraction strategy:",
        ["AI Schema (CSS)", "Standards Extractor"],
        horizontal=True,
        index=1,
    )
    extract_btn = st.button("🚀 Extract", type="primary", use_container_width=True)

    # --- Model controls (Preload / Clear cache) ---
    st.markdown("---")
    col1, col2 = st.columns([1, 1])
    with col1:
        preload_btn = st.button("🔁 Preload model", key="preload_btn")
    with col2:
        clear_cache_btn = st.button("🧹 Clear model cache", key="clear_cache_btn")


def beautify_json(data):
    try:
        return json.dumps(data, indent=2, ensure_ascii=False)
    except Exception:
        return str(data)


# === Model / Tokenizer cache helpers ===


def _load_model_and_tokenizer(model_name: str = "Qwen/Qwen3-4B"):
    """Lädt das Tokenizer und Model auf das richtige Device.

    Gibt (tokenizer, model, device) zurück.
    """
    global bnb_config
    device = "cuda" if torch.cuda.is_available() else "cpu"

    if device == "cuda":
        bnb_config = BitsAndBytesConfig(
            load_in_8bit=True,
        )

    tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
    # Set pad_token to eos_token if not already set
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        quantization_config=bnb_config,
        device_map="auto",
        trust_remote_code=True,
        low_cpu_mem_usage=True,
    )

    model.eval()
    return tokenizer, model, device


# Try to use Streamlit's cache_resource (best for long-lived heavy resources).
# Fall back to a simple module-level cache if it's not available.
try:

    @st.cache_resource
    def get_model_and_tokenizer(model_name: str):
        torch.cuda.empty_cache()
        gc.collect()

        device = "cuda" if torch.cuda.is_available() else "cpu"

        # 4-Bit ist Pflicht bei 16GB VRAM, um Platz für den KV-Cache zu lassen
        bnb_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.float16,
            bnb_4bit_use_double_quant=True,
        )

        tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
        model = AutoModelForCausalLM.from_pretrained(
            model_name,
            quantization_config=bnb_config,
            device_map="auto",
            trust_remote_code=True,
        )
        return tokenizer, model, device
except Exception:
    # Simple fallback cache
    _MODEL_CACHE = {}

    def get_model_and_tokenizer(model_name: str):
        if model_name not in _MODEL_CACHE:
            _MODEL_CACHE[model_name] = _load_model_and_tokenizer(model_name)
        return _MODEL_CACHE[model_name]


def clear_model_cache():
    """Versucht den Streamlit-Cache zu leeren, sonst den Modul-Fallback-Cache."""
    # Try to clear Streamlit cache_resource if available
    try:
        if hasattr(st, "cache_resource"):
            try:
                st.cache_resource.clear()
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                    torch.cuda.ipc_collect()
                st.info("GPU-Speicher wurde bereinigt.")
            except Exception:
                # ignore if not supported
                pass
    except Exception:
        pass

    # Fallback: clear module-level cache if exists
    if "_MODEL_CACHE" in globals():
        try:
            globals()["_MODEL_CACHE"].clear()
        except Exception:
            pass


# Handle preload / clear cache UI actions
if "preload_btn" in locals() and preload_btn:
    st.info(f"Preloading model '{model_name}' ...")
    with st.spinner("Lade Modell (kann einige Zeit dauern)..."):
        try:
            tokenizer, model, device = get_model_and_tokenizer(model_name)
            st.success(f"Modell geladen auf {device} ({model.__class__.__name__})")
        except Exception as e:
            st.error(f"Fehler beim Laden: {e}")

if "clear_cache_btn" in locals() and clear_cache_btn:
    clear_model_cache()
    st.info("Model cache cleared.")


async def extract_and_display_standards(url: str, model_name: str = "Qwen/Qwen3-4B"):
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        check_robots_txt=True,
        excluded_tags=["nav", "footer", "header"],
    )
    browser_config = BrowserConfig(headless=True, verbose=False)

    # Load the tokenizer and the Qwen3 model (cached using provided model_name)
    tokenizer, model, _ = get_model_and_tokenizer(model_name)

    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=url, config=run_config)
        if result.success:
            markdown = result.markdown[:40000]

            logger.error(markdown)
            logger.error(f"Länge normaler Markdown {len(markdown)}")

            empty_json = "{}"

            prompt = f"""
                    ### TASK
                    Determine if the content is a SINGLE product page or a LIST/BLOG.

                    ### STEP 1: STRUCTURE ANALYSIS (STRICT)
                    Check for these 'Red Flags' of a List or Blog page:
                    - Presence of multiple "WEITERLESEN" or "READ MORE" buttons.
                    - Multiple different Art.Nr. (e.g., 1941, 2025, 3344) in the same text.

                    IF ANY RED FLAGS ARE FOUND:
                    You MUST return ONLY {empty_json}. Do not extract any data.

                    ### STEP 2: PRODUCT VERIFICATION
                    Only proceed if there is EXACTLY ONE main product.
                    - A real product page must have a "Warenkorb" (Cart) button or a clear "In den Warenkorb" text.
                    - A real product page must have a clear Price with a Currency symbol (e.g. €, EUR) that is NOT inside an image link.

                    ### STEP 3: EXTRACTION
                    If Step 1 and 2 pass:
                    - shop_item_id (string): The product ID, SKU, or article number (e.g., "Art.Nr", "SKU", "ID")
                    - title (string): The product title or name
                    - current_price (number, optional): The current selling price as a numeric value
                    - currency (string, optional): The currency code (e.g., "EUR", "USD")
                    - description (string): The longest and most detailed product description found
                        - Extract ONLY the technical description of the primary item.
                        - CRITICAL: Escape all double quotes with a backslash (e.g. \") or replace them with single quotes to ensure valid JSON.
                    - state (string | UNKNOWN): One of: LISTED, AVAILABLE, RESERVED, SOLD, or REMOVED
                    - images (array of strings, optional): Product images (must end with .jpeg .png) ony if present in the markdown
                    - language (string, optional): The language code of the product informations (e.g., "de", "en")


                    ### OUTPUT
                    Return ONLY the JSON or {empty_json}. No words. No markdown.

                    CONTENT:
                    {markdown}
            """

            logger.info("Mardown length: " + str(len(markdown)))

            # Execute the prompt
            messages = [
                {
                    "role": "system",
                    "content": "You are a helpful assistant that extracts product data and returns valid JSON.",
                },
                {"role": "user", "content": prompt},
            ]

            prompt = tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True,
                enable_thinking=False,
            )

            inputs = tokenizer(
                [prompt],
                return_tensors="pt",
                padding=True,
                truncation=True,
            ).to(model.device)

            with torch.inference_mode():
                outputs = model.generate(
                    **inputs,
                    max_new_tokens=4096,
                    do_sample=False,
                    pad_token_id=tokenizer.pad_token_id,
                    eos_token_id=tokenizer.eos_token_id,
                )

            input_length = inputs["input_ids"].shape[1]

            product_raw_string = tokenizer.batch_decode(
                outputs[:, input_length:],
                skip_special_tokens=True,
            )[0]

            logger.info("Raw extracted product string: " + product_raw_string)

            # Check if the string contains "```json" and extract the raw JSON if present
            match = re.search(r"```json\s*(.*?)\s*```", product_raw_string, re.DOTALL)

            if match:
                # Extract the JSON string from the matched group
                json_string = match.group(1).strip()
            else:
                # Try to find JSON object directly
                json_match = re.search(r"{.*}", product_raw_string, re.DOTALL)
                if json_match:
                    json_string = json_match.group(0)
                else:
                    # Assume the returned data is already in JSON format
                    json_string = product_raw_string.strip()

            # Parse the extracted JSON string into a Python dictionary
            try:
                data = json.loads(json_string)
            except json.JSONDecodeError as e:
                logger.error(f"JSON parsing failed: {e}")
                logger.error(f"Attempted to parse: {json_string}")
                raise ValueError(
                    f"Failed to parse JSON response from model. Raw output: {product_raw_string[:200]}..."
                )

    st.subheader("Extracted Product Data")
    if "description" in data and data["description"]:
        wrapped_desc = "\n".join(textwrap.wrap(data["description"], width=80))
        data["description"] = wrapped_desc

    st.code(beautify_json(data), language="json", line_numbers=False)


if extract_btn:
    if not url:
        st.error("❌ Bitte gib eine gültige URL ein.")
    else:
        st.info(f"Extrahiere Daten mit **{strategy}**...")
        with st.spinner("⏳ Daten werden geladen und analysiert..."):
            try:
                loop = asyncio.get_event_loop()
                nest_asyncio.apply()
                loop.run_until_complete(extract_and_display_standards(url, model_name))
                st.success("✅ Extraktion abgeschlossen!")
            except Exception as e:
                st.error(f"⚠️ Extraktion fehlgeschlagen: {e}")
