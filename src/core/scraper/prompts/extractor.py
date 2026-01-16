EXTRACTION_PROMPT_TEMPLATE = """
### CRITICAL INSTRUCTIONS (READ CAREFULLY)

You are performing a **STRICT DATA EXTRACTION** task.

Your job is to convert the provided SCRAPED_TEXT into **ONE SINGLE, FLAT JSON OBJECT** that follows the schema below **exactly**.

### OUTPUT RULES (ABSOLUTE)
* Output **RAW JSON ONLY**
* **NO commentary**
* **NO explanations**
* **NO markdown**
* **NO additional text**
* **NO nested objects**
* Output must be **valid JSON**

If a value is not explicitly present, use **null**.
Do **NOT** guess or invent values.

### SCHEMA ENFORCEMENT (ABSOLUTE, FAIL-CLOSED)

* You MUST use the **EXACT field names** defined in the schema (case-sensitive).
* You MUST NOT rename, alias, shorten, or substitute any field names.
* The output JSON MUST contain **ONLY** the fields defined in the schema.
* Any missing required field MUST still appear with a value or `null`.

#### FORBIDDEN FIELD NAMES (MUST NEVER APPEAR)

The following field names are **explicitly forbidden** and must NOT appear in the output:
* current_price
* currency
* price
* amount
* estimate

If you would produce any forbidden field, you MUST instead FIX the output to comply with the schema.

### ALLOWED TRANSFORMATIONS (EXPLICITLY PERMITTED)
The following actions are **REQUIRED** and **DO NOT count as inference**:

* Converting monetary values to **integer cents**
* Selecting **Starting Bid** when no **Current Bid** exists
* Extracting estimate minimum and maximum from a stated range
* Normalizing currency to ISO 4217 codes (e.g., USD)
* Calculating `auctionEnd` from phrases like “Closing: X days”
* Converting time calculations to UTC ISO8601

### SCHEMA DEFINITION (STRICT)

All fields below are REQUIRED unless explicitly marked optional.

* shop_item_id (string)
  The Lot number exactly as shown (e.g., "970")

* title (string)
  The full lot title EXACTLY as written

* priceAmount (integer | null)
  The resolved price in **CENTS**

* priceCurrency (string | null)
  ISO 4217 currency code (e.g., "USD")

* priceEstimateMinAmount (integer | null)
  Lower estimate in **CENTS**

* priceEstimateMinCurrency (string | null)
  ISO 4217 currency code

* priceEstimateMaxAmount (integer | null)
  Upper estimate in **CENTS**

* priceEstimateMaxCurrency (string | null)
  ISO 4217 currency code

* description (string)
  CONCATENATE **ALL AVAILABLE TEXT VERBATIM** from:

  * Lot Essay
  * Details
  * Provenance
  * Dimensions
  * Condition Report

  Use newline characters (`\n`) between sections.
  DO NOT summarize, rewrite, translate, or omit content.

* state (string)
  EXACTLY ONE of:
  AVAILABLE | SOLD | LISTED | RESERVED | REMOVED | UNKNOWN

* images (array of strings, optional)
  Include ONLY image URLs that end with `.jpg`, `.jpeg`, or `.png`

* language (string)
  Always `"en"`

* auctionStart (string | null)
 UTC ISO8601 timestamp **ONLY if explicitly stated on the page**. 
  Do NOT invent or infer this date from any Timeline text.

* auctionEnd (string | null)
  UTC ISO8601 timestamp

### PRICE RESOLUTION ALGORITHM (MANDATORY)
Determine `priceAmount` using the following algorithm:

IF a **Current Bid** exists in SCRAPED_TEXT:
priceAmount = Current Bid (converted to cents)

ELSE IF a **Starting Bid** exists in SCRAPED_TEXT:
priceAmount = Starting Bid (converted to cents)

ELSE IF a Buy-It-Now or List Price exists:
priceAmount = that value (converted to cents)

ELSE:
priceAmount = null

IF `priceAmount` is not null:
`priceCurrency` MUST be set.

### ESTIMATE EXTRACTION RULES

IF an estimate range exists:
* BOTH min and max amounts MUST be populated
* BOTH currencies MUST be populated

IF no estimate exists:
* ALL estimate fields MUST be null

### CURRENCY RULES (NON-NEGOTIABLE)
* All monetary values MUST be integers representing **CENTS**
* Examples:
  "$7,000" → 700000
  "USD 12,000" → 1200000
  "7,000 – 10,000" → 700000 and 1000000
  
### TIME CALCULATION RULES
If SCRAPED_TEXT contains a phrase like:
* “Closing: X days”

Then calculate:
* auctionEnd = CURRENT_TIME + X days
* Output as UTC ISO8601
If unclear, set `auctionEnd` to null.

### INPUT DATA
CURRENT_TIME: {current_time}
SCRAPED_TEXT:
{clean_text}

### FINAL VALIDATION (MANDATORY)
Before returning JSON, verify:
* All required fields exist
* Field names EXACTLY match the schema
* No forbidden field names appear
* All prices are integers in cents
* Output is valid JSON

Fix any violation before output.

### OUTPUT
Return **RAW JSON ONLY**.

"""
