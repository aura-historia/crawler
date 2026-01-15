EXTRACTION_PROMPT_TEMPLATE = """
### TASK

### PRODUCT VERIFICATION
Only proceed if there is EXACTLY ONE main product.
- A real product page must have a price with a currency symbol (e.g. €, EUR).
- A real product page has a detailed description and multiple images of the same product.
- A real product page is not a category page.
- A real product page is not a search results page.
- A real product page is not a list of products.

### EXTRACTION
- shop_item_id (string): The product ID, SKU, Lot-Nr, or article number (e.g., "Art.Nr", "SKU", "ID", "Lot 970")
- title (string | required): The product title or name
- current_price (number, optional): The current selling price. If the product is an auction product, use the starting bid as the current price unless there are existing bids, in which case use the highest bid.
- currency (string, optional): The currency code (e.g., "EUR", "USD")
- description (string | required): The longest and most detailed product description found
    - Extract ONLY the technical description of the primary item.
    - CRITICAL: Escape all double quotes with a backslash (e.g. \\") or replace them with single quotes to ensure valid JSON.
- state (string | UNKNOWN): Only exactly one of these: AVAILABLE, SOLD, LISTED, RESERVED, REMOVED or UNKNOWN.
- images (array of strings, optional): Product images (must end with .jpeg .png) only if present in the markdown. (Only one url per image)
- language (string, required): The language code of the product information (e.g., "de", "en")
- auctionStart (string, optional): If the product is sold via auction and a specific start date is provided, return the auction start datetime in UTC ISO8601 (e.g., "2026-02-15T10:00:00Z"). Do NOT set this field if no start date is explicitly mentioned.
- auctionEnd (string, optional): If the product is sold via auction, return the auction end datetime in UTC ISO8601 (e.g., "2026-02-15T14:00:00Z")

### SPECIAL RULES
- If the page contains "Auction ended" or "achieved price", the `state` MUST be `SOLD`.
- If the state is `SOLD`, the `current_price` should be the "achieved price".
- If the state is `SOLD`, the `auctionEnd` time should be extracted from the page if available, NOT calculated from `CURRENT_TIME`.

### RELATIVE TIME CALCULATION
If the page contains a relative time for an ONGOING auction's END (e.g., "ends in 3 days", "Closing: 12 days"), use the provided `CURRENT_TIME` (below) to compute the absolute UTC datetime for `auctionEnd`. Do not calculate `auctionStart`. If auction info is not present, omit these fields.

### OUTPUT
Return ONLY the JSON or {{}}. No words. No markdown.

CURRENT_TIME: {current_time}

CONTENT:
{markdown}
"""
