EXTRACTION_PROMPT_TEMPLATE = """
### EXTRACTION
- shop_item_id (string): The product ID, SKU, Lot-Nr or article number
- title (string | required): The product title or name
- current_price (number, optional): The current selling price. For auctions: starting bid or highest bid.
- currency (string, optional): Currency code (e.g. "EUR", "USD")
- description (string | required): The longest and most detailed product description
    - Only extract the technical description of the main item.
    - Escape double quotes or replace with single quotes.
- state (string | UNKNOWN): Exactly one of: AVAILABLE, SOLD, LISTED, RESERVED, REMOVED, UNKNOWN.
- images (array of strings, optional): Product images (only URLs ending with .jpeg/.png)
- language (string, required): Language code of the product info (e.g. "de", "en")
- auctionStart (string, optional): Only if explicitly mentioned in the cleaned text, as UTC ISO8601.
- auctionEnd (string, optional): Only if explicitly or relatively mentioned in the cleaned text, as UTC ISO8601.

### SPECIAL RULES
- If the page contains "Auction ended" or "achieved price", state must be SOLD.
- If state is SOLD, current_price is the achieved price.
- If state is SOLD, auctionEnd only extract if explicitly in the text, do not calculate.

### RELATIVE TIME
- For relative auctionEnd (e.g. "ends in 3 days"), calculate the absolute date using CURRENT_TIME.
- Only extract auctionStart if explicitly present.

### OUTPUT
Return only the JSON, no comments, no markdown.

CURRENT_TIME: {current_time}
CLEANED TEXT:
{clean_text}
"""
