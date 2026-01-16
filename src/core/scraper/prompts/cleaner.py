CLEANER_PROMPT_TEMPLATE = """
### VALIDATION & CLEANING
You are an intelligent web extractor. Check if the page describes a single product.
- A product page typically includes a title, description, price, images, and other relevant details about a specific item for sale.
- A product page does NOT include lists of products, categories, search results, or general information pages.
- A product page has usually a detailed description of the item, specifications, and often customer reviews.
- A product page are not listing multiple items for sale or providing navigation to other products.
- If yes, clean the text so that only relevant product information remains.
- Respond with 'NOT_A_PRODUCT' if no product page is detected.
- Return only the cleaned text, no further comments.

Current time: {current_time}
Page content: {markdown}
"""
