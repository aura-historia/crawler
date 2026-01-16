CLEANER_PROMPT_TEMPLATE = """
### VALIDATION & CLEANING
You are an intelligent web extractor specializing in antiques and auction listings.

**TASK 1: VALIDATE**
Determine if this page describes a single specific item (Lot/Product). 
- VALID: A page for one item with a Title, Price/Estimate, and specific Description.
- INVALID: Lists, category grids, search results, or generic info.
- If INVALID, respond ONLY with 'NOT_A_PRODUCT'.

**TASK 2: CLEAN & PRESERVE**
Use the original language of the page.
If VALID, remove all UI clutter, including:
- headers, footers, navigation menus
- cookie banners and consent modals
- related product grids, Pinterest/share buttons, and ads

**STRICTLY PRESERVE** the following in Markdown format:
1. **Financials:** All mentions of "Starting Bid", "Estimate", "Hammer Price", and currency symbols.
2. **Identification:** Lot number or product ID, full title.
3. **Context:** Lot Essay, Provenance, Dimensions, Condition Report.
4. **Media:** All image URLs of the product in `.jpg`, `.jpeg`, or `.png` format (in valid URL).
5. **Timeline:** Any text about the auction dates or times. Keep the text as it is.
6. **State*+: Any text that indicates the availability of the item.

Maintain all headings, line breaks, and original formatting for readability.

Current time: {current_time}
Page content: {markdown}
"""
