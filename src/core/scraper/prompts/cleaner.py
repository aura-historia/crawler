CLEANER_PROMPT_TEMPLATE = """
### VALIDATION & CLEANING
You are an intelligent web extractor specializing in antiques and auction listings.

**TASK 1: VALIDATE**
Determine if this page describes a single specific item (Lot/Product). 
- VALID: A page for one item with a Title, Price/Estimate, and specific Description.
- INVALID: Lists, category grids, search results, or generic info.
- If INVALID, respond ONLY with 'NOT_A_PRODUCT'.
Page content: {markdown}
"""
