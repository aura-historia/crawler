from aura_historia_backend_api_client.models import PutProductData
from aura_historia_backend_api_client.models.localized_text_data import (
    LocalizedTextData,
)
from aura_historia_backend_api_client.models.price_data import PriceData
from aura_historia_backend_api_client.models.currency_data import CurrencyData
from aura_historia_backend_api_client.models.product_state_data import ProductStateData
from aura_historia_backend_api_client.models.language_data import LanguageData
from src.core.scraper.schemas.extracted_product import ExtractedProduct


def map_extracted_product_to_api(
    extracted: ExtractedProduct,
    url: str,
) -> PutProductData:
    title = None
    if extracted.title:
        title = LocalizedTextData(
            text=extracted.title.text,
            language=LanguageData(extracted.title.language),
        )

    description = None
    if extracted.description:
        description = LocalizedTextData(
            text=extracted.description.text,
            language=LanguageData(extracted.description.language),
        )

    price = None
    if extracted.price:
        price = PriceData(
            amount=extracted.price.amount,
            currency=CurrencyData(extracted.price.currency),
        )

    images = None
    if extracted.images is not None:
        images = [str(i) for i in extracted.images]

    state = ProductStateData(extracted.state) if extracted.state is not None else None

    return PutProductData(
        url=url,
        shops_product_id=extracted.shopsProductId,
        title=title,
        description=description,
        price=price,
        state=state,
        images=images,
        auction_start=extracted.auctionStart,
        auction_end=extracted.auctionEnd,
    )
