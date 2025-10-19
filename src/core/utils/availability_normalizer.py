def normalize_availability(value) -> str:
    """
    Normalize an availability value to a standardized uppercase string.

    Parameters:
        value (Any): The raw availability value, which may be None, a string, or another type.

    Returns:
        str: The normalized availability string in uppercase. Returns an empty string if value is None.
    """
    if value is None:
        return ""
    s = str(value)
    # handle URIs like https://schema.org/InStock or schema:InStock
    for sep in ("/", "#", ":"):
        if sep in s:
            s = s.split(sep)[-1]
    return s.strip().upper()


AVAILABILITY_MAP = {
    "INSTOCK": "AVAILABLE",
    "INSTOREONLY": "AVAILABLE",
    "ONLINEONLY": "AVAILABLE",
    "LIMITEDAVAILABILITY": "AVAILABLE",
    "MADETOORDER": "AVAILABLE",
    "BACKORDER": "LISTED",
    "PREORDER": "LISTED",
    "PRESALE": "AVAILABLE",
    "RESERVED": "RESERVED",
    "SOLDOUT": "SOLD",
    "DISCONTINUED": "REMOVED",
    "OUTOFSTOCK": "SOLD",
}


def map_availability_to_state(availability_raw: str) -> str:
    """
    Maps a raw availability value (string or list) to a normalized availability state.

    Parameters:
        availability_raw (str or list): The raw availability value, which can be a string or a list of strings.

    Returns:
        str: The normalized availability state. Returns "AVAILABLE", "LISTED", "RESERVED", "SOLD", "REMOVED", or "UNKNOWN".
    """
    # Support lists or individual values
    if isinstance(availability_raw, list) and availability_raw:
        key = normalize_availability(availability_raw[0])
    else:
        key = normalize_availability(availability_raw)
    return AVAILABILITY_MAP.get(key, "UNKNOWN")
