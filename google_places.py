class GooglePlacesClient:
    """
    Local stub for validation.
    The real version would query Google Places, but for now we just return None
    unless the JSON already provides google_place_id.
    """

    def __init__(self, *args, **kwargs):
        pass

    def find_place_id(self, address: str):
        # model expects this method name
        return None
