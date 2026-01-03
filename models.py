"""
Core data models for item management.

This module provides Pydantic models for handling:
- Unified item entities (events/locations)
- Geospatial data (coordinates, addresses, place IDs)
- Temporal data (dates, times, timezones)
- Multi-language support

Key Features:
- Automatic coordinate geocoding and ID generation
- Context-aware validation (events vs locations)
- Integration with Google Maps APIs
- Strict type checking and value constraints

Primary Models:
- Item: Main entity model for events and locations
- Language: Locale support configuration
"""

import logging
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo
from datetime import date, time, datetime

from rapidfuzz import fuzz
from pydantic import (
    BaseModel,
    ValidationError,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
)

from .google_places import GooglePlacesClient
from .item_deduplication_id import create_item_deduplication_id

logger = logging.getLogger(__name__)

google_places_client = GooglePlacesClient()

# Ratio above which item names are considered equal
FUZZY_COMPARISON_RATIO = 80


class MediaType(str, Enum):
    """Enumeration of supported media types."""

    TYPE_INSTAGRAM_PROFILE = "instagram_profile"
    TYPE_INSTAGRAM_POST = "instagram_post"
    TYPE_EVENTBRITE_PROFILE = "eventbrite_profile"
    TYPE_EVENTBRITE_EVENT = "eventbrite_event"
    TYPE_FACEBOOK_PROFILE = "facebook_profile"
    TYPE_FACEBOOK_EVENT = "facebook_event"
    TYPE_REDDIT_POST = "reddit_post"
    TYPE_YOUTUBE_VIDEO = "youtube_video"
    TYPE_YOUTUBE_CHANNEL = "youtube_channel"
    TYPE_SPOTIFY_ARTIST = "spotify_artist"
    TYPE_SPOTIFY_PLAYLIST = "spotify_playlist"
    TYPE_SOUNDCLOUD_ARTIST = "soundcloud_artist"
    TYPE_MEETUP_PROFILE = "meetup_profile"
    TYPE_MEETUP_EVENT = "meetup_event"
    TYPE_RESIDENT_ADVISOR_PROFILE = "resident_advisor_profile"
    TYPE_RESIDENT_ADVISOR_EVENT = "resident_advisor_event"
    TYPE_SHOTGUN_PROFILE = "shotgun_profile"
    TYPE_SHOTGUN_EVENT = "shotgun_event"
    TYPE_TRIPADVISOR_PROFILE = "tripadvisor_profile"
    TYPE_MICHELIN_PROFILE = "michelin_profile"
    TYPE_OPENTABLE_PROFILE = "opentable_profile"
    TYPE_THEFORK_PROFILE = "thefork_profile"


class Image(BaseModel):
    """
    Represents an image associated with an item.

    Attributes:
        url: URL of the image.
        width: Width of the image in pixels.
        height: Height of the image in pixels.
        text: Text content extracted from or associated with the image.
        description: Description of the image content.
    """

    url: HttpUrl = Field(..., max_length=1000, example="https://example.com/image.jpg")
    width: Optional[int] = Field(None, ge=0, le=5000, description="Width in pixels")
    height: Optional[int] = Field(None, ge=0, le=5000, description="Height in pixels")
    text: Optional[str] = Field(
        None, max_length=10000, description="Text content from image"
    )
    description: Optional[str] = Field(
        None, max_length=10000, description="Image description"
    )


class Media(BaseModel):
    """
    Represents a media.

    Attributes:
        type: Type of media, e.g., 'instagram_profile', 'instagram_post', 'eventbrite_profile'
        url: URL of the media.
    """

    type: MediaType
    url: HttpUrl = Field(..., max_length=1000)


class Language(BaseModel):
    """
    Represents a language with standardized coding.

    Attributes:
        code: ISO 639-1 two-letter language code (e.g., 'en', 'de')
    """

    code: str = Field(..., min_length=2, max_length=2, example="en")


class EventDate(BaseModel):
    """
    Represents temporal information for an event occurrence.

    Attributes:
        start_date (date): **Required.** Date portion of event start.
        start_time (time | None): Precise time of event start.
        end_date (date | None): Date portion of event end.
        end_time (time | None): Precise end time of event.
    """

    start_date: date = Field(..., description="Date portion of event start")
    start_time: Optional[time] = Field(None, description="Precise time of event start")
    end_date: Optional[date] = Field(None, description="Date portion of event end")
    end_time: Optional[time] = Field(None, description="Precise end time of event")

    @model_validator(mode="after")
    def validate_event_dates(self):
        """
        Ensure temporal consistency for event duration fields.

        Validation rules:
        - end_date must be ≥ start_date when both present
        - end_time requires end_date
        - start_time requires start_date

        Returns:
            Self: Validated temporal values

        Raises:
            ValueError: For chronologically inconsistent dates
        """
        if self.end_date and self.end_date < self.start_date:
            raise ValueError("End date cannot be before start date")
        if self.end_time and not self.end_date:
            raise ValueError("End time requires end date")
        if self.start_time and not self.start_date:
            raise ValueError("Start time requires start date")
        return self


class Item(BaseModel):
    """
    Represents a unified entity model for both events and locations, containing common
    attributes and relationships with validation and ID generation logic.

    The model combines geospatial data, temporal information, and source system identifiers,
    with automatic coordinate geocoding and ID generation based on location features.

    Attributes:
        item_id (str): **Required.** Universally unique identifier generated using
            Blake2b hashing of location features and event dates. 32-character hex string.
        name (str): **Required.** Primary display name (event title for events,
            location name for locations). 2-100 characters.
        location_name (str): **Required.** Official venue/location designation.
            2-100 characters.
        street (str | None): Street component from the address.
        city (str | None): Locality/municipality name from address parsing.
        state (str | None): State/province/region code from address parsing.
        postal_code (str | None): Local postal code identifier.
        country (str | None): ISO 3166-1 alpha-2 country code (2 letters).
        latitude (float): **Required.** WGS-84 geographic coordinate (-90 to +90).
        longitude (float): **Required.** WGS-84 geographic coordinate (-180 to +180).
        google_place_id (str | None): Unique Google Places API identifier when available.
        description (str | None): Rich-text event details with HTML support.
        is_event (bool): Indicates if the item represents an event entity (default: False).
        event_dates (list[EventDate] | None): Temporal information for event occurrences.
        timezone (str | None): IANA timezone name for temporal context.
        languages (list[Language]): Supported content languages (default empty list).
        media (list[Media]): Associated media links (default empty list).
        source_platform (str): **Required.** Source system identifier (e.g., 'facebook').
        source_id (str): **Required.** Source system's unique entity identifier.
        source_data (dict): **Required.** Original data based on which the item was created.
        source_created_at: (datetime): Timestamp of item publication.
        images (list[Image]): Item images with main image first (default empty list).
    """

    # Common fields for both events and locations
    item_id: str = Field(
        None, min_length=32, max_length=32, example="44a8995dd50b6657a037a7839304535b"
    )
    name: str = Field(
        ..., min_length=2, max_length=100, example="Summer Music Festival"
    )

    # Source-related fields
    source_platform: str = Field(
        ..., min_length=2, example="facebook", description="Source connector identifier"
    )
    source_id: str = Field(
        ...,
        min_length=1,
        example="1234567890",
        description="Unique ID from source platform",
    )
    source_data: dict = Field(
        description="Original data based on which the item was created"
    )
    source_created_at: Optional[datetime] = Field(
        None, description="Timestamp of item publication"
    )

    images: list[Image] = Field(
        default_factory=list, description="Item images with main image first"
    )

    # Location-specific fields
    location_name: str = Field(..., example="Central Park")
    street: Optional[str] = Field(
        None, min_length=5, max_length=100, example="123 Main Street"
    )
    city: Optional[str] = Field(
        None, min_length=2, max_length=20, example="San Francisco"
    )
    state: Optional[str] = Field(None, min_length=2, max_length=20, example="CA")
    postal_code: Optional[str] = Field(
        None, min_length=2, max_length=10, example="94105"
    )
    country: Optional[str] = Field(None, min_length=2, max_length=2, example="US")
    latitude: float = Field(None, ge=-90, le=90, example=40.7128)
    longitude: float = Field(None, ge=-180, le=180, example=-74.0060)
    # The length of the Google Place ID may vary
    # https://developers.google.com/maps/documentation/places/web-service/place-id#id-overview
    google_place_id: str = Field(
        ..., min_length=1, max_length=1000, example="ChId8m_rA"
    )
    category: Optional[str] = Field(
        None, min_length=2, max_length=50, example="Concert"
    )
    description: Optional[str] = Field(
        None, max_length=10000, example="Annual outdoor music celebration"
    )
    merged_descriptions: list[str] = Field(
        [], description="List of descriptions from merged duplicate items"
    )
    languages: list[Language] = Field(default_factory=list, description="Languages")
    media: list[Media] = Field(default_factory=list, description="Associated media")

    # Event-specific fields
    is_event: bool = Field(
        False, description="Indicates if the item represents an event entity"
    )
    event_dates: Optional[list[EventDate]] = Field(
        None, description="Temporal information for event occurrences"
    )

    timezone: Optional[str] = Field(None, example="America/New_York")

    is_mergeable: bool = Field(False, description="Allow merging during deduplication")

    def __eq__(self, other):
        """
        Determine entity equivalence using context-aware comparison logic.

        For events:
        - Primary comparison via item_id match
        - Secondary fuzzy name similarity (WRatio ≥ FUZZY_COMPARISON_RATIO)
        For locations:
        - Strict item_id equivalence only

        Args:
            other: Comparison target object

        Returns:
            bool: True if objects represent the same logical entity
        """
        if isinstance(other, Item):
            if self.is_event:
                # Event: equality checked based on Item ID first and then via fuzzy name comparison
                if self.item_id == other.item_id:
                    # Calculate similarity ratio of item names
                    name_similarity_ratio = fuzz.WRatio(self.name, other.name)
                    # If the ratio equals or is above the threshold, the items are considered equal
                    return name_similarity_ratio >= FUZZY_COMPARISON_RATIO
            else:
                return self.item_id == other.item_id
        return False

    def __hash__(self):
        """
        Generate hash value based on item_id for consistent hashing.

        Returns:
            int: Stable hash value derived from the entity's ID
        """
        return hash(self.item_id)

    @classmethod
    def _set_location_details(cls, data):
        """
        Finds the Place ID for the address and updates the data dictionary.

        Constructs an address from available components, attempts to find the Place ID
        using Google Places API, and updates the data dictionary. Preserves original
        data if geocoding fails or no address components are available.

        Args:
            data (dict): Input data containing potential address components. Expected keys:
                - location_name: Optional specific point of interest
                - street: Optional street address
                - city: Optional city name
                - country: Optional country name

        Returns:
            dict: Updated data with the Place ID results if successful, otherwise original data.

        Notes:
            - Requires at least two address component to attempt geocoding
            - Overwrites address fields with normalized versions from geocoding service
            - Preserves original data if geocoding fails or returns incomplete coordinates
        """
        # Extract address components with empty string fallback
        location_name = data.get("location_name")
        street = data.get("street")
        city = data.get("city")
        country = data.get("country")

        # Validate at least three address component exists
        if not location_name or not any([location_name, street, city]):
            logger.warning(
                "Skipping Place ID retrieval: no address components provided"
            )
            return data

        # Build address string from available components
        address_parts = []
        for component in [location_name, street, city, country]:
            # Skip empty, whitespace-only, and short components
            if component and component.strip() and len(component) >= 2:
                address_parts.append(component.strip())

        if not address_parts:
            logger.warning("Empty address constructed from components: %s", data)
            return data

        address = ", ".join(address_parts)

        try:
            logger.info('Initiating Place ID retrieval for address: "%s"', address)
            place_id = google_places_client.find_place_id(address)

            # Check if the ID was found
            if not place_id:
                logger.warning('No Place ID found for address: "%s"', address)
                return data

            logger.info('Place ID for address "%s": %s', address, place_id)

            # Update the data with the found ID
            update_fields = {
                "google_place_id": place_id,
            }
            data.update(update_fields)
            logger.debug('Successfully update Place ID for address: "%s"', address)

        except (KeyError, TypeError) as e:
            logger.error(
                'Response parsing failed: %s - Address: "%s"',
                str(e),
                address,
                exc_info=True,
            )

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.critical(
                'Unexpected Place ID retrieval error: "%s"; address: "%s"',
                str(e),
                address,
                exc_info=True,
            )

        return data

    @classmethod
    def _set_id(cls, data):
        """
        Generate deterministic item_id using location features and dates.

        Args:
            data: Input data for model instantiation

        Returns:
            dict: Updated data with computed item_id
        """
        data["item_id"] = None
        place_id = data.get("google_place_id")

        if not place_id:
            # No Google Place ID—can't assign Item ID
            return data

        data["item_id"] = create_item_deduplication_id(
            is_event=data.get("is_event", False),
            place_id=place_id,
            name=data.get("name"),
        )

        return data

    @model_validator(mode="before")
    @classmethod
    def set_ids(cls, data):
        """
        Pre-validation hook for coordinate geocoding and ID generation.

        Execution order:
        1. Geocode address to get coordinates and Google Place ID
        2. Generate deterministic item_id

        Args:
            data: Incoming model data

        Returns:
            Any: Processed data with computed fields
        """
        if isinstance(data, dict):
            # Update Google Place ID only if it's not provided
            if not data.get("google_place_id"):
                data = cls._set_location_details(data)
            data = cls._set_id(data)
        return data

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value):
        """
        Validate timezone string against IANA database.

        Args:
            value: Input timezone identifier

        Returns:
            str: Validated timezone name

        Raises:
            ValueError: For invalid/missing IANA timezone identifiers
        """
        try:
            ZoneInfo(value)
        except Exception as e:  # pylint: disable=broad-exception-caught
            raise ValueError("Invalid IANA timezone format") from e
        return value

    @model_validator(mode="before")
    @classmethod
    def validate_and_filter_event_dates(cls, data: dict) -> dict:
        """
        Pre-validate and filter event dates before main validation.
        """
        if isinstance(data, dict) and "event_dates" in data and data["event_dates"]:
            current_date = date.today()
            valid_event_dates = []

            for event_date_data in data["event_dates"]:
                try:
                    # Validate the event date
                    event_date = EventDate.model_validate(event_date_data)

                    # Check if not in past
                    if event_date.end_date and event_date.end_date < current_date:
                        continue
                    elif (
                        not event_date.end_date and event_date.start_date < current_date
                    ):
                        continue

                    valid_event_dates.append(event_date_data)

                except ValidationError:
                    # Skip invalid event dates but continue processing the item
                    continue

            data["event_dates"] = valid_event_dates

        return data

    @model_validator(mode="after")
    def validate_event_consistency(self):
        """
        Ensure event consistency rules.

        Validation rules:
        - If is_event is True, event_dates must not be empty
        - Validate temporal consistency for all event dates

        Returns:
            Self: Validated model instance

        Raises:
            ValueError: For inconsistent event configuration
        """
        if self.is_event and not self.event_dates:
            raise ValueError("Event items must have at least one event date")

        # Validate each event date's temporal consistency
        if self.event_dates:
            for event_date in self.event_dates:
                # The EventDate model's validator will handle temporal validation
                event_date.model_validate(event_date)

        return self
