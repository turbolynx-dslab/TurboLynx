"""Semantic schema catalog for the synthetic VENUE_PROFILE graphlets."""

SECTIONS = [
    ("Basics", "venue_type capacity rating price_level age_limit"),
    ("Hours", "opens_at closes_at closed_days last_entry timezone"),
    ("Amenities", "wifi coat_check lockers charging_points air_conditioning"),
    ("Accessibility", "wheelchair_access step_free_entry accessible_restroom hearing_support service_animals"),
    ("Music", "primary_genre live_music_days sound_system dance_floor noise_level"),
    ("Food & drink", "serves_food cuisine signature_drink vegetarian_options food_last_order"),
    ("Reservations", "reservation_required walk_ins booking_url cancellation_window group_booking"),
    ("Pricing", "average_spend cover_charge minimum_spend currency service_charge"),
    ("Transit", "nearest_station transit_lines walking_minutes taxi_stand bike_parking"),
    ("Parking", "parking_available parking_spaces valet_parking parking_fee ev_charging"),
    ("Events", "event_type event_frequency ticketing average_duration audience_size"),
    ("Atmosphere", "ambience dress_code lighting crowd_age conversation_level"),
    ("Outdoor", "outdoor_seating rooftop smoking_area weather_cover patio_heaters"),
    ("Family", "family_friendly minimum_age stroller_access kids_menu changing_table"),
    ("Safety", "security_staff bag_check cctv emergency_exits first_aid"),
    ("Payments", "accepts_cash accepts_cards contactless split_bills payment_service_charge"),
    ("Contact", "phone email website social_handle booking_contact"),
    ("Location", "district street postal_code latitude longitude"),
    ("Sustainability", "recycling renewable_energy reusable_cups local_sourcing sustainability_cert"),
    ("Pets", "pet_friendly water_bowls pet_area pet_events leash_required"),
    ("Stage", "stage_width stage_depth backstage_rooms green_room loading_access"),
    ("Acoustics", "acoustic_treatment decibel_limit monitor_system microphone_count mixing_console"),
    ("Seating", "seating_style table_count bar_seats booth_count flexible_layout"),
    ("Restrooms", "restroom_count unisex_restroom accessible_stall baby_changing restroom_attendant"),
    ("Connectivity", "public_wifi wifi_speed cellular_reception livestream_support av_network"),
    ("Hospitality", "table_service counter_service host_desk queue_system cloakroom"),
    ("Policies", "reentry_allowed photography outside_food refund_policy lost_and_found"),
    ("Check-in", "checkin_method qr_entry id_required average_wait capacity_updates"),
    ("Promotions", "happy_hour member_discount student_discount loyalty_program promo_channel"),
    ("Neighborhood", "nearby_landmark safety_at_night foot_traffic neighborhood_style nearby_hotels"),
    ("Weather", "indoor_backup rain_policy seasonal_hours climate_control umbrella_storage"),
    ("Art", "art_style rotating_exhibits local_artists gallery_space installation_count"),
    ("Community", "community_events nonprofit_partner volunteer_program local_hiring community_score"),
    ("Corporate", "corporate_events meeting_rooms projector catering invoice_payment"),
    ("Production", "house_lights projection recording technician_available power_capacity"),
    ("Inventory", "chair_inventory table_inventory glassware_capacity storage_rooms kitchen_capacity"),
    ("Languages", "service_languages multilingual_signage translation_support menu_languages staff_languages"),
    ("Compliance", "liquor_license occupancy_permit fire_inspection food_grade inspection_date"),
    ("Technology", "mobile_ordering digital_menu guest_app beacon_support analytics_opt_out"),
    ("History", "established_year former_name founder renovated_year heritage_status"),
]
SECTIONS = [(name, fields.split()) for name, fields in SECTIONS]

_SECTION_EXAMPLES = [line.split("|") for line in (
    "live music club|450|4.6|3|21",
    "18:00|02:00|Mon|01:15|America/Los_Angeles",
    "yes|yes|no|12|yes",
    "yes|yes|yes|no|yes",
    "jazz|Fri Sat|line array|yes|lively",
    "yes|small plates|house highball|yes|23:30",
    "no|yes|venue.example/book|24h|yes",
    "42|15|0|USD|18%",
    "Central|A C|4|yes|yes",
    "yes|36|no|12|4",
    "live set|weekly|online door|120 min|320",
    "intimate|casual|low|25-40|moderate",
    "yes|no|outside|awning|yes",
    "daytime|all ages|yes|no|yes",
    "4|events only|yes|6|yes",
    "yes|yes|yes|yes|none",
    "+1-555-0142|hello@venue.example|venue.example|@venue|events@venue.example",
    "Music District|42 Blue Note Ave|94107|37.7749|-122.4194",
    "yes|60%|yes|yes|silver",
    "patio|yes|patio|monthly|yes",
    "9m|6m|2|yes|rear dock",
    "full|105|digital|24|48-channel",
    "mixed|34|28|10|yes",
    "6|yes|yes|yes|events",
    "yes|500 Mbps|good|yes|dedicated",
    "yes|yes|yes|digital|yes",
    "yes|no flash|no|48h|front desk",
    "QR|yes|evening|8 min|live",
    "17:00-19:00|10%|Tue|yes|newsletter",
    "Central Market|high|busy|arts|7",
    "yes|move indoors|summer extended|yes|yes",
    "contemporary|yes|yes|120 sqm|8",
    "weekly|arts fund|yes|70%|4.8",
    "yes|3|4K|in-house|yes",
    "LED|4K|multitrack|yes|200A",
    "480|60|900|4|350 meals",
    "English Spanish|yes|on request|3|5",
    "active|450|passed|A|2026-06-14",
    "yes|yes|no|yes|yes",
    "1998|The Workshop|M. Rivera|2024|local",
)]


def example_value(key, seed=0, section=None, offset=None):
    """Return a compact, human-readable deterministic demo value."""
    if section is not None and offset is not None:
        return _SECTION_EXAMPLES[section][offset]
    if key.endswith(("_at", "_order")):
        return "23:30"
    if key.endswith(("_year",)):
        return str(2018 + seed % 7)
    if key.endswith(("_count", "_rooms", "_spaces", "_exits", "_inventory")):
        return str(2 + seed % 18)
    if key.endswith(("_capacity", "_size")):
        return str(120 + seed % 380)
    if key.endswith(("_fee", "_charge", "_spend")):
        return str(10 + seed % 35)
    if key.endswith(("_url", "website")):
        return "venue.example"
    if key == "email" or key.endswith("_contact"):
        return "hello@venue.example"
    if key in {"latitude", "longitude"}:
        return "37.7749" if key == "latitude" else "-122.4194"
    if any(token in key for token in (
            "available", "access", "allowed", "accepts", "support", "friendly",
            "required", "parking", "wifi", "restroom", "entry", "program",
            "service", "cctv", "recycling", "ordering", "signage")):
        return "yes" if seed % 3 else "no"
    return key.replace("_", " ")


def validate():
    keys = [key for _, fields in SECTIONS for key in fields]
    assert len(SECTIONS) == 40
    assert all(len(fields) == 5 for _, fields in SECTIONS)
    assert len(keys) == len(set(keys)) == 200
    assert len(_SECTION_EXAMPLES) == 40
    assert all(len(values) == 5 for values in _SECTION_EXAMPLES)


validate()
