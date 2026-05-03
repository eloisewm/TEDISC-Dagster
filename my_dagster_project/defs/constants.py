
############ BIRDLIFE AUSTRALIA ################

# Expected column headers
EXPECTED_BLA_COLUMNS = [
    "survey_id", "program_id", "program_name", "survey_point_id",
    "lat", "lon", "accuracy_in_metres", "start_date", "start_time",
    "duration_in_minutes", "observer_count", "survey_type_id", "survey_type",
    "source_id", "source_code", "all_species_recorded", "water_level_cat",
    "is_private", "sighting_id", "taxon_id", "common_name", "scientific_name",
    "individual_count", "vetting_id", "vetting_status",
    "all_shorebirds_visible_counted", "complete_count", "tide_category",
    "wind_speed_category", "wind_dir_category", "disturbance_people_count",
    "disturbance_dogs_off_lead_count", "disturbance_dogs_on_lead_count",
    "disturbance_watercraft_at_anchor_count", "disturbance_watercraft_moving_count",
    "disturbance_vehicles_count", "disturbance_other_count"
]

# Valid program names for the column "program_name"
BLA_PROGRAM_NAMES = [
    "Shorebirds",
    "General Birdata",
    "Birds in Backyards",
    "Beach-nesting Birds",
    "Colonial Nesting Birds",
    "WA Black Cockies",
    "Sydney Olympic Park",
    "Wetland Birds",
    "Swift Parrot Search",
    "Birds on Farms",
    "Powerful Owl",
    "Orange-bellied Parrot",
    "Bittern",
    "Beach-washed Birds"
]

# Valid survey types for column "survey_type"
BLA_SURVEY_TYPES = [
    "Shorebird count",
    "Incidental search",
    "5km area search",
    "500m area search",
    "Fixed route search",
    "2ha, non-20 minute search",
    "2ha, 20 minute search",
    "Bird list",
    "Colony survey",
    "5 minute point search",
    "Breeding territory monitoring"
]

# Expected common names 
BLA_EXPECTED_COMMON_NAMES = [
    "Amsterdam Albatross",
    "Curlew Sandpiper",
    "Lesser Sand Plover",
    "Northern Royal Albatross",
    "Swift Parrot",
    "Southern Giant-Petrel",
    "Orange-bellied Parrot",
    "Far Eastern Curlew",
    "Shy Albatross",
    "Grey-headed Albatross",
    "White-capped Albatross",
    "Shy/White-capped Albatross spp",
    "Red Knot"

]

# Expected scientific names
BLA_EXPECTED_SCIENTIFIC_NAMES = [
    "Calidris canutus",
    "Calidris ferruginea",
    "Charadrius mongolus",
    "Diomedea sanfordi",
    "Lathamus discolor",
    "Macronectes giganteus",
    "Neophema chrysogaster",
    "Numenius madagascariensis",
    "Thalassarche cauta",
    "Thalassarche chrysostoma",
    "Thalassarche steadi",
]