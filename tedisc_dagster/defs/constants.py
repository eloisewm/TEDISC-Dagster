
################## GENERAL #####################

ALL_TARGET_SPECIES_COMMON = [
    "Curlew Sandpiper",
    "Red Knot",
    "Far Eastern Curlew",
    "Lesser Sand Plover",
    "Grey-Headed Albatross",
    "Northern Royal Albatross",
    "Orange-bellied Parrot",
    "Shy Albatross",
    "Southern Giant-Petrel",
    "Swift Parrot",
    "White-capped Albatross",
    "Humpback Whale",
    "Southern Right Whale",
    "Blue Whale"
]

ALL_TARGET_SPECIES_SCIENTIFIC = [
    "Calidris ferruginea",        # Curlew Sandpiper
    "Calidris canutus",           # Red Knot
    "Numenius madagascariensis",  # Far Eastern Curlew
    "Anarhynchus mongolus",       # Lesser Sand Plover
    "Thalassarche chrysostoma",   # Grey-headed Albatross
    "Diomedea sanfordi",          # Northern Royal Albatross
    "Neophema chrysogaster",      # Orange-bellied Parrot
    "Thalassarche cauta",         # Shy Albatross
    "Macronectes giganteus",      # Southern Giant-Petrel
    "Lathamus discolor",          # Swift Parrot
    "Thalassarche steadi",        # White-capped Albatross
    "Megaptera novaeangliae",     # Humpback Whale
    "Eubalaena australis",        # Southern Right Whale
    "Balaenoptera musculus",      # Blue Whale
]


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


# Programs that are always PO regardless of survey type or other fields.
# Membership here is based on program DESIGN ambiguity — specifically that
# counts in these programs may not represent total birds present at a site,
# making counts uninterpretable as abundance indices.
#
# Beach-nesting Birds: surveys only record nesting birds, not all birds present.
#   A count of 3 Hooded Plovers means 3 nesting birds, not 3 total birds at
#   the site. This ambiguity is not recoverable from the data fields alone.
#
# Colonial Nesting Birds: surveys may only count nesting/colonial birds and
#   could miss flying or non-nesting individuals. Same count ambiguity as above.
BLA_PO_PROGRAMS = {
    "Beach-nesting Birds",
    "Colonial Nesting Birds",
}

# Survey types that represent genuine standardised effort.
# AA/PA assignment is permitted for records with these survey types
# (subject to program membership and other field conditions).
BLA_SYSTEMATIC_SURVEY_TYPES = {
    "2ha, 20 minute search",
    "2ha, non-20 minute search",
    "500m area search",
    "5km area search",
    "5 minute point search",
    "Shorebird count",
    "Fixed route search",
    "Breeding territory monitoring",
}

# Survey types that are always PO regardless of program or other fields.
BLA_ALWAYS_PO_SURVEY_TYPES = {
    "Incidental search",
    "Bird list",
}

# Programs where all_species_recorded is forced to FALSE regardless of observer input.
# These are single-target programs where general absence inference is not valid,
# regardless of what the observer recorded in the all_species_recorded field.
BLA_FORCE_ALL_SPECIES_FALSE = {
    "Swift Parrot Search",
    "WA Black Cockies",
}

# Shorebird target species — used to set is_shorebird flag and for shorebird-only padding.
# Includes the alternate spelling present in the source data.
BLA_SHOREBIRD_TARGET_SPECIES = {
    "Curlew Sandpiper",
    "Red Knot",
    "Far Eastern Curlew",
    "Lesser Sand Plover",
    "Lesser Sandplover",
}

# All target species for absence padding (all-species condition)
BLA_ALL_TARGET_SPECIES = [
    "Curlew Sandpiper",
    "Red Knot",
    "Far Eastern Curlew",
    "Lesser Sand Plover",
    "Grey-Headed Albatross",
    "Northern Royal Albatross",
    "Orange-bellied Parrot",
    "Shy Albatross",
    "Shy/White-capped Albatross spp",
    "Southern Giant-Petrel",
    "Swift Parrot",
    "White-capped Albatross",
]

# Shorebird target species for shorebird-only absence padding (condition 2)
BLA_SHOREBIRD_SPECIES_FOR_PADDING = [
    "Curlew Sandpiper",
    "Red Knot",
    "Far Eastern Curlew",
    "Lesser Sand Plover",
]

# Ambiguous species groups — if any member is observed in a survey, all members
# are excluded from absence inference to avoid spurious absences.
BLA_AMBIGUOUS_SPECIES_GROUPS = [
    {"Shy Albatross", "White-capped Albatross", "Shy/White-capped Albatross spp"},
]

# Species taxonomy dictionary for constructing absence records
BLA_SPECIES_DICT = {
    "Curlew Sandpiper":               {"scientific_name": "Calidris ferruginea",      "taxon_id": "u161"},
    "Red Knot":                       {"scientific_name": "Calidris canutus",          "taxon_id": "164"},
    "Far Eastern Curlew":             {"scientific_name": "Numenius madagascariensis", "taxon_id": "u149"},
    "Lesser Sand Plover":             {"scientific_name": "Charadrius mongolus",       "taxon_id": "u90"},
    "Grey-Headed Albatross":          {"scientific_name": "Thalassarche chrysostoma",  "taxon_id": "139"},
    "Northern Royal Albatross":       {"scientific_name": "Diomedea sanfordi",         "taxon_id": "u973"},
    "Orange-bellied Parrot":          {"scientific_name": "Neophema chrysogaster",     "taxon_id": "u305"},
    "Shy Albatross":                  {"scientific_name": "Thalassarche cauta",        "taxon_id": "u91"},
    "Shy/White-capped Albatross spp": {"scientific_name": "",                          "taxon_id": "5072"},
    "Southern Giant-Petrel":          {"scientific_name": "Macronectes giganteus",     "taxon_id": "u929"},
    "Swift Parrot":                   {"scientific_name": "Lathamus discolor",         "taxon_id": "u309"},
    "White-capped Albatross":         {"scientific_name": "Thalassarche steadi",       "taxon_id": "u861"},
}

