
import pandas as pd
from . import constants

def assign_data_type(row: pd.Series) -> str:
    """
    Assigns a data_type of AA, PA, PO, or PO_FALLBACK to a single observation.

    PO_FALLBACK is returned for any combination not explicitly handled - these
    are logged as warnings in typed_bla and should be investigated before use
    in modelling.

    Evaluation order:
    1. PO_PROGRAMS - always PO (program design makes counts uninterpretable)
    2. ALWAYS_PO_SURVEY_TYPES - always PO (incidental search, bird list)
    3. Shorebirds program - shorebird-specific logic using all_shorebirds_visible_counted
    4. Swift Parrot Search - single-target; AA if count given, PO otherwise
    5. All remaining programs with systematic survey types - standard logic
    6. PO_FALLBACK - unrecognised survey type, flagged for review

    FORCE_ALL_SPECIES_FALSE: Swift Parrot Search and WA Black Cockies have
    all_species_recorded treated as FALSE regardless of observer input. WA Black
    Cockies reaches step 5 with all_species_recorded=FALSE, so it will only
    reach AA (count given) or PO (no count) - never PA.
    """

    program = row.get("program_name", "")
    survey_type = row.get("survey_type", "")
    count_made = not pd.isna(row.get("individual_count"))
    is_shorebird = row.get("is_shorebird", False)

    # Resolve all_species_recorded - forced FALSE for single-target programs
    if program in constants.BLA_FORCE_ALL_SPECIES_FALSE:
        all_species = False
    else:
        raw_all_species = row.get("all_species_recorded", False)
        all_species = False if pd.isna(raw_all_species) else bool(raw_all_species)

    raw_shorebirds = row.get("all_shorebirds_visible_counted", False)
    all_shorebirds = False if pd.isna(raw_shorebirds) else bool(raw_shorebirds)

    # 1. PO programs 
    if program in constants.BLA_PO_PROGRAMS:
        return "PO"

    # 2. Always-PO survey types 
    if survey_type in constants.BLA_ALWAYS_PO_SURVEY_TYPES:
        return "PO"

    # 3. Shorebirds program 
    # Uses all_shorebirds_visible_counted as an additional completeness flag.
    # Logic differs for shorebird vs non-shorebird species within the same survey.
    if program == "Shorebirds":

        if is_shorebird:
            if count_made:
                return "AA"
            if all_shorebirds or all_species:
                return "PA"
            return "PO"

        else:
            # Non-shorebird species recorded within a Shorebirds survey.
            # all_shorebirds_visible_counted does not imply completeness for
            # non-shorebird species, so only all_species_recorded gates PA here.
            if count_made:
                return "AA"
            if all_species:
                return "PA"
            return "PO"

    # 4. Swift Parrot Search 
    # Single-target program. all_species_recorded forced to FALSE (step above),
    # so PA is unreachable. AA if count given, PO otherwise.
    if program == "Swift Parrot Search":
        if count_made:
            return "AA"
        return "PO"

    # 5. Standard systematic survey logic
    # Applies to all remaining programs with a recognised systematic survey type.
    # Covers: Bittern, Birds in Backyards (incidental caught at step 2),
    # Birds on Farms, General BirdData, Powerful Owl, Sydney Olympic Park,
    # WA Black Cockies, Wetland Birds, and any future programs.
    if survey_type in constants.BLA_SYSTEMATIC_SURVEY_TYPES:
        if count_made:
            return "AA"
        if all_species:
            return "PA"
        return "PO"

    # 6. Fallback
    # Reached when survey_type is not in SYSTEMATIC_SURVEY_TYPES or
    # ALWAYS_PO_SURVEY_TYPES and the program is not otherwise handled above.
    # Indicates an unrecognised survey type - logged as warning in typed_bla.
    return "PO_FALLBACK"



def create_absence_records(
    survey_data: pd.DataFrame,
    survey_id_val: str,
    target_species: list[str],
) -> pd.DataFrame | None:
    """
    Creates absence records for all target species not observed in a given survey.

    Returns a DataFrame of absence records, or None if all target species
    were already observed (or excluded via ambiguous group logic).

    Ambiguous species groups: if any member of a group is observed, all members
    of that group are excluded from absence inference. This prevents spurious
    absences when an observer recorded a group-level sighting (e.g.
    Shy/White-capped Albatross spp) that implicitly covers individual species.

    Absence data_type:
    - PA if any PA records exist in the survey
    - AA otherwise
    Note: this can produce asymmetric cases where a survey has AA presences
    and PA absences (one species counted, another only detected without count).
    Carried forward from the original R code - verify with project lead if
    this asymmetry is acceptable for downstream modelling.

    Absence records have:
    - individual_count = NaN (no count; bird was not observed)
    - is_absence = True
    Original observation records have is_absence = False (set in typed_bla).
    """

    survey_rows = survey_data[survey_data["survey_id"] == survey_id_val]
    if survey_rows.empty:
        return None

    survey_info = survey_rows.iloc[0].copy()
    observed_species = set(survey_rows["common_name"].dropna().tolist())

    # Exclude ambiguous groups where any member was observed
    species_to_exclude: set[str] = set()
    for group in constants.BLA_AMBIGUOUS_SPECIES_GROUPS:
        if observed_species & group:
            species_to_exclude |= group

    missing_species = [
        sp for sp in target_species
        if sp not in observed_species and sp not in species_to_exclude
    ]

    if not missing_species:
        return None

    # Determine absence data_type
    absence_data_type = "PA" if "PA" in survey_rows["data_type"].values else "AA"

    # Build absence records
    records = []
    for sp in missing_species:
        taxa = constants.BLA_SPECIES_DICT.get(sp, {})
        row = survey_info.to_dict()
        row["sighting_id"] = None
        row["taxon_id"] = taxa.get("taxon_id")
        row["common_name"] = sp
        row["scientific_name"] = taxa.get("scientific_name", "")
        row["individual_count"] = None  # NaN - bird not observed, no count possible
        row["is_absence"] = True
        row["data_type"] = absence_data_type
        row["vetting_id"] = None
        row["vetting_status"] = None
        row["is_shorebird"] = sp in constants.BLA_SHOREBIRD_TARGET_SPECIES
        records.append(row)

    return pd.DataFrame(records)