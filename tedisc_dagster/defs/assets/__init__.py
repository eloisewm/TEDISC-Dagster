from dagster import load_assets_from_modules
from . import BirdLife  # add a line like this for each new source you add later
from . import iNaturalist

bla_assets = load_assets_from_modules([BirdLife])
inat_assets = load_assets_from_modules([iNaturalist])