from dagster import Definitions

from eoflow.dag.materialise import materialise_local

defs = Definitions(
    jobs=[materialise_local],
)
