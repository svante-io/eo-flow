from dagster import Definitions

from eoflow.dag.materialize import materialize_local

defs = Definitions(
    jobs=[materialize_local],
)
