from dagster import Definitions

from eoflow.dag.materialize import materialize_eager, materialize_local

defs = Definitions(
    jobs=[materialize_local, materialize_eager],
)
