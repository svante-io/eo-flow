import dask.array as da

from eoflow.models.granule import GCPS2Granule
from eoflow.models.models import DataSpec, S2IndexDF


class Archive:

    def __init__(self, path: str, cfg: DataSpec, revisits_df: S2IndexDF):
        self.path = path
        self.cfg = cfg
        self.df = revisits_df
        self._get_granules()
        self._create()

    def _get_granules(self):
        """decide which granules to get based on the composition strategy."""
        self.granules: GCPS2Granule = self.df.apply(
            lambda row: GCPS2Granule(url=row["url"]), axis=1
        )

    def _create(self):
        """lazily create the archive for computation from the dask array."""

        self.data = da.concatenate([granule.stack for granule in self.granules], axis=0)

    def fill(self):
        """execute the dask-delayed computation to fill the archive."""
        self.data.compute()

    # def mask -> mask the archive

    # def composite -> composite the archive

    # def chip -> create chips from the archive
