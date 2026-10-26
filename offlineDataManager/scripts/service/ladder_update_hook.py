"""Refresh the ladder after an upstream commit; never fail market ingestion."""
import sys
from pathlib import Path
from loguru import logger


def refresh_after_source_update(start_date=None,end_date=None):
    sys.path.insert(0,str(Path(__file__).resolve().parents[3]))
    try:
        from limitup_ladder.build import Builder
        # The start date also invalidates descendants after an explicit historical repair.
        # Fingerprints make an unchanged service rerun a no-op.
        result=Builder().run(start_date=start_date)
        logger.info('[limitup_ladder] {}',result)
        return result
    except Exception:
        logger.exception('[limitup_ladder] Derived refresh failed; retaining published generation')
        return {'status':'retained_previous_generation'}
