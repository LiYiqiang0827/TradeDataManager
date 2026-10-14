"""外部CSV数据接入客户端。

负责将历史全市场每日1分钟CSV转换为Parquet、将旧15分钟CSV写入兼容库，
并维护各自的导入状态。2026-05-07起的1分钟日更改由TDX service负责；
数据读取统一走 ``coreClient.data_provider``。
"""
from __future__ import annotations

from pathlib import Path
import sys
from typing import Optional, Union


from coreClient._paths import manager_root

_MANAGER_ROOT = manager_root(__file__, 1)
_OFFLINE_SCRIPTS = _MANAGER_ROOT / "offlineDataManager" / "scripts"
if str(_OFFLINE_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_OFFLINE_SCRIPTS))

from config.settings import (
    FIFTEEN_MIN_CSV_ROOT, FIFTEEN_MIN_LEGACY_DB_PATH,
    ONE_MIN_CATALOG_PATH, ONE_MIN_CSV_ROOT, ONE_MIN_PARQUET_ROOT,
)
from core.fifteen_min_store import (
    get_fifteen_min_catalog,
    get_fifteen_min_source_files,
    import_fifteen_min_directory,
    scan_fifteen_min_sources,
)
from core.one_min_store import (
    get_one_min_catalog,
    get_one_min_source_files,
    import_one_min_directory,
    import_one_min_csv,
    scan_one_min_sources,
)


class CSVClient:
    """CSV到离线数据集的统一转换客户端。"""

    def __init__(
        self,
        csv_root: Optional[Union[str, Path]] = None,
        one_min_root: Optional[Union[str, Path]] = None,
        catalog_path: Optional[Union[str, Path]] = None,
    ) -> None:
        self.csv_root = Path(csv_root) if csv_root is not None else ONE_MIN_CSV_ROOT
        self.one_min_root = Path(one_min_root) if one_min_root is not None else ONE_MIN_PARQUET_ROOT
        self.catalog_path = Path(catalog_path) if catalog_path is not None else ONE_MIN_CATALOG_PATH

    def scan_oneMinute(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> dict:
        """扫描源目录，刷新新增、变化、缺失和已转换状态。"""
        return scan_one_min_sources(
            self.csv_root,
            root=self.one_min_root,
            catalog_path=self.catalog_path,
            start_date=start_date,
            end_date=end_date,
        )

    def convert_oneMinute_csv(self, csv_path: Union[str, Path], *, overwrite: bool = False) -> dict:
        """转换一个交易日CSV；一般日更应调用 ``update_oneMinute``。"""
        source = Path(csv_path)
        results = import_one_min_directory(
            source.parent,
            root=self.one_min_root,
            catalog_path=self.catalog_path,
            start_date=source.name[:8],
            end_date=source.name[:8],
            overwrite=overwrite,
        )
        if not results:
            return {"action": "unchanged", "source_file": str(source)}
        return results[0]

    def update_oneMinute(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        *,
        overwrite: bool = False,
    ) -> list[dict]:
        """扫描并转换所有待处理日期，是每日service使用的入口。"""
        return import_one_min_directory(
            self.csv_root,
            root=self.one_min_root,
            catalog_path=self.catalog_path,
            start_date=start_date,
            end_date=end_date,
            overwrite=overwrite,
        )

    def get_oneMinute_catalog(self, status: Optional[str] = None):
        return get_one_min_catalog(self.catalog_path, status=status)

    def get_oneMinute_source_files(self):
        """查看移动硬盘上被目录表记录的每一个源CSV，包括同日副本。"""
        return get_one_min_source_files(self.catalog_path)

    def scan_fifteenMinute(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        *,
        csv_root: Optional[Union[str, Path]] = None,
        fifteen_min_root: Optional[Union[str, Path]] = None,
        catalog_path: Optional[Union[str, Path]] = None,
    ) -> dict:
        return scan_fifteen_min_sources(
            Path(csv_root) if csv_root is not None else FIFTEEN_MIN_CSV_ROOT,
            root=Path(fifteen_min_root) if fifteen_min_root is not None else FIFTEEN_MIN_LEGACY_DB_PATH,
            catalog_path=Path(catalog_path) if catalog_path is not None else None,
            start_date=start_date,
            end_date=end_date,
        )

    def convert_fifteenMinute_csv(
        self,
        csv_path: Union[str, Path],
        *,
        fifteen_min_root: Optional[Union[str, Path]] = None,
        catalog_path: Optional[Union[str, Path]] = None,
        overwrite: bool = False,
    ) -> dict:
        source = Path(csv_path)
        results = import_fifteen_min_directory(
            source.parent,
            root=Path(fifteen_min_root) if fifteen_min_root is not None else FIFTEEN_MIN_LEGACY_DB_PATH,
            catalog_path=Path(catalog_path) if catalog_path is not None else None,
            start_date=source.name[:8],
            end_date=source.name[:8],
            overwrite=overwrite,
        )
        if not results:
            return {"action": "unchanged", "source_file": str(source)}
        return results[0]

    def update_fifteenMinute(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        *,
        csv_root: Optional[Union[str, Path]] = None,
        fifteen_min_root: Optional[Union[str, Path]] = None,
        catalog_path: Optional[Union[str, Path]] = None,
        overwrite: bool = False,
    ) -> list[dict]:
        return import_fifteen_min_directory(
            Path(csv_root) if csv_root is not None else FIFTEEN_MIN_CSV_ROOT,
            root=Path(fifteen_min_root) if fifteen_min_root is not None else FIFTEEN_MIN_LEGACY_DB_PATH,
            catalog_path=Path(catalog_path) if catalog_path is not None else None,
            start_date=start_date,
            end_date=end_date,
            overwrite=overwrite,
        )

    def get_fifteenMinute_catalog(
        self,
        status: Optional[str] = None,
        catalog_path: Optional[Union[str, Path]] = None,
    ):
        return get_fifteen_min_catalog(
            Path(catalog_path) if catalog_path is not None else FIFTEEN_MIN_LEGACY_DB_PATH,
            status=status,
        )

    def get_fifteenMinute_source_files(
        self,
        catalog_path: Optional[Union[str, Path]] = None,
    ):
        return get_fifteen_min_source_files(
            Path(catalog_path) if catalog_path is not None else FIFTEEN_MIN_LEGACY_DB_PATH
        )


def update_oneMinute_from_csv(
    csv_root: Optional[Union[str, Path]] = None,
    one_min_root: Optional[Union[str, Path]] = None,
    catalog_path: Optional[Union[str, Path]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    *,
    overwrite: bool = False,
) -> list[dict]:
    """函数式便捷入口。"""
    return CSVClient(csv_root, one_min_root, catalog_path).update_oneMinute(
        start_date=start_date,
        end_date=end_date,
        overwrite=overwrite,
    )


def update_fifteenMinute_from_csv(
    csv_root: Optional[Union[str, Path]] = None,
    fifteen_min_root: Optional[Union[str, Path]] = None,
    catalog_path: Optional[Union[str, Path]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    *,
    overwrite: bool = False,
) -> list[dict]:
    return CSVClient().update_fifteenMinute(
        start_date=start_date,
        end_date=end_date,
        csv_root=csv_root,
        fifteen_min_root=fifteen_min_root,
        catalog_path=catalog_path,
        overwrite=overwrite,
    )
