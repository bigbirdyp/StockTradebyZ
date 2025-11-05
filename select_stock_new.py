from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List
import datetime

import pandas as pd

from ts_client import get_stock_basic_info


# 缓存股票基本信息
_stock_info_cache = None

def get_stock_info_cache() -> pd.DataFrame:
    """获取缓存的股票信息"""
    global _stock_info_cache
    if _stock_info_cache is None:
        _stock_info_cache = get_stock_basic_info()
    return _stock_info_cache


# ---------- 日志 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # 将日志写入文件
        logging.FileHandler("select_results.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("select")


# ---------- 工具 ----------

def load_data(data_dir: Path, codes: Iterable[str]) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    for code in codes:
        fp = data_dir / f"{code}.csv"
        if not fp.exists():
            logger.warning("%s 不存在，跳过", fp.name)
            continue
        df = pd.read_csv(fp, parse_dates=["date"]).sort_values("date")
        frames[code] = df
    return frames


def load_config(cfg_path: Path) -> List[Dict[str, Any]]:
    if not cfg_path.exists():
        logger.error("配置文件 %s 不存在", cfg_path)
        sys.exit(1)
    with cfg_path.open(encoding="utf-8") as f:
        cfg_raw = json.load(f)

    # 兼容三种结构：单对象、对象数组、或带 selectors 键
    if isinstance(cfg_raw, list):
        cfgs = cfg_raw
    elif isinstance(cfg_raw, dict) and "selectors" in cfg_raw:
        cfgs = cfg_raw["selectors"]
    else:
        cfgs = [cfg_raw]

    if not cfgs:
        logger.error("configs.json 未定义任何 Selector")
        sys.exit(1)

    return cfgs


def instantiate_selector(cfg: Dict[str, Any]):
    """动态加载 Selector 类并实例化"""
    cls_name: str = cfg.get("class")
    if not cls_name:
        raise ValueError("缺少 class 字段")

    try:
        module = importlib.import_module("Selector")
        cls = getattr(module, cls_name)
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(f"无法加载 Selector.{cls_name}: {e}") from e

    params = cfg.get("params", {})
    return cfg.get("alias", cls_name), cls(**params)


def save_selection_results_to_excel(
    picks: List[str], 
    # trade_date: pd.Timestamp, 
    alias: str, 
    output_dir: str = "./excel_results"
) -> Path:
    """
    将选股结果保存到Excel文件，按日期和策略分别保存
    
    Args:
        picks: 选中的股票代码列表
        trade_date: 交易日期
        alias: 策略别名
        output_dir: 输出目录路径
    
    Returns:
        Path: 保存的文件路径
    """
    if not picks:
        logger.warning("没有选股结果需要保存")
        return None
    
    # 创建输出目录
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(exist_ok=True)
    
    # 生成文件名：按日期和策略命名
    date_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    # 处理策略名称，移除特殊字符，确保文件名合法
    safe_alias = "".join(c for c in alias if c.isalnum() or c in (' ', '-', '_')).rstrip()
    safe_alias = safe_alias.replace(' ', '_')
    
    excel_filename = f"{safe_alias}_{date_str}.xlsx"
    excel_file_path = output_dir_path / excel_filename
    
    # 为股票代码添加后缀
    picks_with_suffix = picks
    
    # 获取股票基本信息
    stock_info_df = get_stock_info_cache()
    
    # 创建结果数据
    result_data = {
        'ts_code': picks_with_suffix
    }
    
    # 添加股票名称、地区、行业信息
    names = []
    areas = []
    industries = []
    
    for ts_code in picks_with_suffix:
        if not stock_info_df.empty:
            stock_info = stock_info_df[stock_info_df['ts_code'] == ts_code]
            if not stock_info.empty:
                names.append(stock_info.iloc[0]['name'])
                areas.append(stock_info.iloc[0]['area'])
                industries.append(stock_info.iloc[0]['industry'])
            else:
                names.append('')
                areas.append('')
                industries.append('')
        else:
            names.append('')
            areas.append('')
            industries.append('')
    
    result_data['name'] = names
    result_data['area'] = areas
    result_data['industry'] = industries
    
    result_df = pd.DataFrame(result_data)
    
    # 保存到Excel文件
    try:
        result_df.to_excel(excel_file_path, index=False, engine='openpyxl')
        logger.info("选股结果已保存到Excel文件: %s", excel_file_path)
        return excel_file_path
    except Exception as e:
        logger.error("保存Excel文件失败: %s", e)
        return None
# ---------- 主函数 ----------

def main():
    p = argparse.ArgumentParser(description="Run selectors defined in configs.json")
    p.add_argument("--data-dir", default="./data", help="CSV 行情目录")
    p.add_argument("--config", default="./configs.json", help="Selector 配置文件")
    p.add_argument("--date", help="交易日 YYYY-MM-DD；缺省=数据最新日期")
    p.add_argument("--tickers", default="all", help="'all' 或逗号分隔股票代码列表")
    p.add_argument("--output", default=None, help="选股结果输出CSV文件路径，可选；未指定则自动命名")
    p.add_argument("--excel", action="store_true", help="使用Excel格式保存，按日期和策略分别保存到不同文件")
    p.add_argument("--excel-dir", default="./excel_results", help="Excel文件保存目录，默认./excel_results")
    args = p.parse_args()

    # --- 加载行情 ---
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error("数据目录 %s 不存在", data_dir)
        sys.exit(1)

    codes = (
        [f.stem for f in data_dir.glob("*.csv")]
        if args.tickers.lower() == "all"
        else [c.strip() for c in args.tickers.split(",") if c.strip()]
    )
    if not codes:
        logger.error("股票池为空！")
        sys.exit(1)

    data = load_data(data_dir, codes)
    if not data:
        logger.error("未能加载任何行情数据")
        sys.exit(1)

    trade_date = (
        pd.to_datetime(args.date)
        if args.date
        else max(
            df["date"].dropna().max() 
            for df in data.values() 
            if not df.empty and not df["date"].dropna().empty
        )
    )
    if not args.date:
        logger.info("未指定 --date，使用最近日期 %s", trade_date.date())

    # --- 加载 Selector 配置 ---
    selector_cfgs = load_config(Path(args.config))

    # --- 逐个 Selector 运行 ---
    for cfg in selector_cfgs:
        if cfg.get("activate", True) is False:
            continue
        try:
            alias, selector = instantiate_selector(cfg)
        except Exception as e:
            logger.error("跳过配置 %s：%s", cfg, e)
            continue

        picks = selector.select(trade_date, data)

        # 将结果写入日志，同时输出到控制台
        logger.info("")
        logger.info("============== 选股结果 [%s] ==============", alias)
        logger.info("交易日: %s", trade_date.date())
        logger.info("符合条件股票数: %d", len(picks))
        logger.info("%s", ", ".join(picks) if picks else "无符合条件股票")

                # 将选股结果保存到文件
        if picks and args.excel:
            # 使用Excel格式保存
            save_selection_results_to_excel(picks, alias, args.excel_dir)


if __name__ == "__main__":
    main()
