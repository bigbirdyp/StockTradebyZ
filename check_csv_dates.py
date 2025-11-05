#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查CSV文件中date列为空的情况

用法:
    python check_csv_dates.py --data-dir ./data/
    python check_csv_dates.py --data-dir ./data/ --verbose
"""

import argparse
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd


def check_csv_date_column(csv_file: Path) -> Tuple[bool, dict]:
    """
    检查单个CSV文件的date列是否有空值
    
    Args:
        csv_file: CSV文件路径
        
    Returns:
        (是否有问题, 详细信息字典)
        详细信息包括:
        - total_rows: 总行数
        - null_count: 空值数量
        - null_percentage: 空值百分比
        - has_date_column: 是否有date列
    """
    try:
        # 读取CSV文件，尝试解析date列
        df = pd.read_csv(csv_file, parse_dates=["date"])
        
        # 检查是否有date列
        if "date" not in df.columns:
            return True, {
                "has_date_column": False,
                "total_rows": len(df),
                "null_count": None,
                "null_percentage": None,
                "error": "缺少date列"
            }
        
        # 统计空值
        total_rows = len(df)
        null_count = df["date"].isna().sum()
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
        
        has_issue = null_count > 0 or not df["date"].notna().any()
        
        return has_issue, {
            "has_date_column": True,
            "total_rows": total_rows,
            "null_count": int(null_count),
            "null_percentage": round(null_percentage, 2),
            "error": None
        }
        
    except Exception as e:
        return True, {
            "has_date_column": None,
            "total_rows": None,
            "null_count": None,
            "null_percentage": None,
            "error": str(e)
        }


def check_folder(data_dir: Path, verbose: bool = False) -> dict:
    """
    检查文件夹中所有CSV文件的date列
    
    Args:
        data_dir: 数据文件夹路径
        verbose: 是否显示详细信息
        
    Returns:
        统计结果字典
    """
    if not data_dir.exists():
        print(f"错误: 文件夹 {data_dir} 不存在", file=sys.stderr)
        sys.exit(1)
    
    # 获取所有CSV文件
    csv_files = list(data_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"警告: 文件夹 {data_dir} 中没有找到CSV文件")
        return {
            "total_files": 0,
            "problematic_files": [],
            "ok_files": 0
        }
    
    print(f"开始检查 {len(csv_files)} 个CSV文件...\n")
    
    problematic_files: List[dict] = []
    ok_count = 0
    
    # 逐个检查文件
    for csv_file in sorted(csv_files):
        has_issue, info = check_csv_date_column(csv_file)
        
        if has_issue:
            problematic_files.append({
                "file": csv_file.name,
                "path": str(csv_file),
                **info
            })
            
            # 显示问题文件信息
            print(f"❌ {csv_file.name}")
            if info.get("error"):
                print(f"   错误: {info['error']}")
            elif not info.get("has_date_column"):
                print(f"   问题: 缺少date列")
            else:
                print(f"   总行数: {info['total_rows']}")
                print(f"   空值数量: {info['null_count']}")
                print(f"   空值比例: {info['null_percentage']}%")
            print()
        else:
            ok_count += 1
            if verbose:
                print(f"✓ {csv_file.name} - 正常 (总行数: {info['total_rows']})")
    
    # 打印汇总信息
    print("=" * 60)
    print(f"检查完成!")
    print(f"总文件数: {len(csv_files)}")
    print(f"正常文件: {ok_count}")
    print(f"问题文件: {len(problematic_files)}")
    print("=" * 60)
    
    if problematic_files:
        print("\n问题文件列表:")
        for item in problematic_files:
            print(f"  - {item['file']}")
            if item.get("null_count") is not None:
                print(f"    空值: {item['null_count']}/{item['total_rows']} ({item['null_percentage']}%)")
            elif item.get("error"):
                print(f"    错误: {item['error']}")
    
    return {
        "total_files": len(csv_files),
        "problematic_files": problematic_files,
        "ok_files": ok_count
    }


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="检查CSV文件中date列为空的情况",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python check_csv_dates.py --data-dir ./data/
  python check_csv_dates.py --data-dir ./data/ --verbose
        """
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="包含CSV文件的文件夹路径"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="显示所有文件的检查结果（包括正常文件）"
    )
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    check_folder(data_dir, verbose=args.verbose)


if __name__ == "__main__":
    main()

