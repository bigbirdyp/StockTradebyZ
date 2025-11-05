#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
根据 bj_market.csv 中的新旧代码对应关系，更新 stocklist.csv 中的股票代码

用法:
    python update_stock_codes.py --stocklist stocklist.csv --mapping bj_market.csv --output stocklist_updated.csv
    python update_stock_codes.py --stocklist stocklist.csv --mapping bj_market.csv  # 直接覆盖原文件
"""

import argparse
import sys
from pathlib import Path
from typing import Dict

import pandas as pd


def load_code_mapping(mapping_file: Path) -> Dict[str, str]:
    """
    加载新旧代码映射表
    
    Args:
        mapping_file: 映射文件路径（包含 old 和 new 列）
        
    Returns:
        旧代码到新代码的字典映射
    """
    if not mapping_file.exists():
        print(f"错误: 映射文件 {mapping_file} 不存在", file=sys.stderr)
        sys.exit(1)
    
    try:
        df = pd.read_csv(mapping_file)
        
        # 检查必要的列是否存在
        if "old" not in df.columns or "new" not in df.columns:
            print(f"错误: 映射文件必须包含 'old' 和 'new' 列", file=sys.stderr)
            sys.exit(1)
        
        # 转换为字符串类型，确保格式一致
        df["old"] = df["old"].astype(str)
        df["new"] = df["new"].astype(str)
        
        # 创建映射字典
        mapping = dict(zip(df["old"], df["new"]))
        
        print(f"已加载 {len(mapping)} 个代码映射关系")
        return mapping
        
    except Exception as e:
        print(f"错误: 读取映射文件失败: {e}", file=sys.stderr)
        sys.exit(1)


def update_stocklist(stocklist_file: Path, mapping: Dict[str, str], output_file: Path = None):
    """
    更新 stocklist.csv 中的股票代码
    
    Args:
        stocklist_file: 股票列表文件路径
        mapping: 旧代码到新代码的映射字典
        output_file: 输出文件路径，如果为 None 则覆盖原文件
    """
    if not stocklist_file.exists():
        print(f"错误: 股票列表文件 {stocklist_file} 不存在", file=sys.stderr)
        sys.exit(1)
    
    try:
        # 读取股票列表
        df = pd.read_csv(stocklist_file)
        
        # 检查必要的列是否存在
        if "symbol" not in df.columns or "ts_code" not in df.columns:
            print(f"错误: 股票列表文件必须包含 'symbol' 和 'ts_code' 列", file=sys.stderr)
            sys.exit(1)
        
        # 转换为字符串类型，确保格式一致
        df["symbol"] = df["symbol"].astype(str)
        
        # 统计更新信息
        updated_count = 0
        updated_codes = []
        
        # 遍历每一行，检查是否需要更新
        for idx, row in df.iterrows():
            old_symbol = str(row["symbol"])
            
            # 检查是否在映射表中
            if old_symbol in mapping:
                new_symbol = mapping[old_symbol]
                
                # 更新 symbol 列
                df.at[idx, "symbol"] = new_symbol
                
                # 更新 ts_code 列（保持后缀不变）
                # 例如：872931.BJ -> 920931.BJ
                if "." in str(row["ts_code"]):
                    suffix = str(row["ts_code"]).split(".")[-1]
                    df.at[idx, "ts_code"] = f"{new_symbol}.{suffix}"
                else:
                    df.at[idx, "ts_code"] = new_symbol
                
                updated_count += 1
                updated_codes.append(f"{old_symbol} -> {new_symbol}")
        
        # 确定输出文件路径
        if output_file is None:
            output_file = stocklist_file
            print(f"将覆盖原文件: {stocklist_file}")
        else:
            print(f"将保存到新文件: {output_file}")
        
        # 保存更新后的文件
        df.to_csv(output_file, index=False, encoding="utf-8")
        
        # 打印更新结果
        print("\n" + "=" * 60)
        print(f"更新完成!")
        print(f"总记录数: {len(df)}")
        print(f"更新记录数: {updated_count}")
        print("=" * 60)
        
        if updated_codes:
            print("\n更新的代码列表（前20个）:")
            for code_change in updated_codes[:20]:
                print(f"  {code_change}")
            if len(updated_codes) > 20:
                print(f"  ... 还有 {len(updated_codes) - 20} 个代码已更新")
        
    except Exception as e:
        print(f"错误: 处理股票列表文件失败: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="根据映射文件更新股票列表中的代码",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 更新并保存到新文件
  python update_stock_codes.py --stocklist stocklist.csv --mapping bj_market.csv --output stocklist_updated.csv
  
  # 直接覆盖原文件
  python update_stock_codes.py --stocklist stocklist.csv --mapping bj_market.csv
        """
    )
    parser.add_argument(
        "--stocklist",
        type=str,
        required=True,
        help="股票列表文件路径（stocklist.csv）"
    )
    parser.add_argument(
        "--mapping",
        type=str,
        required=True,
        help="代码映射文件路径（bj_market.csv）"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="输出文件路径（可选，不指定则覆盖原文件）"
    )
    
    args = parser.parse_args()
    
    # 加载映射关系
    mapping = load_code_mapping(Path(args.mapping))
    
    # 更新股票列表
    output_path = Path(args.output) if args.output else None
    update_stocklist(Path(args.stocklist), mapping, output_path)


if __name__ == "__main__":
    main()

