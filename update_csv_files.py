#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
根据 bj_market.csv 中的新旧代码映射关系，更新文件夹中的 CSV 文件名

功能:
- 如果文件有数据（非空），将旧代码文件名重命名为新代码
- 如果文件没有数据（空文件或只有表头），直接删除

用法:
    python update_csv_files.py --data-dir ./data/ --mapping bj_market.csv
    python update_csv_files.py --data-dir ./data/ --mapping bj_market.csv --dry-run  # 预览模式，不实际执行
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple

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
        
        print(f"已加载 {len(mapping)} 个代码映射关系\n")
        return mapping
        
    except Exception as e:
        print(f"错误: 读取映射文件失败: {e}", file=sys.stderr)
        sys.exit(1)


def check_file_has_data(csv_file: Path) -> bool:
    """
    检查 CSV 文件是否有数据（非空）
    
    Args:
        csv_file: CSV 文件路径
        
    Returns:
        True 表示有数据，False 表示无数据或只有表头
    """
    try:
        # 尝试读取文件
        df = pd.read_csv(csv_file)
        
        # 如果 DataFrame 为空或只有列名，认为没有数据
        if len(df) == 0:
            return False
        
        # 检查是否所有列都是空值
        if df.isna().all().all():
            return False
        
        return True
        
    except Exception as e:
        # 如果读取失败，认为文件有问题，返回 False（将会被删除）
        print(f"  警告: 读取文件失败 {csv_file.name}: {e}")
        return False


def update_csv_files(data_dir: Path, mapping: Dict[str, str], dry_run: bool = False):
    """
    更新文件夹中的 CSV 文件名
    
    Args:
        data_dir: 数据文件夹路径
        mapping: 旧代码到新代码的映射字典
        dry_run: 是否为预览模式（不实际执行操作）
    """
    if not data_dir.exists():
        print(f"错误: 文件夹 {data_dir} 不存在", file=sys.stderr)
        sys.exit(1)
    
    if not data_dir.is_dir():
        print(f"错误: {data_dir} 不是文件夹", file=sys.stderr)
        sys.exit(1)
    
    # 获取所有 CSV 文件
    csv_files = list(data_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"警告: 文件夹 {data_dir} 中没有找到 CSV 文件")
        return
    
    print(f"找到 {len(csv_files)} 个 CSV 文件\n")
    
    if dry_run:
        print("=" * 60)
        print("预览模式：以下操作将被执行")
        print("=" * 60 + "\n")
    
    # 统计信息
    renamed_count = 0
    deleted_count = 0
    skipped_count = 0
    
    renamed_files: List[Tuple[str, str]] = []
    deleted_files: List[str] = []
    
    # 处理每个文件
    for csv_file in sorted(csv_files):
        # 获取文件名（不含扩展名），即股票代码
        file_stem = csv_file.stem
        
        # 检查是否在映射表中
        if file_stem not in mapping:
            # 不在映射表中，跳过
            skipped_count += 1
            if dry_run:
                print(f"⏭  {csv_file.name} - 不在映射表中，跳过")
            continue
        
        new_code = mapping[file_stem]
        new_file = csv_file.parent / f"{new_code}.csv"
        
        # 检查文件是否有数据
        has_data = check_file_has_data(csv_file)
        
        if has_data:
            # 有数据，重命名文件
            if new_file.exists() and new_file != csv_file:
                print(f"⚠️  警告: 目标文件已存在 {new_file.name}，跳过 {csv_file.name}")
                skipped_count += 1
                continue
            
            if dry_run:
                print(f"📝 {csv_file.name} -> {new_file.name} (有数据，将重命名)")
            else:
                try:
                    csv_file.rename(new_file)
                    print(f"✓ {csv_file.name} -> {new_file.name}")
                except Exception as e:
                    print(f"✗ 重命名失败 {csv_file.name}: {e}")
                    skipped_count += 1
                    continue
            
            renamed_count += 1
            renamed_files.append((csv_file.name, new_file.name))
            
        else:
            # 没有数据，删除文件
            if dry_run:
                print(f"🗑  {csv_file.name} (无数据，将删除)")
            else:
                try:
                    csv_file.unlink()
                    print(f"✓ 已删除 {csv_file.name} (无数据)")
                except Exception as e:
                    print(f"✗ 删除失败 {csv_file.name}: {e}")
                    skipped_count += 1
                    continue
            
            deleted_count += 1
            deleted_files.append(csv_file.name)
    
    # 打印汇总信息
    print("\n" + "=" * 60)
    if dry_run:
        print("预览结果:")
    else:
        print("处理完成!")
    print("=" * 60)
    print(f"总文件数: {len(csv_files)}")
    print(f"重命名: {renamed_count}")
    print(f"删除: {deleted_count}")
    print(f"跳过: {skipped_count}")
    print("=" * 60)
    
    if renamed_files and (dry_run or len(renamed_files) <= 20):
        print("\n重命名文件列表（前20个）:")
        for old_name, new_name in renamed_files[:20]:
            print(f"  {old_name} -> {new_name}")
        if len(renamed_files) > 20:
            print(f"  ... 还有 {len(renamed_files) - 20} 个文件已重命名")
    
    if deleted_files and (dry_run or len(deleted_files) <= 20):
        print("\n删除文件列表（前20个）:")
        for file_name in deleted_files[:20]:
            print(f"  {file_name}")
        if len(deleted_files) > 20:
            print(f"  ... 还有 {len(deleted_files) - 20} 个文件已删除")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="根据映射文件更新文件夹中的 CSV 文件名",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 预览模式（不实际执行操作）
  python update_csv_files.py --data-dir ./data/ --mapping bj_market.csv --dry-run
  
  # 实际执行
  python update_csv_files.py --data-dir ./data/ --mapping bj_market.csv
        """
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="包含 CSV 文件的文件夹路径"
    )
    parser.add_argument(
        "--mapping",
        type=str,
        required=True,
        help="代码映射文件路径（bj_market.csv）"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="预览模式：显示将要执行的操作，但不实际执行"
    )
    
    args = parser.parse_args()
    
    # 加载映射关系
    mapping = load_code_mapping(Path(args.mapping))
    
    # 更新文件
    update_csv_files(Path(args.data_dir), mapping, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

