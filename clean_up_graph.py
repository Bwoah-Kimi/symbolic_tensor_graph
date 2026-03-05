"""
脚本用于重新分配STG LUT中的graph_id并重命名对应的目录
避免多个目录间的graph_id冲突
同时清理无效的LUT条目和目录
"""

import os
import csv
import shutil
import argparse
import fcntl
from datetime import datetime
import tempfile

def get_existing_directories(iteration_generated_dir):
    """
    获取现有的图目录列表
    """
    existing_dirs = {}
    if not os.path.exists(iteration_generated_dir):
        return existing_dirs
    
    for item in os.listdir(iteration_generated_dir):
        full_path = os.path.join(iteration_generated_dir, item)
        if os.path.isdir(full_path):
            if item.startswith('graph_'):
                # 提取graph_id
                if item.endswith('_modified'):
                    try:
                        graph_id = int(item[6:-9])  # 去掉 'graph_' 和 '_modified'
                        if graph_id not in existing_dirs:
                            existing_dirs[graph_id] = {'original': False, 'modified': False}
                        existing_dirs[graph_id]['modified'] = True
                    except ValueError:
                        continue
                else:
                    try:
                        graph_id = int(item[6:])  # 去掉 'graph_'
                        if graph_id not in existing_dirs:
                            existing_dirs[graph_id] = {'original': False, 'modified': False}
                        existing_dirs[graph_id]['original'] = True
                    except ValueError:
                        continue
    
    return existing_dirs

def clean_and_renumber_graph_ids(lut_file_path, start_id=5000, dry_run=False):
    """
    清理并重新分配LUT中的graph_id并重命名对应的目录
    
    清理规则：
    1. 如果ID只在LUT里有（目录中不存在），删除LUT行
    2. 如果目录里没有modified版本，删除LUT行和原始目录
    3. 如果LUT里状态不是completed，删除LUT行和对应目录
    4. 确保保留的都有original和modified两个版本
    
    Args:
        lut_file_path: STG LUT文件路径
        start_id: 新的起始graph_id
        dry_run: 是否只是预览，不实际执行
    """
    if not os.path.exists(lut_file_path):
        print(f"错误: LUT文件不存在: {lut_file_path}")
        return False
    
    # 获取LUT文件所在目录（应该包含iteration_generated目录）
    lut_dir = os.path.dirname(lut_file_path)
    iteration_generated_dir = os.path.join(lut_dir, "iteration_generated")
    
    if not os.path.exists(iteration_generated_dir):
        print(f"警告: iteration_generated目录不存在: {iteration_generated_dir}")
        existing_dirs = {}
    else:
        existing_dirs = get_existing_directories(iteration_generated_dir)
    
    print(f"LUT文件: {lut_file_path}")
    print(f"图目录: {iteration_generated_dir}")
    print(f"起始ID: {start_id}")
    print(f"预览模式: {dry_run}")
    print(f"发现现有图目录: {len(existing_dirs)} 个")
    print("-" * 50)
    
    # 读取现有LUT数据并进行清理
    valid_rows = []
    invalid_rows = []
    dirs_to_delete = []
    lut_graph_ids = set()
    
    try:
        with open(lut_file_path, 'r', newline='', encoding='utf-8') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            reader = csv.reader(f)
            header = next(reader, None)
            
            if not header:
                print("错误: LUT文件为空或格式错误")
                return False
            
            for row_idx, row in enumerate(reader, 2):  # 从第2行开始计数
                if len(row) < 19:  # 确保有status列
                    print(f"警告: 第{row_idx}行数据不完整，跳过: {row}")
                    invalid_rows.append((row_idx, row, "数据不完整"))
                    continue
                
                try:
                    graph_id = int(row[16])
                    status = row[18] if len(row) > 18 else 'unknown'
                    lut_graph_ids.add(graph_id)
                    
                    # 检查清理规则
                    should_keep = True
                    reason = ""
                    
                    # 规则1: 状态不是completed
                    if status != 'completed':
                        should_keep = False
                        reason = f"状态不是completed (当前: {status})"
                        # 标记对应目录删除
                        if graph_id in existing_dirs:
                            dirs_to_delete.append(graph_id)
                    
                    # 规则2: ID只在LUT里有（目录中不存在）
                    elif graph_id not in existing_dirs:
                        should_keep = False
                        reason = "目录不存在"
                    
                    # 规则3: 目录里没有modified版本
                    elif not existing_dirs[graph_id]['modified']:
                        should_keep = False
                        reason = "缺少modified版本"
                        # 标记原始目录删除
                        dirs_to_delete.append(graph_id)
                    
                    # 规则4: 确保有original版本
                    elif not existing_dirs[graph_id]['original']:
                        should_keep = False
                        reason = "缺少original版本"
                        # 标记modified目录删除
                        dirs_to_delete.append(graph_id)
                    
                    if should_keep:
                        valid_rows.append(row)
                    else:
                        invalid_rows.append((row_idx, row, reason))
                        print(f"  删除LUT第{row_idx}行 (graph_id {graph_id}): {reason}")
                        
                except (ValueError, IndexError) as e:
                    print(f"警告: 第{row_idx}行数据格式错误，跳过: {row}, 错误: {e}")
                    invalid_rows.append((row_idx, row, f"格式错误: {e}"))
                    continue
    
    except Exception as e:
        print(f"错误: 读取LUT文件失败: {e}")
        return False
    
    print(f"\n清理统计:")
    print(f"  有效LUT行数: {len(valid_rows)}")
    print(f"  删除LUT行数: {len(invalid_rows)}")
    print(f"  需要删除的目录: {len(set(dirs_to_delete))} 个")
    
    if len(invalid_rows) > 0:
        print(f"\n删除的LUT行详情:")
        for row_idx, row, reason in invalid_rows[:10]:  # 只显示前10个
            graph_id = row[16] if len(row) > 16 else "unknown"
            print(f"  第{row_idx}行 (graph_id {graph_id}): {reason}")
        if len(invalid_rows) > 10:
            print(f"  ... 还有 {len(invalid_rows) - 10} 行被删除")
    
    # 重新分配graph_id
    old_to_new_mapping = {}
    rows_to_update = []
    current_new_id = start_id
    
    for row in valid_rows:
        try:
            old_graph_id = int(row[16])
            
            # 创建新的graph_id映射
            if old_graph_id not in old_to_new_mapping:
                old_to_new_mapping[old_graph_id] = current_new_id
                current_new_id += 1
            
            # 更新行数据
            new_row = row.copy()
            new_row[16] = str(old_to_new_mapping[old_graph_id])
            new_row[17] = datetime.now().isoformat()  # 更新时间戳
            
            rows_to_update.append(new_row)
            
        except (ValueError, IndexError) as e:
            print(f"警告: 处理有效行时出错: {row}, 错误: {e}")
            continue
    
    print(f"\nGraph ID重新分配:")
    print(f"  保留的graph_id数量: {len(old_to_new_mapping)}")
    if len(old_to_new_mapping) > 0:
        print(f"  ID范围: {min(old_to_new_mapping.keys())} -> {min(old_to_new_mapping.values())} 到 {max(old_to_new_mapping.keys())} -> {max(old_to_new_mapping.values())}")
    
    if dry_run:
        print("\n[预览模式] 不执行实际操作")
        
        print("\n将要删除的目录:")
        unique_dirs_to_delete = set(dirs_to_delete)
        for graph_id in sorted(unique_dirs_to_delete):
            if graph_id in existing_dirs:
                if existing_dirs[graph_id]['original']:
                    print(f"  graph_{graph_id}/")
                if existing_dirs[graph_id]['modified']:
                    print(f"  graph_{graph_id}_modified/")
        
        print("\n将要重命名的目录:")
        for old_id in sorted(old_to_new_mapping.keys()):
            new_id = old_to_new_mapping[old_id]
            if old_id != new_id:
                print(f"  graph_{old_id}/ -> graph_{new_id}/")
                print(f"  graph_{old_id}_modified/ -> graph_{new_id}_modified/")
        
        return True
    
    # 确认操作
    response = input(f"\n是否继续执行清理和重新分配操作? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("操作已取消")
        return False
    
    # 删除无效目录
    print("\n删除无效目录...")
    deleted_dirs = []
    failed_deletes = []
    
    if os.path.exists(iteration_generated_dir):
        unique_dirs_to_delete = set(dirs_to_delete)
        for graph_id in unique_dirs_to_delete:
            if graph_id in existing_dirs:
                # 删除原始目录
                if existing_dirs[graph_id]['original']:
                    old_dir = os.path.join(iteration_generated_dir, f"graph_{graph_id}")
                    try:
                        shutil.rmtree(old_dir)
                        deleted_dirs.append(old_dir)
                        print(f"  删除: graph_{graph_id}/")
                    except Exception as e:
                        failed_deletes.append((old_dir, str(e)))
                        print(f"  删除失败: graph_{graph_id}/ - {e}")
                
                # 删除modified目录  
                if existing_dirs[graph_id]['modified']:
                    old_modified_dir = os.path.join(iteration_generated_dir, f"graph_{graph_id}_modified")
                    try:
                        shutil.rmtree(old_modified_dir)
                        deleted_dirs.append(old_modified_dir)
                        print(f"  删除: graph_{graph_id}_modified/")
                    except Exception as e:
                        failed_deletes.append((old_modified_dir, str(e)))
                        print(f"  删除失败: graph_{graph_id}_modified/ - {e}")
    
    # 重命名保留的目录
    print("\n重命名保留的目录...")
    renamed_dirs = []
    failed_renames = []
    
    if os.path.exists(iteration_generated_dir):
        # 使用临时后缀避免冲突
        temp_suffix = f"_temp_{datetime.now().strftime('%H%M%S')}"
        
        for old_id, new_id in old_to_new_mapping.items():
            if old_id == new_id:
                print(f"  跳过: graph_{old_id} (ID未变化)")
                continue
                
            # 重命名原始目录
            old_dir = os.path.join(iteration_generated_dir, f"graph_{old_id}")
            new_dir = os.path.join(iteration_generated_dir, f"graph_{new_id}")
            temp_dir = os.path.join(iteration_generated_dir, f"graph_{old_id}{temp_suffix}")
            
            if os.path.exists(old_dir):
                try:
                    if os.path.exists(new_dir):
                        failed_renames.append((old_dir, new_dir, "目标目录已存在"))
                        print(f"  重命名失败: graph_{old_id} -> graph_{new_id} (目标已存在)")
                    else:
                        shutil.move(old_dir, temp_dir)
                        shutil.move(temp_dir, new_dir)
                        renamed_dirs.append((old_dir, new_dir))
                        print(f"  重命名: graph_{old_id}/ -> graph_{new_id}/")
                except Exception as e:
                    failed_renames.append((old_dir, new_dir, str(e)))
                    print(f"  重命名失败: graph_{old_id} -> graph_{new_id}: {e}")
            
            # 重命名modified目录
            old_modified_dir = os.path.join(iteration_generated_dir, f"graph_{old_id}_modified")
            new_modified_dir = os.path.join(iteration_generated_dir, f"graph_{new_id}_modified")
            temp_modified_dir = os.path.join(iteration_generated_dir, f"graph_{old_id}_modified{temp_suffix}")
            
            if os.path.exists(old_modified_dir):
                try:
                    if os.path.exists(new_modified_dir):
                        failed_renames.append((old_modified_dir, new_modified_dir, "目标目录已存在"))
                        print(f"  重命名失败: graph_{old_id}_modified -> graph_{new_id}_modified (目标已存在)")
                    else:
                        shutil.move(old_modified_dir, temp_modified_dir)
                        shutil.move(temp_modified_dir, new_modified_dir)
                        renamed_dirs.append((old_modified_dir, new_modified_dir))
                        print(f"  重命名: graph_{old_id}_modified/ -> graph_{new_id}_modified/")
                except Exception as e:
                    failed_renames.append((old_modified_dir, new_modified_dir, str(e)))
                    print(f"  重命名失败: graph_{old_id}_modified -> graph_{new_id}_modified: {e}")
    
    # 更新LUT文件
    print(f"\n更新LUT文件...")
    backup_file = f"{lut_file_path}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # 创建备份
        shutil.copy2(lut_file_path, backup_file)
        print(f"已创建备份文件: {backup_file}")
        
        # 写入新的LUT文件
        with open(lut_file_path, 'w', newline='', encoding='utf-8') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows_to_update)
        
        print(f"LUT文件更新成功")
        
    except Exception as e:
        print(f"错误: 更新LUT文件失败: {e}")
        # 尝试恢复备份
        if os.path.exists(backup_file):
            try:
                shutil.copy2(backup_file, lut_file_path)
                print("已恢复备份文件")
            except:
                print("恢复备份文件失败")
        return False
    
    # 输出操作总结
    print(f"\n操作完成:")
    print(f"  删除无效LUT行数: {len(invalid_rows)}")
    print(f"  删除目录数: {len(deleted_dirs)}")
    print(f"  重命名目录数: {len(renamed_dirs)}")
    print(f"  最终保留LUT行数: {len(rows_to_update)}")
    print(f"  备份文件: {backup_file}")
    
    if failed_deletes:
        print(f"\n删除失败的目录:")
        for path, error in failed_deletes:
            print(f"  {path}: {error}")
    
    if failed_renames:
        print(f"\n重命名失败的目录:")
        for old_path, new_path, error in failed_renames:
            print(f"  {old_path} -> {new_path}: {error}")
    
    return True

def main():
    parser = argparse.ArgumentParser(description="清理并重新分配STG LUT中的graph_id并重命名对应目录")
    parser.add_argument("lut_file", help="STG LUT文件路径")
    parser.add_argument("--start-id", type=int, default=5000, help="新的起始graph_id (默认: 5000)")
    parser.add_argument("--dry-run", action="store_true", help="预览模式，不执行实际操作")
    
    args = parser.parse_args()
    
    success = clean_and_renumber_graph_ids(args.lut_file, args.start_id, args.dry_run)
    
    if success:
        print("\n脚本执行完成")
    else:
        print("\n脚本执行失败")
        exit(1)

if __name__ == "__main__":
    main()
