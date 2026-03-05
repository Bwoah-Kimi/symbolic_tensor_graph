"""
脚本用于删除LUT中配置相同但graph_id不同的重复行
只保留graph_id最小的那一行，并删除对应的重复目录
"""

import os
import csv
import shutil
import argparse
import fcntl
from datetime import datetime
from collections import defaultdict

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

def remove_duplicate_configs(lut_file_path, dry_run=False):
    """
    删除配置相同但graph_id不同的重复行
    只保留graph_id最小的那一行
    
    Args:
        lut_file_path: STG LUT文件路径
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
    print(f"预览模式: {dry_run}")
    print(f"发现现有图目录: {len(existing_dirs)} 个")
    print("-" * 50)
    
    # 读取现有LUT数据并按配置分组
    config_groups = defaultdict(list)  # config_key -> [(row_index, row_data, graph_id), ...]
    all_rows = []
    
    try:
        with open(lut_file_path, 'r', newline='', encoding='utf-8') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            reader = csv.reader(f)
            header = next(reader, None)
            
            if not header:
                print("错误: LUT文件为空或格式错误")
                return False
            
            for row_idx, row in enumerate(reader, 2):  # 从第2行开始计数
                if len(row) < 17:  # 确保有graph_id列
                    print(f"警告: 第{row_idx}行数据不完整，跳过: {row}")
                    continue
                
                try:
                    # 提取配置键（除了graph_id、timestamp、status之外的所有列）
                    config_key = tuple(row[:16])  # 前16列为配置参数
                    graph_id = int(row[16])
                    
                    # 按配置分组
                    config_groups[config_key].append((row_idx, row, graph_id))
                    all_rows.append((row_idx, row, graph_id, config_key))
                    
                except (ValueError, IndexError) as e:
                    print(f"警告: 第{row_idx}行数据格式错误，跳过: {row}, 错误: {e}")
                    continue
    
    except Exception as e:
        print(f"错误: 读取LUT文件失败: {e}")
        return False
    
    # 找出重复的配置
    duplicate_configs = {}
    rows_to_keep = []
    rows_to_delete = []
    dirs_to_delete = []
    
    for config_key, entries in config_groups.items():
        if len(entries) > 1:
            # 有重复配置，按graph_id排序，保留最小的
            entries.sort(key=lambda x: x[2])  # 按graph_id排序
            keep_entry = entries[0]  # 保留graph_id最小的
            delete_entries = entries[1:]  # 删除其他的
            
            rows_to_keep.append(keep_entry)
            rows_to_delete.extend(delete_entries)
            
            # 记录要删除的目录
            for _, _, graph_id in delete_entries:
                dirs_to_delete.append(graph_id)
            
            duplicate_configs[config_key] = {
                'keep': keep_entry[2],  # 保留的graph_id
                'delete': [entry[2] for entry in delete_entries]  # 要删除的graph_id列表
            }
            
            print(f"发现重复配置:")
            print(f"  配置: {config_key[:5]}...")  # 只显示前5个参数
            print(f"  保留 graph_id: {keep_entry[2]}")
            print(f"  删除 graph_id: {[entry[2] for entry in delete_entries]}")
        else:
            # 没有重复，直接保留
            rows_to_keep.append(entries[0])
    
    print(f"\n去重统计:")
    print(f"  总配置数: {len(config_groups)}")
    print(f"  重复配置数: {len(duplicate_configs)}")
    print(f"  保留行数: {len(rows_to_keep)}")
    print(f"  删除行数: {len(rows_to_delete)}")
    print(f"  需要删除的目录: {len(dirs_to_delete)} 个")
    
    if len(duplicate_configs) == 0:
        print("没有发现重复的配置")
        return True
    
    if dry_run:
        print("\n[预览模式] 不执行实际操作")
        
        print("\n将要删除的目录:")
        for graph_id in sorted(dirs_to_delete):
            if graph_id in existing_dirs:
                if existing_dirs[graph_id]['original']:
                    print(f"  graph_{graph_id}/")
                if existing_dirs[graph_id]['modified']:
                    print(f"  graph_{graph_id}_modified/")
        
        print("\n重复配置详情:")
        for i, (config_key, info) in enumerate(list(duplicate_configs.items())[:5]):  # 只显示前5个
            print(f"  配置 {i+1}: 保留 {info['keep']}, 删除 {info['delete']}")
        if len(duplicate_configs) > 5:
            print(f"  ... 还有 {len(duplicate_configs) - 5} 个重复配置")
        
        return True
    
    # 确认操作
    response = input(f"\n是否继续执行去重操作? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("操作已取消")
        return False
    
    # 删除重复的目录
    print("\n删除重复的目录...")
    deleted_dirs = []
    failed_deletes = []
    
    if os.path.exists(iteration_generated_dir):
        for graph_id in dirs_to_delete:
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
    
    # 更新LUT文件
    print(f"\n更新LUT文件...")
    backup_file = f"{lut_file_path}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # 创建备份
        shutil.copy2(lut_file_path, backup_file)
        print(f"已创建备份文件: {backup_file}")
        
        # 按原始行号排序，准备写入新的LUT文件
        rows_to_keep.sort(key=lambda x: x[0])  # 按原始行号排序
        
        # 写入新的LUT文件
        with open(lut_file_path, 'w', newline='', encoding='utf-8') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            writer = csv.writer(f)
            writer.writerow(header)
            
            for row_idx, row_data, graph_id in rows_to_keep:
                # 更新时间戳
                updated_row = row_data.copy()
                updated_row[17] = datetime.now().isoformat()
                writer.writerow(updated_row)
        
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
    print(f"  删除重复配置数: {len(duplicate_configs)}")
    print(f"  删除目录数: {len(deleted_dirs)}")
    print(f"  最终保留LUT行数: {len(rows_to_keep)}")
    print(f"  备份文件: {backup_file}")
    
    if failed_deletes:
        print(f"\n删除失败的目录:")
        for path, error in failed_deletes:
            print(f"  {path}: {error}")
    
    # 显示详细的去重结果
    print(f"\n详细去重结果:")
    for config_key, info in duplicate_configs.items():
        config_str = f"tp={config_key[10]}, dp={config_key[11]}, pp={config_key[12]}, ep={config_key[13]}, cp={config_key[14]}"
        print(f"  {config_str}: 保留 {info['keep']}, 删除 {info['delete']}")
    
    return True

def main():
    parser = argparse.ArgumentParser(description="删除LUT中配置相同但graph_id不同的重复行")
    parser.add_argument("lut_file", help="STG LUT文件路径")
    parser.add_argument("--dry-run", action="store_true", help="预览模式，不执行实际操作")
    
    args = parser.parse_args()
    
    success = remove_duplicate_configs(args.lut_file, args.dry_run)
    
    if success:
        print("\n脚本执行完成")
    else:
        print("\n脚本执行失败")
        exit(1)

if __name__ == "__main__":
    main()
