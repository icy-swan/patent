# 每行，方法2-专利质量列表的中位数，赋值为“方法2-QM”，并添加列“方法2-QM”
# 根据会计年度，求所有行的“方法2-QM”的 - 最大值，赋值为“方法2-QM-MAX”；最小值，赋值为“方法2-QM-MIN”。并给每一行该年度的数据都添加这两列数据
# 每行，计算“方法2-Qit"= （“方法2-QM” - “方法2-QM-MIN”） /（“方法2-QM-MAX” - “方法2-QM-MIN”），并添加列“方法2-Qit”

import pandas as pd
import numpy as np
import ast  # 用于安全地将字符串转为列表
import os
from tqdm import tqdm

def calculate_qm_median(list_str):
    """
    计算“方法2-专利质量列表”字符串的中位数。
    空列表或无效数据返回 0。
    """
    try:
        data_list = ast.literal_eval(str(list_str))
        if not data_list or not isinstance(data_list, list):
            return 0  # 空列表 "[]" 或无效数据，返回 0
        return np.median(data_list)
    except (ValueError, SyntaxError, TypeError):
        return 0  # 格式不正确 (例如 None, NaN) 也返回 0

def process_file_for_task2(input_path, output_path):
    """
    执行Task 2的三个步骤：QM, QM-MIN/MAX, Qit
    """
    if not os.path.exists(input_path):
        print(f"❌ 错误：找不到输入文件: {input_path}")
        return

    print(f"\n" + "="*50)
    print(f"--- 正在处理文件: {os.path.basename(input_path)} ---")
    print(f"读取中: {input_path}")
    try:
        df = pd.read_excel(input_path)
    except Exception as e:
        print(f"❌ 读取Excel文件时出错: {e}")
        return
        
    print(f"共 {len(df)} 行数据。")
    
    # 检查必需的列
    if '方法2-专利质量列表' not in df.columns or '会计年度' not in df.columns:
        print(f"❌ 错误：文件未包含 '方法2-专利质量列表' 或 '会计年度' 列。")
        return

    # --- 步骤 1: 遍历每一行，求“方法2-专利质量列表”的中位数-“方法2-QM” ---
    print("步骤 1: 正在计算 '方法2-QM' (中位数)...")
    tqdm.pandas(desc="计算 QM")
    df['方法2-QM'] = df['方法2-专利质量列表'].progress_apply(calculate_qm_median)

    # --- 步骤 2: 根据“会计年度”，求QM的最大值-“方法2-QM-MAX”和最小值-“方法2-QM-MIN” ---
    print("步骤 2: 正在计算 '会计年度' 组内的 MIN 和 MAX...")
    # .transform() 会将分组计算的结果广播回原始的每一行
    df['方法2-QM-MAX'] = df.groupby('会计年度')['方法2-QM'].transform('max')
    df['方法2-QM-MIN'] = df.groupby('会计年度')['方法2-QM'].transform('min')
    print("MIN/MAX 计算完成。")

    # --- 步骤 3: 遍历每行，计算“方法2-Qit” ---
    print("步骤 3: 正在计算 '方法2-Qit' (归一化)...")
    
    # 使用矢量化计算（非常快），无需进度条
    numerator = df['方法2-QM'] - df['方法2-QM-MIN']
    denominator = df['方法2-QM-MAX'] - df['方法2-QM-MIN']
    
    # 使用 np.where() 来处理分母为0的情况（即该年度的MAX==MIN）
    # 如果分母为0，则 Qit 也为 0 (0/0 的情况)
    df['方法2-Qit'] = np.where(denominator == 0, 0, numerator / denominator)
    print("Qit 计算完成。")

    # --- 导出 ---
    # 确保输出目录存在
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"已创建新目录: {output_dir}")
        
    # 保存到新的Excel文件
    try:
        df.to_excel(output_path, index=False)
        print(f"✅ 成功保存结果到: {output_path}")
    except Exception as e:
        print(f"❌ 保存Excel文件时出错: {e}")

def main():
    """
    主执行函数
    """
    # 1. 定义文件路径
    root_dir = '/Users/bl/git/patent/251102'
    
    # 输入路径 (V6的输出)
    input_merged_excel = os.path.join(root_dir, 'result', '上市公司&子公司发明申请专利分类号_proce.xlsx')
    input_listed_excel = os.path.join(root_dir, 'result', '上市公司本身发明申请专利分类号_proce.xlsx')
    
    # 输出路径 (Task 2)
    output_merged_excel = os.path.join(root_dir, 'result', 'task2', 'task2-上市公司&子公司发明申请专利分类号_proce.xlsx')
    output_listed_excel = os.path.join(root_dir, 'result', 'task2', 'task2-上市公司本身发明申请专利分类号_proce.xlsx')

    # 2. 处理第一个文件 (合并版)
    process_file_for_task2(input_merged_excel, output_merged_excel)
    
    # 3. 处理第二个文件 (仅上市公司)
    process_file_for_task2(input_listed_excel, output_listed_excel)
    
    print("\n--- 所有 Task 2 计算任务已完成。 ---")

# --- 程序入口 ---
if __name__ == "__main__":
    main()