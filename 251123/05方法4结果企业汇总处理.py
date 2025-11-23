import pandas as pd
import numpy as np
import ast  # 用于安全地将字符串转为列表
import os
from tqdm import tqdm

def calculate_n_median(list_str):
    """
    计算“方法2-小类数量列表”字符串的中位数。
    空列表或无效数据返回 0。
    """
    try:
        # 1. 使用 ast.literal_eval 安全地将字符串转为 Python 列表
        data_list = ast.literal_eval(str(list_str))
        
        # 2. 检查列表是否为空
        if not data_list or not isinstance(data_list, list):
            # 如果是空列表 "[]" 或无效数据，返回 0
            return 0
            
        # 3. 计算中位数
        return np.median(data_list)
        
    except (ValueError, SyntaxError, TypeError):
        # 如果字符串格式不正确 (例如 None, NaN, 或 "abc")，返回 0
        return 0

def process_file_for_task4(input_path, output_path):
    """
    执行Task 4: 计算 '方法2-小类数量列表' 的中位数 -> '方法4-N'
    """
    if not os.path.exists(input_path):
        print(f"❌ 错误：找不到输入文件: {input_path}")
        return

    print(f"\n" + "="*50)
    print(f"--- 正在处理文件 (Task 4): {os.path.basename(input_path)} ---")
    print(f"读取中: {input_path}")
    try:
        df = pd.read_excel(input_path)
    except Exception as e:
        print(f"❌ 读取Excel文件时出错: {e}")
        return
        
    print(f"共 {len(df)} 行数据。")
    
    # 检查必需的列
    if '方法2-小类数量列表' not in df.columns:
        print(f"❌ 错误：文件未包含 '方法2-小类数量列表' 列。")
        return

    # --- 步骤 1: 计算 '方法4-N' (中位数) ---
    print("步骤 1: 正在计算 '方法4-N' (中位数)...")
    tqdm.pandas(desc="计算 方法4-N")
    # 将 'calculate_n_median' 函数应用到目标列，并将结果存入新列
    df['方法4-N'] = df['方法2-小类数量列表'].progress_apply(calculate_n_median)
    print("计算完成。")

    # --- 导出 ---
    # 确保输出目录存在
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True) # exist_ok=True 避免在目录已存在时报错
        
    # 保存到新的Excel文件
    try:
        df.to_excel(output_path, index=False)
        print(f"✅ 成功保存结果到: {output_path}")
    except Exception as e:
        print(f"❌ 保存Excel文件时出错: {e}")

def main():
    """
    主执行函数 - 循环处理所有6个文件
    """
    # 1. 定义文件路径
    root_dir = '/Users/bl/git/patent/251123' # 根目录
    
    # 输入路径 (不带 task- 前缀的源文件)
    input_base_dir = os.path.join(root_dir, 'result')
    
    # 输出路径 (将存入 result/task4/ 子目录)
    output_base_dir = os.path.join(root_dir, 'result', 'task4')

    print(f"--- Task 4 (方法4-N) 计算启动 ---")
    print(f"读取源目录: {input_base_dir}")
    print(f"写入目标目录: {output_base_dir}")

    # 2. 定义6个文件的 *基础* 文件名
    base_filenames = [
        '上市公司&子公司绿色发明申请专利分类号_proce.xlsx',
        '上市公司本身绿色发明申请专利分类号_proce.xlsx',
        '上市公司&子公司绿色实用新型申请专利分类号_proce.xlsx',
        '上市公司本身绿色实用新型申请专利分类号_proce.xlsx',
        '上市公司&子公司绿色发明&实用申请专利分类号_proce.xlsx',
        '上市公司本身绿色发明&实用申请专利分类号_proce.xlsx'
    ]
    
    # 3. 循环处理所有文件
    for basename in base_filenames:
        # 构造输入路径
        # e.g., .../result/上市公司&子公司...
        input_path = os.path.join(input_base_dir, basename)
        
        # 构造输出文件名 (添加 'task4-' 前缀)
        # e.g., task4-上市公司&子公司...
        output_filename = f"task4-{basename}"
        
        # 构造输出路径
        # e.g., .../result/task4/task4-上市公司&子公司...
        output_path = os.path.join(output_base_dir, output_filename)
        
        # 执行处理
        process_file_for_task4(input_path, output_path)
    
    print("\n--- 所有 Task 4 计算任务已完成。 ---")

# --- 程序入口 ---
if __name__ == "__main__":
    main()