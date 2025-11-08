import pandas as pd
import numpy as np
import ast  # 用于安全地将字符串转为列表 (AST = Abstract Syntax Tree)
import os
from tqdm import tqdm

def calculate_median(list_str):
    """
    计算一个代表列表的字符串的中位数。
    例如: "[0.44, 0.5]"
    (来自您的脚本，保持不变)
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

def process_file_for_median(input_path, output_path):
    """
    读取一个处理后的Excel文件，计算中位数，并保存到新路径。
    (来自您的脚本，保持不变)
    """
    if not os.path.exists(input_path):
        print(f"❌ 错误：找不到输入文件: {input_path}")
        return

    print(f"\n--- 正在处理文件 ---")
    print(f"读取中: {os.path.basename(input_path)}")
    try:
        df = pd.read_excel(input_path)
    except Exception as e:
        print(f"❌ 读取Excel文件时出错: {e}")
        return
        
    print(f"共 {len(df)} 行数据。开始计算 '方法1-专利质量列表' 的中位数...")
    
    # 检查目标列是否存在
    if '方法1-专利质量列表' not in df.columns:
        print(f"❌ 错误：在文件中未找到列 '方法1-专利质量列表'。")
        return

    # 初始化 tqdm
    tqdm.pandas(desc="计算中位数")
    
    # 4. 将函数应用到列，创建新列
    df['方法1-专利质量中位数'] = df['方法1-专利质量列表'].progress_apply(calculate_median)

    print("计算完成。")
    
    # 5. 确保输出目录存在
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"已创建新目录: {output_dir}")
        
    # 6. 保存到新的Excel文件
    try:
        df.to_excel(output_path, index=False)
        print(f"✅ 成功保存结果到: {output_path}")
    except Exception as e:
        print(f"❌ 保存Excel文件时出错: {e}")

def main():
    """
    主执行函数 - (v2 更新)
    自动循环处理所有6个文件。
    """
    # 1. 定义根路径和目录
    root_dir = '/Users/bl/git/patent/251108' # <<< 已更新
    
    # 输入目录 (不带 task1- 前缀的源文件)
    input_base_dir = os.path.join(root_dir, 'result')
    
    # 输出目录 (将存入 result/task1/ 子目录)
    output_base_dir = os.path.join(root_dir, 'result', 'task1')

    print(f"--- 专利质量中位数计算 ---")
    print(f"读取源目录: {input_base_dir}")
    print(f"写入目标目录: {output_base_dir}")
    
    # 2. 定义6个文件的 *基础* 文件名 (即您在提示中列出的)
    base_filenames = [
        '上市公司&子公司发明申请专利分类号_proce.xlsx',
        '上市公司本身发明申请专利分类号_proce.xlsx',
        '上市公司&子公司实用新型申请专利分类号_proce.xlsx',
        '上市公司本身实用新型申请专利分类号_proce.xlsx',
        '上市公司&子公司发明&实用申请专利分类号_proce.xlsx',
        '上市公司本身发明&实用申请专利分类号_proce.xlsx'
    ]
    
    # 3. 循环处理所有文件
    for basename in base_filenames:
        # 构造输入路径
        # e.g., .../result/上市公司&子公司...
        input_path = os.path.join(input_base_dir, basename)
        
        # 构造输出文件名 (添加 'task1-' 前缀)
        # e.g., task1-上市公司&子公司...
        output_filename = f"task1-{basename}"
        
        # 构造输出路径
        # e.g., .../result/task1/task1-上市公司&子公司...
        output_path = os.path.join(output_base_dir, output_filename)
        
        # 执行处理
        process_file_for_median(input_path, output_path)

    print("\n--- 所有6个文件的中位数计算任务已完成。 ---")

# --- 程序入口 ---
if __name__ == "__main__":
    main()