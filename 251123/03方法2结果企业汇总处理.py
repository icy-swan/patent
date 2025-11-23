import pandas as pd
import numpy as np
import ast  # 用于安全地将字符串转为列表
import os
from tqdm import tqdm

def calculate_qm_median(list_str):
    """
    计算“方法2-专利质量列表”字符串的中位数。
    空列表或无效数据返回 0。
    (来自您的脚本，保持不变)
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
    (来自您的脚本，保持不变)
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
    主执行函数 - (v2 更新)
    自动循环处理所有6个文件。
    """
    # 1. 定义文件路径
    root_dir = '/Users/bl/git/patent/251123' # <<< 已更新
    
    # 输入路径 (不带 task- 前缀的源文件)
    input_base_dir = os.path.join(root_dir, 'result')
    
    # 输出路径 (将存入 result/task2/ 子目录)
    output_base_dir = os.path.join(root_dir, 'result', 'task2')

    print(f"--- Task 2 (QM, Qit) 计算启动 ---")
    print(f"读取源目录: {input_base_dir}")
    print(f"写入目标目录: {output_base_dir}")

    # 2. 定义6个文件的 *基础* 文件名
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
        
        # 构造输出文件名 (添加 'task2-' 前缀)
        # e.g., task2-上市公司&子公司...
        output_filename = f"task2-{basename}"
        
        # 构造输出路径
        # e.g., .../result/task2/task2-上市公司&子公司...
        output_path = os.path.join(output_base_dir, output_filename)
        
        # 执行处理
        process_file_for_task2(input_path, output_path)
    
    print("\n--- 所有 Task 2 计算任务已完成。 ---")

# --- 程序入口 ---
if __name__ == "__main__":
    main()