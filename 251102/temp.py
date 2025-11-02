import pandas as pd
import re
from collections import Counter
import time
from tqdm import tqdm

root_dir = '/Users/bl/git/patent/251102'

def extract_patent_parts(patent_num_str):
    """
    从单个专利号字符串中提取其“大组”和“小类”。
    
    处理三种情况:
    1. 特殊格式: //(A61K31/546...) 或 foo(A61K31/...) - / 在括号内
    2. 普通格式: G06Q40/00(2012.01)I - / 在括号外
    3. 简单格式: G06Q11/10 - / 在括号外，无括号
    4. 无斜杠格式: G06Q10(2012.01)I
    """
    s = patent_num_str.strip()
    if not s:
        return None, None
    
    main_group = ""
    
    # 查找第一对括号内的内容
    match = re.search(r'\((.*?)\)', s)
    
    # 1. 特殊情况: "/" 在括号内部, e.g., //(A61K31/546...)
    if match and '/' in match.group(1):
        target_s = match.group(1)  # "A61K31/546,31∶43"
        main_group_raw = target_s.split('/')[0]
        # 清理掉可能的逗号、分号等
        main_group = re.split(r'[,;:(]', main_group_raw)[0].strip() # "A61K31"
    
    # 2. & 3. 普通情况: "/" 在括号外部, 或没有括号
    # e.g., G06Q40/00(2012.01)I  or  G06Q11/10
    elif '/' in s:
        main_group_raw = s.split('/')[0] # "G06Q40(2012.01)I" or "G06Q11"
        main_group = re.split(r'\(', main_group_raw)[0].strip() # "G06Q40" or "G06Q11"
    
    # 4. 无斜杠情况: e.g., G06Q10(2012.01)I
    else:
        main_group = re.split(r'\(', s)[0].strip() # "G06Q10"

    if not main_group:
        return None, None

    # 提取小类：取大组的前4个字符（长度不足4则取全部）
    sub_class = main_group[:4]
    
    return main_group, sub_class

def process_row(row):
    """
    处理DataFrame的单行数据。
    """
    # 1. 汇总所有发明申请列的字符串
    patent_cols = [f'发明申请{c}类' for c in 'ABCDEFGH']
    full_patent_string = ""
    for col in patent_cols:
        if col in row.index and pd.notna(row[col]):
            full_patent_string += str(row[col])

    if not full_patent_string:
        # 如果行是空的，返回空结果
        row['方法1-专利质量列表'] = []
        row['方法2-小类数量列表'] = []
        row['方法2-大组数量列表'] = []
        row['方法2-专利质量列表'] = []
        row['方法3-专利大组分类计数'] = {}
        return row

    # 2. 提取所有独立的专利 ( {...} )
    # re.findall会找到所有 {} 括号内的内容
    patent_blocks_content = re.findall(r'\{(.*?)\}', full_patent_string)

    method1_q_list = []
    method2_N_list = []
    method2_n_list = []
    method2_q_list = []
    all_main_groups_for_row = [] # 用于方法3

    # 3. 遍历该行的每一个专利 ( e.g., "G06Q11/10;G06Q11/11;G06Q13/10" )
    for block_content in patent_blocks_content:
        # 3.1 提取该专利内的所有专利号
        patent_num_strings = block_content.split(';')
        parts_list = [] # 存储 (main_group, sub_class) 元组
        
        for s in patent_num_strings:
            s_clean = s.strip()
            if s_clean:
                main_group, sub_class = extract_patent_parts(s_clean)
                if main_group: # 仅当成功提取到大组时才添加
                    parts_list.append((main_group, sub_class))

        if not parts_list:
            # 这个专利块是空的或无效的，跳过
            continue

        # --- 方法1: 计算专利质量 q = 1 - sum( (t/p)^2 ) ---
        main_groups_in_block = [mg for mg, sc in parts_list]
        p = len(main_groups_in_block) # p = 专利内的所有专利号的总数
        
        if p > 0:
            group_counts = Counter(main_groups_in_block) # 统计各个大组t的数量
            sum_sq_ratio = 0
            for t in group_counts.values():
                sum_sq_ratio += (t / p) ** 2
            q1 = 1 - sum_sq_ratio
            method1_q_list.append(q1)
        # else:
        #     method1_q_list.append(None) # 如果p=0，不处理

        # --- 方法2: 计算专利质量 q = N + 1 - (1/n) ---
        sub_classes_in_block = [sc for mg, sc in parts_list]
        N = len(set(sub_classes_in_block)) # N = 小类去重后的种类数量
        n = len(set(main_groups_in_block)) # n = 大组去重后的种类数量
        
        # *** 修改点：应用新公式 q = N + 1 - (1/n) ***
        # 因为 parts_list 非空, n 必定 >= 1, 不会触发 ZeroDivisionError
        q2 = N + 1 - (1 / n)
        
        method2_N_list.append(N)
        method2_n_list.append(n)
        method2_q_list.append(q2)

        # --- 方法3: 收集数据 ---
        all_main_groups_for_row.extend(main_groups_in_block)

    # 4. 汇总整行的结果
    
    # 方法3: 统计整行所有大组的计数
    method3_counts = Counter(all_main_groups_for_row)
    
    # 赋值新列
    row['方法1-专利质量列表'] = method1_q_list
    row['方法2-小类数量列表'] = method2_N_list
    row['方法2-大组数量列表'] = method2_n_list
    row['方法2-专利质量列表'] = method2_q_list
    row['方法3-专利大组分类计数'] = dict(method3_counts) # 存为字典

    return row

def main():
    """
    主执行函数
    """
    # --- 请修改这里 ---
    input_file = root_dir + '/res/上市公司发明申请专利分类号.xlsx'
    output_csv = root_dir + '/result/上市公司发明申请专利分类号_proce.csv'
    output_excel = root_dir + '/result/上市公司发明申请专利分类号_proce.xlsx'
    # --- 修改结束 ---

    print(f"开始加载文件: {input_file}")
    start_time = time.time()
    
    try:
        # 优先尝试读取Excel
        df = pd.read_excel(input_file)
    except Exception as e_excel:
        print(f"读取Excel失败: {e_excel}")
        try:
            # 尝试读取CSV (对于大数据量更推荐)
            df = pd.read_csv(input_file)
            print("成功加载CSV文件。")
        except Exception as e_csv:
            print(f"读取CSV也失败: {e_csv}")
            print("请检查文件名和文件格式是否正确。")
            return

    load_time = time.time()
    print(f"文件加载完毕，耗时: {load_time - start_time:.2f} 秒。")
    print(f"共 {len(df)} 行数据。开始处理...")

    tqdm.pandas(desc="正在处理专利数据")
    # 核心处理步骤：将 process_row 函数应用到每一行
    df_processed = df.progress_apply(process_row, axis=1)

    process_time = time.time()
    print(f"\n所有行处理完毕，耗时: {process_time - load_time:.2f} 秒。")

    try:
        df_processed.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"已保存结果到CSV: {output_csv}")

        df_processed.to_excel(output_excel, index=False)
        print(f"已保存结果到Excel: {output_excel}")
        
    except Exception as e_save:
        print(f"保存文件失败: {e_save}")

    end_time = time.time()
    print(f"任务总耗时: {end_time - start_time:.2f} 秒。")


# --- 程序入口 ---
if __name__ == "__main__":
    main()