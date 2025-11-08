import pandas as pd
import re
from collections import Counter
import time
from tqdm import tqdm
import os # 引入 os 模块来创建文件夹

# --- 核心函数1: 提取专利部分 (无需修改) ---
def extract_patent_parts(patent_num_str):
    """
    从单个专利号字符串中提取其“大组”和“小类”。
    (大组：/前的部分；小类：大组的前4个字符)
    """
    s = patent_num_str.strip()
    if not s:
        return None, None
    
    main_group = ""
    match = re.search(r'\((.*?)\)', s)
    
    if match and '/' in match.group(1):
        target_s = match.group(1)
        main_group_raw = target_s.split('/')[0]
        main_group = re.split(r'[,;:(]', main_group_raw)[0].strip()
    elif '/' in s:
        main_group_raw = s.split('/')[0]
        main_group = re.split(r'\(', main_group_raw)[0].strip()
    else:
        main_group = re.split(r'\(', s)[0].strip()

    if not main_group:
        return None, None

    sub_class = main_group[:4]
    return main_group, sub_class

# --- 核心函数2: 处理单行 (v6 重构: 参数化) ---
def process_row(row, patent_cols, summary_col_name):
    """
    处理DataFrame的单行数据。
    (v6: 专利列 和 汇总列名 通过参数传入)
    """
    # patent_cols = [f'发明申请{c}类' for c in 'ABCDEFGH'] # v5的硬编码
    # v6: 使用传入的 patent_cols 列表
    
    full_patent_string = ""
    for col in patent_cols:
        if col in row.index and pd.notna(row[col]):
            full_patent_string += str(row[col])

    # 存入汇总列 (v6: 使用传入的 summary_col_name)
    row[summary_col_name] = full_patent_string

    if not full_patent_string:
        row['方法1-专利质量列表'] = []
        row['方法2-小类数量列表'] = []
        row['方法2-大组数量列表'] = []
        row['方法2-专利质量列表'] = []
        row['方法3-专利大组分类计数'] = {}
        return row

    patent_blocks_content = re.findall(r'\{(.*?)\}', full_patent_string)

    method1_q_list = []
    method2_N_list = []
    method2_n_list = []
    method2_q_list = []
    all_main_groups_for_row = []

    for block_content in patent_blocks_content:
        patent_num_strings = block_content.split(';')
        parts_list = []
        
        for s in patent_num_strings:
            s_clean = s.strip()
            if s_clean:
                main_group, sub_class = extract_patent_parts(s_clean)
                if main_group:
                    parts_list.append((main_group, sub_class))

        if not parts_list:
            continue

        # --- 方法1 ---
        main_groups_in_block = [mg for mg, sc in parts_list]
        p = len(main_groups_in_block)
        if p > 0:
            group_counts = Counter(main_groups_in_block)
            sum_sq_ratio = sum([(t / p) ** 2 for t in group_counts.values()])
            method1_q_list.append(1 - sum_sq_ratio)
        # else:
        #     method1_q_list.append(None) # 如果需要，可以取消注释

        # --- 方法2 ---
        sub_classes_in_block = [sc for mg, sc in parts_list]
        N = len(set(sub_classes_in_block))
        n = len(set(main_groups_in_block))
        # v6 健壮性修复: 避免 n=0 导致的除零错误
        if n > 0:
            method2_q_list.append(N + 1 - (1 / n))
        else:
            method2_q_list.append(N + 1) # 或者 0, None, 取决于业务逻辑
        
        method2_N_list.append(N)
        method2_n_list.append(n)

        # --- 方法3 ---
        all_main_groups_for_row.extend(main_groups_in_block)

    row['方法1-专利质量列表'] = method1_q_list
    row['方法2-小类数量列表'] = method2_N_list
    row['方法2-大组数量列表'] = method2_n_list
    row['方法2-专利质量列表'] = method2_q_list
    row['方法3-专利大组分类计数'] = dict(Counter(all_main_groups_for_row))

    return row

# --- 辅助函数: 加载数据 (v6 新增) ---
def load_data(file_path):
    """
    加载 Excel 或 CSV 文件，带错误处理。
    """
    print(f"\n开始加载文件: {file_path}")
    if not os.path.exists(file_path):
        print(f"❌ 错误: 文件未找到 {file_path}")
        return None
        
    start_time = time.time()
    df = None
    try:
        df = pd.read_excel(file_path)
    except Exception as e_excel:
        print(f"读取Excel失败: {e_excel}")
        try:
            df = pd.read_csv(file_path)
            print("...检测到CSV，成功加载CSV文件。")
        except Exception as e_csv:
            print(f"读取CSV也失败: {e_csv}")
            print("请检查文件格式是否正确。")
            return None

    load_time = time.time()
    print(f"文件加载完毕，耗时: {load_time - start_time:.2f} 秒。共 {len(df)} 行数据。")
    return df

# --- 核心函数3: 专利处理流水线 (v6 重构) ---
def run_processing_task(
    input_df, 
    data_prefixes, 
    count_prefixes, 
    summary_col_name, 
    output_merged_excel, 
    output_listed_excel,
    task_name=""):
    """
    (v6 重构): 这是一个通用的处理函数，取代了 v5 的 main 函数。
    
    参数:
    - input_df (pd.DataFrame): 预加载的输入数据
    - data_prefixes (list): 专利数据列的前缀, e.g., ['发明申请'] or ['发明申请', '实用新型申请']
    - count_prefixes (list): 专利计数列的前缀, e.g., ['发明申请'] or ['发明申请', '实用新型申请']
    - summary_col_name (str): 新增的汇总列的名称, e.g., '发明专利汇总'
    - output_merged_excel (str): 分支1 (合并) 的输出路径
    - output_listed_excel (str): 分支2 (仅上市公司) 的输出路径
    - task_name (str): 用于打印日志的任务名称
    """
    
    print("\n" + "#"*60)
    print(f"--- 任务: {task_name} ---")
    print(f"--- 目标输出 (合并): {os.path.basename(output_merged_excel)}")
    print(f"--- 目标输出 (本身): {os.path.basename(output_listed_excel)}")
    print("#"*60)
    
    # 动态生成列列表
    patent_data_cols = []
    patent_count_cols = []
    
    for prefix in data_prefixes:
        patent_data_cols.extend([f'{prefix}{c}类' for c in 'ABCDEFGH'])
    
    for prefix in count_prefixes:
        patent_count_cols.extend([f'{prefix}{c}类数量' for c in 'ABCDEFGH'])

    # 确保列存在，忽略不存在的列
    existing_patent_data_cols = [col for col in patent_data_cols if col in input_df.columns]
    existing_patent_count_cols = [col for col in patent_count_cols if col in input_df.columns]
    cols_to_drop = existing_patent_data_cols + existing_patent_count_cols
    
    print(f"将处理 {len(existing_patent_data_cols)} 个专利数据列 (前缀: {data_prefixes})")
    print(f"将聚合/移除 {len(existing_patent_count_cols)} 个专利计数列 (前缀: {count_prefixes})")

    df = input_df.copy() # 确保操作的是副本

    # --- 定义列组 ---
    group_keys = ['股票代码', '会计年度']
    
    # 辅助函数：用于合并专利字符串
    def join_strings(series):
        return ''.join(series.dropna().astype(str))
        
    # --------------------------------------------------
    # --- 分支 1: 合并 "上市公司" & "子公司" ---
    # --------------------------------------------------
    print("\n" + "="*50)
    print(f"[{task_name}] --- 开始处理: 1. 上市公司 & 子公司 (合并) ---")
    print("="*50)

    # 找到所有其他需要保留的列（例如 '申请时间'），并取第一个值
    agg_cols = existing_patent_data_cols + existing_patent_count_cols
    other_cols = [col for col in df.columns if col not in group_keys and col not in agg_cols]

    # 定义聚合规则
    agg_funcs = {}
    for col in existing_patent_data_cols:
        agg_funcs[col] = join_strings  # 合并专利字符串
    for col in existing_patent_count_cols:
        agg_funcs[col] = 'sum'       # 合计专利数量
    for col in other_cols:
        if col in df.columns:
            agg_funcs[col] = 'first' # 其他列取第一个值
    
    print("正在按 '股票代码' 和 '会计年度' 合并数据...")
    df_merged = df.groupby(group_keys, as_index=False).agg(agg_funcs)
    
    # 手动设置 '公司类型' 为新值
    df_merged['公司类型'] = '上市公司及其子公司'
    print(f"合并完成，共 {len(df_merged)} 行。")

    # 对合并后的数据运行处理
    tqdm.pandas(desc=f"[{task_name}-分支1] 处理合并数据")
    df_merged_processed = df_merged.progress_apply(
        process_row, 
        axis=1, 
        patent_cols=existing_patent_data_cols, # 传入参数
        summary_col_name=summary_col_name       # 传入参数
    )

    # 清理合并后的数据
    print("清理 [分支1] 的原始列...")
    df_merged_processed = df_merged_processed.drop(columns=cols_to_drop, errors='ignore')

    # 保存合并后的数据
    try:
        df_merged_processed.to_excel(output_merged_excel, index=False)
        print(f"✅ [{task_name}-分支1] 已保存合并后结果到: {output_merged_excel}")
    except Exception as e_save_merged:
        print(f"❌ [{task_name}-分支1] 保存合并后文件失败: {e_save_merged}")

    # --------------------------------------------------
    # --- 分支 2: 仅 "上市公司本身" ---
    # --------------------------------------------------
    print("\n" + "="*50)
    print(f"[{task_name}] --- 开始处理: 2. 仅上市公司本身 ---")
    print("="*50)
    
    # 筛选数据
    df_listed_only = df[df['公司类型'] == '上市公司本身'].copy()
    
    if len(df_listed_only) == 0:
        print("⚠️ 警告: 未在数据中找到 '公司类型' == '上市公司本身' 的行。跳过 [分支2]。")
    else:
        print(f"已筛选 '上市公司本身' 数据，共 {len(df_listed_only)} 行。")

        # 对筛选后的数据运行处理
        tqdm.pandas(desc=f"[{task_name}-分支2] 处理'上市公司本身'数据")
        df_listed_processed = df_listed_only.progress_apply(
            process_row, 
            axis=1, 
            patent_cols=existing_patent_data_cols, # 传入参数
            summary_col_name=summary_col_name       # 传入参数
        )

        # 清理筛选后的数据
        print("清理 [分支2] 的原始列...")
        df_listed_processed = df_listed_processed.drop(columns=cols_to_drop, errors='ignore')

        # 保存筛选后的数据
        try:
            df_listed_processed.to_excel(output_listed_excel, index=False)
            print(f"✅ [{task_name}-分支2] 已保存 '上市公司本身' 结果到: {output_listed_excel}")
        except Exception as e_save_listed:
            print(f"❌ [{task_name}-分支2] 保存 '上市公司本身' 文件失败: {e_save_listed}")

# --- 核心函数4: 主调度函数 (v6 新增) ---
def main():
    """
    (v6 新增): 主执行函数 - 调度中心
    负责定义路径、加载数据、并调用3次处理流水线
    """
    # 1. --- 定义路径 ---
    root_dir = '/Users/bl/git/patent/251108' # <<< 已更新路径
    res_dir = os.path.join(root_dir, 'res')
    result_dir = os.path.join(root_dir, 'result')
    
    # 确保结果文件夹存在
    os.makedirs(result_dir, exist_ok=True)

    # 输入文件路径
    file_invention = os.path.join(res_dir, '上市公司发明申请专利分类号.xlsx')
    file_utility = os.path.join(res_dir, '上市公司实用新型申请专利分类号.xlsx')
    
    # 输出文件路径 (6个)
    out_inv_merged = os.path.join(result_dir, 'task1-上市公司&子公司发明申请专利分类号_proce.xlsx')
    out_inv_listed = os.path.join(result_dir, 'task1-上市公司本身发明申请专利分类号_proce.xlsx')
    
    out_util_merged = os.path.join(result_dir, 'task1-上市公司&子公司实用新型申请专利分类号_proce.xlsx')
    out_util_listed = os.path.join(result_dir, 'task1-上市公司本身实用新型申请专利分类号_proce.xlsx')
    
    out_comb_merged = os.path.join(result_dir, 'task1-上市公司&子公司发明&实用申请专利分类号_proce.xlsx')
    out_comb_listed = os.path.join(result_dir, 'task1-上市公司本身发明&实用申请专利分类号_proce.xlsx')

    print(f"--- 专利处理 v6 启动 ---")
    print(f"根目录: {root_dir}")
    print(f"结果目录: {result_dir}")
    start_time_all = time.time()

    # 2. --- 加载数据 ---
    df_invention = load_data(file_invention)
    df_utility = load_data(file_utility)

    # 3. --- 执行任务 ---

    # --- 任务1: 仅 "发明" ---
    if df_invention is not None:
        run_processing_task(
            input_df = df_invention,
            data_prefixes = ['发明申请'],
            count_prefixes = ['发明申请'],
            summary_col_name = '发明专利汇总',
            output_merged_excel = out_inv_merged,
            output_listed_excel = out_inv_listed,
            task_name = "发明专利"
        )
    else:
        print("\n--- 跳过 任务1 (发明专利)，因为输入文件加载失败 ---")

    # --- 任务2: 仅 "实用新型" ---
    if df_utility is not None:
        run_processing_task(
            input_df = df_utility,
            data_prefixes = ['实用新型申请'],
            count_prefixes = ['实用新型申请'],
            summary_col_name = '实用新型专利汇总',
            output_merged_excel = out_util_merged,
            output_listed_excel = out_util_listed,
            task_name = "实用新型专利"
        )
    else:
        print("\n--- 跳过 任务2 (实用新型专利)，因为输入文件加载失败 ---")

    # --- 任务3: "发明" & "实用新型" 合并 ---
    if df_invention is not None and df_utility is not None:
        print("\n" + "#"*60)
        print("--- 任务: 发明&实用 (合并数据准备) ---")
        print("#"*60)
        
        # 识别基础列 (非专利列) 用于合并
        inv_data_cols = [f'发明申请{c}类' for c in 'ABCDEFGH']
        inv_count_cols = [f'发明申请{c}类数量' for c in 'ABCDEFGH']
        util_data_cols = [f'实用新型申请{c}类' for c in 'ABCDEFGH']
        util_count_cols = [f'实用新型申请{c}类数量' for c in 'ABCDEFGH']

        base_cols_inv = [c for c in df_invention.columns if c not in inv_data_cols + inv_count_cols]
        base_cols_util = [c for c in df_utility.columns if c not in util_data_cols + util_count_cols]
        
        # 找到两边共有的基础列作为合并键
        merge_keys = list(set(base_cols_inv) & set(base_cols_util))
        
        if not merge_keys:
            print("❌ 错误: 无法合并 '发明' 和 '实用新型' 数据，因为它们没有共同的基准列 (如 '股票代码', '会计年度' 等)。")
        else:
            print(f"将使用 {len(merge_keys)} 个共同列进行 outer merge。")
            print(f"合并键 (示例): {merge_keys[:5]}...")
            
            # 使用 outer merge 来保留所有公司的所有年份记录
            df_combined = pd.merge(df_invention, df_utility, on=merge_keys, how='outer')
            print(f"合并后的数据共 {len(df_combined)} 行。")
            
            # 为合并后的任务设置参数
            run_processing_task(
                input_df = df_combined,
                data_prefixes = ['发明申请', '实用新型申请'], # < 关键
                count_prefixes = ['发明申请', '实用新型申请'], # < 关键
                summary_col_name = '发明&实用专利汇总',
                output_merged_excel = out_comb_merged,
                output_listed_excel = out_comb_listed,
                task_name = "发明&实用专利"
            )
            
    else:
        print("\n--- 跳过 任务3 (发明&实用)，因为一个或两个输入文件加载失败 ---")

    end_time_all = time.time()
    print(f"\n--- 所有任务处理完毕，总耗时: {end_time_all - start_time_all:.2f} 秒。 ---")


# --- 程序入口 ---
if __name__ == "__main__":
    main()