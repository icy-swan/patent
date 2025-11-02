# 文件名: process_patents_v5.py
import pandas as pd
import re
from collections import Counter
import time
from tqdm import tqdm

# 1. 分析所有的专利（即{}内的数据为1个专利），分别统计1个专利内的所有专利号。进行每个专利号所在的大组进行各大组的专利号数的计数。最后计算单个专利的质量q。
#   q=1-[(各个大组t的数量/1个专利内的所有专利号的总数p)的平方并求和]。并将一行内所有的q，存入一个列表q_list，存入列“方法1-专利质量列表”。
#   例如该企业有专利：{G06Q11/10;G06Q11/11;G06Q13/10}{G06Q10/10;G06Q12/10}，其第一个专利的大组为G06Q11，2个。G06Q13，1个。
#   其q=1-{[2/(2+1)]^2 + [1/(2+1)]^2}。第二个专利的q=1-{[1/(1+1)]^2 + [1/(1+1)]^2}。再将两个q存入列表“方法1-专利质量列表”
# 2. 分析所有的专利（即{}内的数据为1个专利），分别统计1个专利内其所有小类去重后的小类种类的数量N，和其大组去重后的大组种类的数量n，再计算单个专利的质量q。
#   q=N+1-1/n。将所有的专利的N，存入一个列表N_list，存入列“方法2-小类数量列表”。将所有的专利的n，存入一个列表n_list，存入列“方法2-大组数量列表”。
#   将所有的专利的q，存入一个列表q_list，存入列“方法2-专利质量列表”。
# 3. 分析所有的专利号，按照其所归属的大组，分别统计各个大组的专利号的数量（patent_number_count_by_group），存入列“方法3-专利大组分类计数”。

root_dir = '/Users/bl/git/patent/251102'

# --- 核心函数1: 提取专利部分 ---
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

# --- 核心函数2: 处理单行 (无需修改) ---
def process_row(row):
    """
    处理DataFrame的单行数据。
    """
    patent_cols = [f'发明申请{c}类' for c in 'ABCDEFGH']
    full_patent_string = ""
    for col in patent_cols:
        if col in row.index and pd.notna(row[col]):
            full_patent_string += str(row[col])

    # 存入汇总列
    row['发明专利汇总'] = full_patent_string

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
        #     method1_q_list.append(None)

        # --- 方法2 ---
        sub_classes_in_block = [sc for mg, sc in parts_list]
        N = len(set(sub_classes_in_block))
        n = len(set(main_groups_in_block))
        method2_q_list.append(N + 1 - (1 / n))
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

# --- 核心函数3: 主调度函数 (V5 重构) ---
def main():
    """
    主执行函数 - 分叉处理
    """
    input_file = root_dir + '/res/上市公司发明申请专利分类号.xlsx'
    output_merged_excel = root_dir + '/result/上市公司&子公司发明申请专利分类号_proce.xlsx'
    output_listed_excel = root_dir + '/result/上市公司本身发明申请专利分类号_proce.xlsx'
    # --- 修改结束 ---

    print(f"开始加载文件: {input_file}")
    start_time = time.time()
    
    try:
        df = pd.read_excel(input_file)
    except Exception as e_excel:
        print(f"读取Excel失败: {e_excel}")
        try:
            df = pd.read_csv(input_file)
            print("成功加载CSV文件。")
        except Exception as e_csv:
            print(f"读取CSV也失败: {e_csv}")
            print("请检查文件名和文件格式是否正确。")
            return

    load_time = time.time()
    print(f"文件加载完毕，耗时: {load_time - start_time:.2f} 秒。")
    print(f"共 {len(df)} 行数据。")

    # --- 定义列组 ---
    group_keys = ['股票代码', '会计年度']
    patent_data_cols = [f'发明申请{c}类' for c in 'ABCDEFGH']
    patent_count_cols = [f'发明申请{c}类数量' for c in 'ABCDEFGH']
    cols_to_drop = patent_data_cols + patent_count_cols
    
    # 辅助函数：用于合并专利字符串
    def join_strings(series):
        return ''.join(series.dropna().astype(str))
        
    # --------------------------------------------------
    # --- 分支 1: 合并 "上市公司" & "子公司" ---
    # --------------------------------------------------
    print("\n" + "="*50)
    print("--- 开始处理: 1. 上市公司 & 子公司 (合并) ---")
    print("="*50)

    # 找到所有其他需要保留的列（例如 '申请时间'），并取第一个值
    agg_cols = patent_data_cols + patent_count_cols
    other_cols = [col for col in df.columns if col not in group_keys and col not in agg_cols]

    # 定义聚合规则
    agg_funcs = {}
    for col in patent_data_cols:
        agg_funcs[col] = join_strings  # 合并专利字符串
    for col in patent_count_cols:
        agg_funcs[col] = 'sum'       # 合计专利数量
    for col in other_cols:
        agg_funcs[col] = 'first'     # 其他列取第一个值
    
    print("正在按 '股票代码' 和 '会计年度' 合并数据...")
    df_merged = df.groupby(group_keys, as_index=False).agg(agg_funcs)
    
    # 手动设置 '公司类型' 为新值
    df_merged['公司类型'] = '上市公司及其子公司'
    print(f"合并完成，共 {len(df_merged)} 行。")

    # 对合并后的数据运行处理
    tqdm.pandas(desc="[分支1] 正在处理合并后数据")
    df_merged_processed = df_merged.progress_apply(process_row, axis=1)

    # 清理合并后的数据
    print("清理 [分支1] 的原始列...")
    df_merged_processed = df_merged_processed.drop(columns=cols_to_drop, errors='ignore')

    # 保存合并后的数据
    try:
        df_merged_processed.to_excel(output_merged_excel, index=False)
        print(f"✅ [分支1] 已保存合并后结果到: {output_merged_excel}")
    except Exception as e_save_merged:
        print(f"❌ [分支1] 保存合并后文件失败: {e_save_merged}")

    # --------------------------------------------------
    # --- 分支 2: 仅 "上市公司本身" ---
    # --------------------------------------------------
    print("\n" + "="*50)
    print("--- 开始处理: 2. 仅上市公司本身 ---")
    print("="*50)
    
    # 筛选数据
    df_listed_only = df[df['公司类型'] == '上市公司本身'].copy()
    
    if len(df_listed_only) == 0:
        print("⚠️ 警告: 未在数据中找到 '公司类型' == '上市公司本身' 的行。跳过 [分支2]。")
    else:
        print(f"已筛选 '上市公司本身' 数据，共 {len(df_listed_only)} 行。")

        # 对筛选后的数据运行处理
        tqdm.pandas(desc="[分支2] 正在处理'上市公司本身'数据")
        df_listed_processed = df_listed_only.progress_apply(process_row, axis=1)

        # 清理筛选后的数据
        print("清理 [分支2] 的原始列...")
        df_listed_processed = df_listed_processed.drop(columns=cols_to_drop, errors='ignore')

        # 保存筛选后的数据
        try:
            df_listed_processed.to_excel(output_listed_excel, index=False)
            print(f"✅ [分支2] 已保存 '上市公司本身' 结果到: {output_listed_excel}")
        except Exception as e_save_listed:
            print(f"❌ [分支2] 保存 '上市公司本身' 文件失败: {e_save_listed}")

    end_time = time.time()
    print(f"\n--- 所有任务处理完毕，总耗时: {end_time - start_time:.2f} 秒。 ---")

# --- 程序入口 ---
if __name__ == "__main__":
    main()