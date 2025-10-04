import requests
import json
import pandas as pd
import polars as pl
from datetime import datetime, timedelta

# ======================
# 1. 配置
# ======================
def load_config(path="config.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_tenant_access_token(app_id, app_secret):
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    resp = requests.post(url, json={"app_id": app_id, "app_secret": app_secret}).json()
    if resp.get("code") != 0:
        raise RuntimeError(f"获取token失败: {resp}")
    return resp["tenant_access_token"]

# ======================
# 2. 获取数据
# ======================
def fetch_records(app_token, bitable_app_token, table_id, limit=500):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{bitable_app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {app_token}"}
    params = {"page_size": limit}
    records, page_token = [], None

    while True:
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=headers, params=params).json()
        if resp.get("code") != 0:
            raise RuntimeError(f"获取数据失败: {resp}")
        # 健壮性处理，确保 items 为列表
        items = resp.get("data", {}).get("items", [])
        if not items:
            break
        records.extend(items)
        page_token = resp["data"].get("page_token")
        if not page_token:
            break
    return records
# ======================
# 3. 目标表已有数据 key（去重用）
# ======================
def fetch_target_keys(app_token, bitable_app_token, target_table_id):
    records = fetch_records(app_token, bitable_app_token, target_table_id)
    keys = set(
        (
            r["fields"].get("填报日期"),
            r["fields"].get("姓名"),
            r["fields"].get("项目名称")
        )
        for r in records
    )
    return keys

def filter_new_records(df_transformed, target_keys):
    new_rows = []
    for _, row in df_transformed.iterrows():
        key = (row["填报日期"], row["姓名"], row["项目名称"])
        if key not in target_keys:
            new_rows.append(row)
    return pd.DataFrame(new_rows)


# ======================
# 4. 按日期过滤（增量模式）
# ======================
def filter_records_by_date(records, date_field, start_time, end_time):
    df = pd.DataFrame([r["fields"] for r in records])
    if date_field not in df.columns:
        print(f"[WARN] 数据中缺少 {date_field} 字段，跳过过滤")
        return df
    df[date_field] = pd.to_datetime(df[date_field], errors="coerce")
    df = df[(df[date_field] >= start_time) & (df[date_field] <= end_time)]
    return df

# ======================
# 4. 转换源数据
# ======================
def transform_source_records(df):
    if isinstance(df, list):
        df = pd.DataFrame([r["fields"] for r in df])
    df['检查'] = df['检查'].astype(str).str.strip()
    df = df.loc[
    ~df['检查'].str.contains('重复', case=False, na=False) &
    ~df['检查'].str.contains('空数据', case=False, na=False)
].copy()
    # print(df)
    df['项目1'] = df['项目名称-1'].astype(str) + '/' + df['项目工时-1'].astype(str)
    df['项目2'] = df['项目名称-2'].astype(str) + '/' + df['项目工时-2'].astype(str)
    df['项目3'] = df['项目名称-3'].astype(str) + '/' + df['项目工时-3'].astype(str)

    columns = ['工作状态', '填报日期', '部门', '姓名', '工作日志', '问题与沟通', '项目1', '项目2', '项目3']
    dfc = df[columns]
    # print(dfc)
    df_melted = dfc.melt(
        id_vars=['工作状态', '填报日期', '部门', '姓名', '工作日志', '问题与沟通'], 
        value_vars=["项目1", "项目2", "项目3"], 
        var_name="项目名称", 
        value_name="工时"
    )
    # print(pl.from_pandas(df_melted))
    # 分割 '项目名称-工时' 为两列
    # 只保留项目工时组合不为空且不是"/"的行
    df_melted = df_melted[~df_melted['工时'].str.contains('None', na=False)]
    # print(pl.from_pandas(df_melted))
    
    # 改进分割逻辑，使其更加健壮
    # 首先分割字符串
    split_data = df_melted['工时'].str.split('/', expand=True)
    
    # 确保始终有两列
    if split_data.shape[1] == 1:
        # 只有一列，说明没有分隔符，将第二列设为None
        split_data[1] = None
    elif split_data.shape[1] == 0:
        # 没有数据，添加两列None
        split_data[0] = None
        split_data[1] = None
    elif split_data.shape[1] > 2:
        # 超过两列，只保留前两列
        split_data = split_data.iloc[:, :2]
    
    # 重命名列
    split_data.columns = ['项目名称_new', '工时_new']
    
    # 将分割后的数据合并回原数据框
    df_melted = pd.concat([df_melted, split_data], axis=1)
    
    # 更新列名
    df_melted['项目名称'] = df_melted['项目名称_new']
    df_melted['工时'] = df_melted['工时_new']
    
    # 删除临时列
    df_melted = df_melted.drop(['项目名称_new', '工时_new'], axis=1)
    
    # 替换掉 'None' → NaN，然后转成数字，最后缺失值填 0
    df_melted['工时'] = (
        pd.to_numeric(df_melted['工时'].replace("None", pd.NA), errors="coerce")
        .fillna(0)
        .astype("float64")
    )
    df_melted = df_melted[(df_melted["工时"] != 0) & (df_melted["工时"].notna())]

    print("[INFO] 转换后的数据预览：")
    print(pl.from_pandas(df_melted))
    return df_melted

# ======================
# 5. 写回目标表
# ======================
def write_records(app_token, bitable_app_token, target_table_id, df, batch_size=50):
    headers = {"Authorization": f"Bearer {app_token}", "Content-Type": "application/json"}
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{bitable_app_token}/tables/{target_table_id}/records/batch_create"

    total, written = len(df), 0
    for i in range(0, total, batch_size):
        batch = df.iloc[i:i + batch_size].to_dict(orient="records")
        payload = {"records": [{"fields": row} for row in batch]}
        resp = requests.post(url, headers=headers, json=payload).json()
        if resp.get("code") == 0:
            written += len(batch)
            print(f"[DEBUG] 写入 {len(batch)} 条，累计 {written}/{total}")
        else:
            print(f"[ERROR] 写入失败: {resp}")

    print(f"[INFO] 成功写入 {written}/{total} 条")

# ======================
# 6. 主流程
# ======================
def etl_pipeline(cfg):
    token = get_tenant_access_token(cfg["app_id"], cfg["app_secret"])
    print("===== [ETL START] =====")

    # 抓取源表
    source_records = fetch_records(token, cfg["bitable_app_token"], cfg["source_table"])
    print(f"[INFO] 抓取源表 {len(source_records)} 条")

    # 根据模式选择增量或全量
    if cfg["mode"] == "incremental":
        start_time = datetime.now() - timedelta(days=cfg["days"])
        end_time = datetime.now()
        df_filtered = filter_records_by_date(source_records, cfg["date_field"], start_time, end_time)
        print(f"[INFO] 增量模式: {len(df_filtered)} 条符合时间窗口")
        # print(pl.from_pandas(df_filtered).head(20))
    else:
        df_filtered = pd.DataFrame([r["fields"] for r in source_records])
        print(f"[INFO] 全量模式: {len(df_filtered)} 条")
        # print(df_filtered.shape)

    # 数据转换
    df_transformed = transform_source_records(df_filtered)
    
    # 获取目标表已有 key 做去重
    target_keys = fetch_target_keys(token, cfg["bitable_app_token"], cfg["target_table"])
    df_to_write = filter_new_records(df_transformed, target_keys)

    # dry-run 模式下只打印，不写入
    if cfg.get("dry_run", False):
        print("[DRY-RUN] dry_run 模式开启，不会写入目标表")
        print(f"[DRY-RUN] 本次处理数据量: {len(df_to_write)} 条")
        # print(pl.from_pandas(df_to_write).head(20))
    else:
        if not df_to_write.empty:
            write_records(token, cfg["bitable_app_token"], cfg["target_table"], df_to_write, cfg["batch_size"])
        else:
            print("[INFO] 没有需要写入的数据")

    print("===== [ETL DONE] =====")

if __name__ == "__main__":
    cfg = load_config("config.json")
    etl_pipeline(cfg)
