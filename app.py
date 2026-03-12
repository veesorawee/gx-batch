import os
import urllib
import json
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy.engine import create_engine
from sqlalchemy import text
import pandas as pd
import great_expectations as gx
import datetime
import glob
import re
import traceback
import google.generativeai as genai
import time
import uuid
import threading
import webbrowser 
import time
import os
import sys

# Fix WeasyPrint Library Loading on macOS (Homebrew)
if sys.platform == "darwin":
    homebrew_lib_path = "/opt/homebrew/lib"
    if homebrew_lib_path not in os.environ.get("DYLD_FALLBACK_LIBRARY_PATH", ""):
        os.environ["DYLD_FALLBACK_LIBRARY_PATH"] = f"{homebrew_lib_path}:{os.environ.get('DYLD_FALLBACK_LIBRARY_PATH', '')}"

import weasyprint
from pypdf import PdfWriter, PdfReader
import urllib.parse as urlparse
import io

# --- 0. App Configuration & Setup ---
st.set_page_config(layout="wide", page_title="Tracking Validation")

# ตั้งค่าโฟลเดอร์สำหรับเก็บไฟล์
SQL_DIR = "sql"
EXPECTATIONS_DIR = "expectations"
LOGS_DIR = "logs"
REPORTS_DIR = "reports"
os.makedirs(SQL_DIR, exist_ok=True)
os.makedirs(EXPECTATIONS_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

def cleanup_old_logs(days=1):
    now = time.time()
    for filename in os.listdir(LOGS_DIR):
        file_path = os.path.join(LOGS_DIR, filename)
        if os.path.isfile(file_path):
            if os.stat(file_path).st_mtime < now - days * 86400:
                try:
                    os.remove(file_path)
                except Exception:
                    pass

cleanup_old_logs(1)

# โหลดค่าจาก .env และตั้งค่า Gemini API
load_dotenv()
GEMINI_AVAILABLE = False
api_key = os.getenv("GEMINI_API_KEY") or st.secrets.get("GEMINI_API_KEY")
if api_key:
    try:
        genai.configure(api_key=api_key)
        GEMINI_AVAILABLE = True
    except Exception:
        GEMINI_AVAILABLE = False

# --- DB Configuration Check ---
TRINO_HOST = os.getenv('TRINO_HOST')
TRINO_PORT = os.getenv('TRINO_PORT')
TRINO_USERNAME = os.getenv('TRINO_USERNAME')
TRINO_PASSWORD = os.getenv('TRINO_PASSWORD')

if not all([TRINO_HOST, TRINO_PORT, TRINO_USERNAME, TRINO_PASSWORD]):
    st.error("❌ **Database Configuration Not Found!** Please configure TRINO_HOST, TRINO_PORT, TRINO_USERNAME, and TRINO_PASSWORD in your .env file.")
    st.stop()

DB_CONFIG = (TRINO_HOST, TRINO_PORT, TRINO_USERNAME, TRINO_PASSWORD)

# --- 1. Helper Functions ---

def prepare_spec_sql(base_sql, current_event_type, current_placement_id):
    # 0. ตรวจว่ามี {event_type} placeholder หรือไม่
    has_event_type_placeholder = '{event_type}' in base_sql
    
    # ถ้ามี → แทนที่ด้วย event_type จริง (เช่น click, pageview, impression, custom)
    if has_event_type_placeholder:
        base_sql = base_sql.replace('{event_type}', current_event_type)
    
    sql_lower = base_sql.lower()
    final_sql = base_sql
    
    # 1. หั่น placement_id ด้วยเครื่องหมาย '/'
    # Format: screen_name/event_category/target
    parts = str(current_placement_id).split('/')
    p_screen = parts[0] if len(parts) > 0 else None
    p_category = parts[1] if len(parts) > 1 else None
    p_target = parts[2] if len(parts) > 2 else None
    
    # 2. สร้างเงื่อนไข WHERE แบบ Dynamic
    where_conditions = []
    
    if 'raw' in sql_lower and not has_event_type_placeholder:
        where_conditions.append(f"event_type = '{current_event_type}'")
    
    if p_screen:
        where_conditions.append(f"screen_name = '{p_screen}'")
        
    if current_event_type in ['click', 'impression', 'custom']:
        if p_category:
            where_conditions.append(f"event_category = '{p_category}'")
        if p_target:
            where_conditions.append(f"target = '{p_target}'")
            
    dynamic_where_clause = " AND ".join(where_conditions)
    
    # 3. นำไปประกอบร่างกับ SQL หลัก
    if 'raw' in sql_lower:
        # raw table → ครอบ Subquery (เพราะ raw มีทุก column อยู่แล้ว)
        final_sql = f"SELECT * FROM (\n{base_sql}\n) AS filtered_raw \nWHERE {dynamic_where_clause}"
    elif has_event_type_placeholder:
        # {event_type} SQL → ต่อ WHERE conditions เข้าไปตรงๆ (ไม่ครอบ subquery)
        if where_conditions:
            # ตัดช่องว่างท้าย SQL แล้วต่อ AND เข้าไป
            final_sql = base_sql.rstrip() + "\nAND " + "\nAND ".join(where_conditions)
    else:
        if f"_{current_event_type}" not in sql_lower and current_event_type not in sql_lower:
            st.warning(f"⚠️ คำเตือน: SQL ที่คุณระบุอาจจะไม่มีตารางสำหรับ event_type '{current_event_type}'")
            
    return final_sql

def list_files(directory, extension):
    return [os.path.basename(f) for f in glob.glob(os.path.join(directory, f"*.{extension}"))]

def flatten_json_columns(df: pd.DataFrame, flatten_map: dict) -> pd.DataFrame:
    for parent_col, keys_to_flatten in flatten_map.items():
        if parent_col not in df.columns or not keys_to_flatten: continue
        
        def safe_get_from_json_string(json_string, key):
            # 1. ถ้าข้อมูลเป็น Dict (Object) มาตั้งแต่ต้น (อาจเกิดจากการที่ Trino ส่งมาเป็น Object เลย)
            if isinstance(json_string, dict):
                return json_string.get(key)
                
            # 2. ถ้าข้อมูลเป็น String ให้ลองแปลงเป็น JSON ดู
            if isinstance(json_string, str):
                try: 
                    parsed = json.loads(json_string)
                    # ✅ [พระเอกอยู่ตรงนี้] เช็คให้ชัวร์ว่าเป็น Dict จริงๆ ไม่ใช่ก้อน String หรือขยะ ถึงจะใช้ .get() ได้
                    if isinstance(parsed, dict):
                        return parsed.get(key)
                    return None
                except json.JSONDecodeError: 
                    return None
            
            # 3. ถ้าเป็นค่าว่าง (None) หรือแบบอื่นๆ ให้ข้ามไป
            return None

        for key in keys_to_flatten:
            df[f'{parent_col}_{key}'] = df[parent_col].apply(safe_get_from_json_string, key=key)
    return df

def render_waive_ui(batch):
    if "waived_rules" not in batch:
        batch["waived_rules"] = set()

    results = batch.get("results", [])
    all_failed_items = extract_failed_items(results, set())
    if all_failed_items:
        st.markdown("### 🚨 สรุป กฎที่ไม่ผ่าน (Failed Rules Summary) & Waive")
        import uuid
        df_items = pd.DataFrame(all_failed_items)
        g = df_items.groupby(["col", "exp_type", "rule_key"])
        summary_data = []
        for (col, exp_type, rule_key), group in g:
            first_kwargs = group.iloc[0]["kwargs"] if "kwargs" in group.columns else {}
            
            extra_info = ""
            if "regex" in first_kwargs: extra_info = f'regex: {first_kwargs["regex"]}'
            elif "value_set" in first_kwargs: extra_info = f'set: {first_kwargs["value_set"]}'
            elif "column_set" in first_kwargs: extra_info = f'{len(first_kwargs["column_set"])} cols'
            elif "json_schema" in first_kwargs: extra_info = f'schema'
            
            ex_str = ""
            if "examples" in group.columns:
                for ex_list in group["examples"]:
                    if isinstance(ex_list, list) and ex_list:
                        ex_str = str(ex_list[0])
                        break
                        
            detail_parts = []
            if extra_info: detail_parts.append(f"Expect: {extra_info}")
            if ex_str: detail_parts.append(f"Ex: {ex_str}")
            detail_str = " | ".join(detail_parts) if detail_parts else "-"
            
            summary_data.append({
                "Waive": rule_key in batch["waived_rules"],
                "Column": col,
                "Rule": exp_type,
                "Details": detail_str,
                "Errors": group["count"].sum(),
                "Impacted Placements": len(group["placement"].unique()),
                "_rule_key": rule_key
            })
        
        df_summary = pd.DataFrame(summary_data)
        df_summary = df_summary.sort_values(by=["Errors"], ascending=False).reset_index(drop=True)
        
        editor_key = f"waive_editor_{batch.get('id', 'temp')}"
        
        # --- สร้างฟังก์ชัน Callback สำหรับจัดการ State ทันทีก่อนที่จอจะ Rerun ---
        def process_waive_submission(edited_data, current_batch, failed_items):
            try:
                # สร้าง mapping จาก rule_key ไปหา original expectation type
                rule_key_to_original_type = {
                    item["rule_key"]: item.get("exp_type_original", "") 
                    for item in failed_items
                }
                
                current_waives = set()
                structured_waives = []
                
                for idx, row in edited_data.iterrows():
                    if row["Waive"]:
                        rule_key = f"{row['Column']} ({row['Rule']})"
                        current_waives.add(rule_key)
                        structured_waives.append({
                            "column": row['Column'],
                            "short_rule": row['Rule'],
                            "expectation_type": rule_key_to_original_type.get(rule_key, ""),
                            "rule_key": rule_key
                        })
                        
                current_batch["waived_rules"] = current_waives
                
                # เซฟลง JSON
                for run in current_batch.get("results", []):
                    if "log_filepath" in run and os.path.exists(run["log_filepath"]):
                        try:
                            with open(run["log_filepath"], 'r', encoding='utf-8') as f:
                                log_data = json.load(f)
                            log_data["waived_rules"] = structured_waives
                            with open(run["log_filepath"], 'w', encoding='utf-8') as f:
                                json.dump(log_data, f, indent=4)
                        except Exception as json_err:
                            st.warning(f"⚠️ Could not update waived_rules: {json_err}")
                
                # Update History
                for h_batch in st.session_state.history_batches:
                    if h_batch.get("id") == current_batch.get("id"):
                        h_batch["waived_rules"] = current_waives
                        break

                # Generate PDF
                pdf_path = generate_batch_report_pdf(current_batch, current_batch["waived_rules"])
                if pdf_path:
                    current_batch["pdf_path"] = pdf_path
                    for h_batch in st.session_state.history_batches:
                        if h_batch.get("id") == current_batch.get("id"):
                            h_batch["pdf_path"] = pdf_path
                            break
                            
            except Exception as e:
                current_batch["pdf_error"] = str(e)

            # ล็อค State ให้แน่นก่อน Rerun
            st.session_state.stage = "results_done" if st.session_state.stage != "history_view" else "history_view"
            st.session_state.viewing_batch = current_batch
            st.session_state.waive_success_msg = "✅ ยกเว้น Rule สำเร็จและผูกเข้าไฟล์ Log / PDF เรียบร้อย!"

        # -------------------------------------------------------------
        
        with st.form(key=f"waive_form_{batch.get('id', 'temp')}", border=True):
            edited_df = st.data_editor(
                df_summary[["Waive", "Column", "Rule", "Details", "Errors", "Impacted Placements"]],
                column_config={
                    "Waive": st.column_config.CheckboxColumn("ยกเว้น (Waive) 🛑"),
                },
                disabled=["Column", "Rule", "Details", "Errors", "Impacted Placements"],
                hide_index=True,
                use_container_width=True,
                key=editor_key
            )
            
            submit_waive = st.form_submit_button("✅ 1. Confirm Waived Rules & Regenerate Report", type="primary")
            if submit_waive:
                process_waive_submission(edited_df, batch, all_failed_items)
                st.rerun()
                
        # แสดงข้อความสำเร็จ (ถ้ามีเก็บไว้ใน state จากรอบก่อน)
        if st.session_state.get("waive_success_msg"):
            st.success(st.session_state.waive_success_msg)
            st.session_state.waive_success_msg = "" # ล้างทิ้งหลังโชว์เสร็จ
            
    if "pdf_path" in batch: # Move PDF Download here, right below the form
        if batch.get("pdf_path") and os.path.exists(batch["pdf_path"]):
            with open(batch["pdf_path"], "rb") as f:
                pdf_bytes = f.read()
                dl_name = os.path.basename(batch["pdf_path"])
                if batch.get("waived_rules"): dl_name = dl_name.replace(".pdf", "_waived.pdf")
                st.download_button("📥 2. ดาวน์โหลด PDF Report (ข้อมูลล่าสุด)", pdf_bytes, file_name=dl_name, mime="application/pdf", use_container_width=True)
        elif batch.get("pdf_error"):
            st.warning(f"⚠️ สร้าง PDF ไม่ได้: {batch['pdf_error']}")
            
        st.markdown("---")

def render_summary_metrics(batch, batch_id_display):
    results = batch.get("results", [])
    if "waived_rules" not in batch: batch["waived_rules"] = set()
    success_count, fail_count, error_count = get_effective_metrics(results, batch["waived_rules"])
    st.caption(f"🏷️ Batch ID: `{batch_id_display}` | รวม {len(results)} specs")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("✅ Passed", success_count)
    c2.metric("❌ Mismatch", fail_count)
    c3.metric("🚨 Error", error_count)
    c4.metric("📊 Total", len(results))
    st.markdown("---")

def parse_select_columns(sql_string: str) -> list[str]:
    try:
        match = re.search(r'SELECT(.*?)FROM', sql_string, re.IGNORECASE | re.DOTALL)
        if not match: return []
        columns_str = match.group(1).replace('\n', ' ').replace('\r', '')
        columns = [col.strip().split(' as ')[-1].strip() for col in columns_str.split(',')]
        return [col for col in columns if col]
    except Exception:
        return []

def generate_automated_sql(screen_name: str, app_type: str, event_type: str, event_category: str, target: str, segment_columns: list) -> str:
    table_name_middle = f"_{app_type}" if app_type == "web" else ""
    table_name = f"jitsu_lm_user{table_name_middle}_{event_type}"
    try:
        with open("sql_template.txt", "r", encoding="utf-8") as f:
            template = f.read()
    except FileNotFoundError:
        st.error("File not found: 'sql_template.txt'. Please create it first.")
        st.stop()
    existing_columns = parse_select_columns(template)
    missing_segment_cols = [col for col in segment_columns if col not in existing_columns]
    if missing_segment_cols:
        cols_to_add = ", ".join(missing_segment_cols) + ", "
        template = re.sub(r'SELECT\s+', f'SELECT {cols_to_add}', template, count=1, flags=re.IGNORECASE)
    where_clauses = [f"dt = '{{dt}}'"]
    if screen_name: where_clauses.append(f"screen_name = '{screen_name}'")
    if event_type in ["click", "impression", "custom"]:
        if event_category: where_clauses.append(f"event_category = '{event_category}'")
    if event_type in ["click", "impression"]:
        if target: where_clauses.append(f"target = '{target}'")
    base_query = template.format(table_name=table_name)
    query = f"{base_query} WHERE {' AND '.join(where_clauses)} {{extra_where_conditions}}"
    return query

def display_validation_errors(checkpoint_result, waived_rules=None):
    if waived_rules is None: waived_rules = set()

    failed_results_list = []
    if not checkpoint_result.success:
        for result_identifier, validation_result_dict in checkpoint_result.run_results.items():
            batch_id = result_identifier.batch_identifier
            match = re.search(r'pandas_ds_(?:.*?_)?(.+?)-asset_', batch_id)
            segment = match.group(1) if match else batch_id
            validation_result = validation_result_dict['validation_result']
            for result in validation_result.results:
                if not result.success:
                    kwargs = result.expectation_config.kwargs
                    exp_type = result.expectation_config.expectation_type.replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")
                    col = kwargs.get("column", "-")
                    if "expect_table" in result.expectation_config.expectation_type:
                        col = "-"
                    
                    rule_key = f"{col} ({exp_type})"
                    is_waived = rule_key in waived_rules
                    
                    expected = kwargs.get("value_set") or kwargs.get("regex") or kwargs.get("sum_total") or kwargs.get("type_")
                    unexpected_count = result.result.get("unexpected_count", 0)
                    element_count = result.result.get("element_count", 0)
                    error_percent = (unexpected_count / element_count) * 100 if element_count > 0 else 0
                    raw_examples = result.result.get("partial_unexpected_list", [])
                    string_examples = [str(item) for item in raw_examples]
                    failed_results_list.append({
                        "Status": "⚠️ Waived" if is_waived else "❌ Failed",
                        "Segment": segment, "Expectation": exp_type,
                        "Column": col, "Expected": str(expected),
                        "Error Count": unexpected_count, "Error %": error_percent,
                        "Failing Examples": string_examples
                    })
    if failed_results_list:
        st.subheader("Failure Summary")
        df = pd.DataFrame(failed_results_list)
        df = df.sort_values(by="Status", ascending=False) # Put ❌ Failed above ⚠️ Waived
        st.data_editor(df, column_config={
                "Status": st.column_config.TextColumn("Status", width="small"),
                "Error %": st.column_config.ProgressColumn("Error Rate", format="%.2f%%", min_value=0, max_value=100),
                "Failing Examples": st.column_config.ListColumn("Examples", width="large"),
                "Expected": st.column_config.TextColumn("Expected Value", width="medium"),
            }, use_container_width=True, hide_index=True)

class DummyStatus:
    def write(self, *args, **kwargs): pass
    def update(self, *args, **kwargs): pass
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): pass

def run_validation_process(sql_input, expectations_input, suite_name, db_config, val_params, flatten_map=None, batch_id=None, background=False):
    trino_host, trino_port, trino_username, trino_password = db_config
    db_query_date, SEGMENT_BY_COLUMNS = val_params
    context = gx.get_context(project_root_dir=os.getcwd())
    checkpoint_result, data_samples, docs_info = None, None, {"main": None, "segments": {}, "local_path": None}
    process_log = []
    run_name = None
    docs_error = None

    status_context = DummyStatus() if background else st.status("🚀 Starting Validation Process...", expanded=True)

    with status_context as status:
        def log_and_write(message):
            process_log.append(message)
            status.write(message)

        run_name = f"run_{suite_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        log_and_write(f"**Run Name:** `{run_name}` | **Suite:** `{suite_name}`")
        try:
            expectations_list = json.loads(expectations_input)
        except json.JSONDecodeError:
            process_log.append("🚨 ERROR: Invalid Expectations JSON format.")
            if not background: status.update(label="Input Error", state="error"); st.error("Invalid Expectations JSON format.")
            return None, None, None, process_log, run_name, None

        log_and_write("🔍 Discovering object columns...")
        if flatten_map:
            # ✅ ใช้ flatten_map จาก Spec โดยตรง (มี group + key ชัดเจน)
            keys_to_flatten_map = {parent: list(keys) for parent, keys in flatten_map.items()}
            object_columns = sorted(list(keys_to_flatten_map.keys()))
            log_and_write(f"**Identified Parent Object columns (from spec):** `{object_columns}`")
        else:
            # Fallback สำหรับ Manual Mode: อนุมานจากชื่อคอลัมน์ใน expectations
            all_columns_in_rules = {exp['kwargs']['column'] for exp in expectations_list if 'column' in exp.get('kwargs', {})}
            keys_to_flatten_map = {}
            for col_name in all_columns_in_rules:
                if "_" in col_name:
                    parent = col_name.split('_')[0]
                    key = col_name[len(parent) + 1:]
                    if parent not in keys_to_flatten_map:
                        keys_to_flatten_map[parent] = []
                    if key not in keys_to_flatten_map[parent]:
                        keys_to_flatten_map[parent].append(key)
            object_columns = sorted(list(keys_to_flatten_map.keys()))
            if object_columns:
                log_and_write(f"**Identified Parent Object columns (inferred):** `{object_columns}`")

        if keys_to_flatten_map:
            log_and_write("🗺️ **Fields to flatten:**")
            for obj_col, keys in keys_to_flatten_map.items():
                if keys:
                    keys_to_flatten_map[obj_col] = sorted(list(set(keys)))
                    log_and_write(f"&nbsp;&nbsp;&nbsp;↳ For `{obj_col}`: `{keys_to_flatten_map[obj_col]}`")

        log_and_write(f"**1. 🔗 Connecting to DB...**")
        try:
            engine = create_engine(f'trino://{trino_username}:{urllib.parse.quote(trino_password)}@{trino_host}:{trino_port}/', connect_args={'http_scheme': 'https', 'source': 'gx-streamlit-app'})
            with engine.connect() as conn: conn.execute(text("SELECT 1"))
            log_and_write("... ✅ Connection successful.")
        except Exception as e:
            process_log.append(f"🚨 ERROR: Could not connect to Database: {e}")
            if not background: status.update(label="Connection Failed", state="error"); st.error(f"Could not connect to Database: {e}")
            return None, None, None, process_log, run_name, None

        segments_to_run = []
        if SEGMENT_BY_COLUMNS:
            log_and_write(f"**2. 🔭 Discovering segments by:** `{', '.join(SEGMENT_BY_COLUMNS)}`")
            try:
                base_query_for_discovery = sql_input.format(dt=db_query_date, extra_where_conditions="")
                discovery_sql = f"SELECT {', '.join(SEGMENT_BY_COLUMNS)} FROM ({base_query_for_discovery}) AS discovery_subquery GROUP BY {', '.join(SEGMENT_BY_COLUMNS)}"
                segments_df = pd.read_sql(text(discovery_sql), engine)
                segments_to_run = segments_df.to_dict('records')
            except Exception as e:
                process_log.append(f"🚨 ERROR: Error discovering segments: {e}")
                if not background: status.update(label="Error", state="error"); st.error(f"Error discovering segments: {e}")
                return None, None, None, process_log, run_name, None
            if not segments_to_run:
                process_log.append(f"⚠️ Warning: No segments found for date `{db_query_date}`.")
                if not background: status.update(label="Warning", state="error"); st.warning(f"⚠️ No segments found for date `{db_query_date}`.")
                return None, None, None, process_log, run_name, None
            log_and_write(f"... ✅ Found **{len(segments_to_run)}** segments.")
        else:
            log_and_write("**2. 🗂️ No segmentation specified.**")
            segments_to_run = [{}]

        context = gx.get_context(); context.add_or_update_expectation_suite(suite_name); validations_to_run = []
        log_and_write("**3. ⚙️ Processing each segment...**")
        data_samples = {}
        for i, segment_filters in enumerate(segments_to_run):
            segment_name = "-".join(str(v) for v in segment_filters.values()).replace(".", "_") if segment_filters else "full_dataset"
            log_and_write(f"&nbsp;&nbsp;&nbsp;↳ **Segment {i+1}/{len(segments_to_run)}:** `{segment_name}`")
            extra_where_conditions = " ".join([f"AND {col} = '{val}'" for col, val in segment_filters.items()])
            final_query = sql_input.format(dt=db_query_date, extra_where_conditions=extra_where_conditions) + " LIMIT 500"
            try:
                df = pd.read_sql(text(final_query), engine)
                if df.empty: 
                    status.write(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- No data. Skipping.")
                    continue
                
                # 1. คลีนข้อมูลให้เป็น Dictionary ที่สมบูรณ์ก่อน
                for col_name in object_columns:
                    if col_name in df.columns:
                        def parse_to_dict(val):
                            if isinstance(val, dict): return val
                            if isinstance(val, str):
                                try:
                                    parsed = json.loads(val)
                                    if isinstance(parsed, str): # ดัก Double Encoded
                                        parsed = json.loads(parsed)
                                    if isinstance(parsed, dict): return parsed
                                except: pass
                            return None
                        
                        df[col_name] = df[col_name].apply(parse_to_dict)

                # 2. ตรวจสอบ field เกินจาก Spec (Strict Field Check)
                for parent_col, expected_keys in keys_to_flatten_map.items():
                    if parent_col in df.columns:
                        actual_keys = set()
                        for val in df[parent_col].dropna():
                            if isinstance(val, dict):
                                actual_keys.update(val.keys())
                        extra_keys = actual_keys - set(expected_keys)
                        if extra_keys:
                            log_and_write(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;⚠️ **Extra fields in `{parent_col}`** (ไม่อยู่ใน Spec): `{sorted(extra_keys)}`")

                # 3. ดึงข้อมูลย่อย (Flatten) ตอนที่มันยังเป็น Dict อยู่
                df = flatten_json_columns(df, keys_to_flatten_map)
                
                # =======================================================
                # 3. 🛠️ แปลงคอลัมน์ก้อนแม่ กลับไปเป็น JSON String สะอาดๆ 1 ชั้น
                # เพื่อให้ GX (json_schema) เอาไปเข้า json.loads() ต่อได้โดยไม่พัง
                for col_name in object_columns:
                    if col_name in df.columns:
                        df[col_name] = df[col_name].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)
                # แปลงค่า dict/list ในคอลัมน์ลูกให้เป็น JSON string ด้วย (เช่น event_audience_list ที่เป็น array)
                for parent_col, keys in keys_to_flatten_map.items():
                    for key in keys:
                        flat_col = f"{parent_col}_{key}"
                        if flat_col in df.columns:
                            df[flat_col] = df[flat_col].apply(
                                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)
                # =======================================================

                # 4. 🎯 ตัด column ที่ไม่อยู่ใน Spec ออก ทั้งหมด
                # parent struct (dt, event, screen, entry, target) และ field ที่ใช้ query loop
                # (event_type, event_category, screen_name, target) ไม่ต้องเช็ค
                spec_columns = set()
                for exp_config in expectations_list:
                    if exp_config.get("expectation_type") == "expect_table_columns_to_match_set":
                        spec_columns = set(exp_config["kwargs"].get("column_set", []))
                        break
                if spec_columns:
                    keep_cols = [c for c in df.columns if c in spec_columns]
                    dropped = sorted([c for c in df.columns if c not in spec_columns])
                    if dropped:
                        log_and_write(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;🗑️ Dropped non-spec columns: `{dropped}`")
                    df = df[keep_cols]

                ds_suffix = f"{batch_id}_{segment_name}" if batch_id else segment_name
                data_samples[segment_name] = df.head(5)
                datasource = context.sources.add_or_update_pandas(f"pandas_ds_{ds_suffix}")
                asset_name = f"asset_{ds_suffix}"
                data_asset = datasource.add_dataframe_asset(name=asset_name)
                batch_request = data_asset.build_batch_request(dataframe=df)
                validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
                for exp_config in expectations_list:
                    getattr(validator, exp_config['expectation_type'])(**exp_config["kwargs"])
                validator.save_expectation_suite(discard_failed_expectations=False)
                validations_to_run.append({"batch_request": batch_request, "expectation_suite_name": suite_name})
            except Exception as e:
                tb_str = traceback.format_exc()
                
                # เขียนข้อความสั้นๆ ให้ User เห็นตอนรัน
                status.write(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- 🚨 Failed: {e}")
                if not background:
                    st.expander(f"Full Error Traceback for Segment: {segment_name}").code(tb_str, language='text')
                
                # [เพิ่มตรงนี้] เอา Full Traceback ยัดใส่ Markdown เพื่อให้เซฟลง History Log ด้วย
                error_log_markdown = (
                    f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- 🚨 **Failed:** `{e}`\n\n"
                    f"<details><summary>🔎 คลิกเพื่อดู Full Error Traceback</summary>\n\n"
                    f"```python\n{tb_str}\n```\n\n"
                    f"</details>"
                )
                process_log.append(error_log_markdown)
                continue

        if not validations_to_run:
            process_log.append("🚨 ERROR: No valid batches were created.")
            if not background: status.update(label="Error", state="error"); st.error("No valid batches were created.")
            return None, None, None, process_log, run_name, None

        log_and_write("**4. 🏁 Running final Great Expectations checkpoint...**")
        checkpoint = context.add_or_update_checkpoint(name=f"checkpoint_{run_name}", run_name_template=run_name,
            action_list=[
                {"name": "store_validation_result", "action": {"class_name": "StoreValidationResultAction"}},
                {"name": "store_evaluation_params", "action": {"class_name": "StoreEvaluationParametersAction"}},
                {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
            ])
        checkpoint_result = checkpoint.run(validations=validations_to_run)

        log_and_write("**5. 📄 Building Data Docs...**")
        context.build_data_docs()
        
        log_and_write("... Retrieving local report paths.")
        docs_sites = context.get_docs_sites_urls()
        local_site_url = docs_sites[0]['site_url'] if docs_sites else None
        if local_site_url:
            log_and_write(f"... 📑 Main local report path found: `{local_site_url}`")
            docs_info["local_path"] = local_site_url

            if checkpoint_result:
                for result_identifier in checkpoint_result.run_results.keys():
                    batch_id = result_identifier.batch_identifier
                    match = re.search(r'pandas_ds_(?:.*?_)?(.+?)-asset_', batch_id)
                    if match:
                        segment_name = match.group(1)
                        segment_url_list = context.get_docs_sites_urls(resource_identifier=result_identifier)
                        if segment_url_list:
                            segment_file_url = segment_url_list[0]['site_url']
                            docs_info["segments"][segment_name] = segment_file_url
                            log_and_write(f"... 📑 Segment report path for '{segment_name}': `{segment_file_url}`")
        else:
            log_and_write("... ⚠️ Could not retrieve local path for Data Docs.")
                            
        if not background: status.update(label="✅ **Validation Process Complete!**", state="complete", expanded=False)

    return checkpoint_result, data_samples, docs_info, process_log, run_name, docs_error

def extract_failed_items(results, waived_rules=None):
    if waived_rules is None: waived_rules = set()
    failed_items = []
    for run_data in results:
        if run_data.get('status') == "FAILURE" and run_data.get('checkpoint_result'):
            plc_id = run_data.get("plc_id", "Unknown")
            ev_type = run_data.get("ev_type", "Unknown")
            spec_key = f"{ev_type} | {plc_id}"
            
            run_results = run_data.get("log_data", {}).get("run_results", {})
            for vr_key, vr_val in run_results.items():
                total_exp = 0
                fail_count = 0
                validation = vr_val.get("validation_result", {})
                for exp_result in validation.get("results", []):
                    total_exp += 1
                    if not exp_result.get("success", False):
                        exp_config = exp_result.get("expectation_config", {})
                        exp_type = exp_config.get("expectation_type", "").replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")
                        col = exp_config.get("kwargs", {}).get("column", "-")
                        if "expect_table" in exp_config.get("expectation_type", ""): col = "-"
                        rule_key = f"{col} ({exp_type})"
                        if rule_key not in waived_rules: # Assuming waived_rules is passed correctly here
                            fail_count += 1
                
                # If no rules were generated, treat as an error for this specific validation result
                if total_exp == 0:
                    # This part of the code is for extracting failed items,
                    # so if there are no rules, there are no *failed* items to extract.
                    # The "No Rules" error is handled at a higher level (get_effective_metrics, get_effective_tracker_data)
                    # and in the PDF generation for display purposes.
                    pass
                else:
                    # Original logic for failed items extraction
                    for exp_result in validation.get("results", []):
                        if not exp_result.get("success", False):
                            exp_config = exp_result.get("expectation_config", {})
                            exp_type = exp_config.get("expectation_type", "").replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")
                            col = exp_config.get("kwargs", {}).get("column", "-")
                            if "expect_table" in exp_config.get("expectation_type", ""):
                                col = "-"
                            rule_key = f"{col} ({exp_type})"
                            
                            if rule_key not in waived_rules:
                                failed_items.append({
                                    "col": col, "exp_type": exp_type, "rule_key": rule_key,
                                    "exp_type_original": exp_config.get("expectation_type", ""),
                                    "placement": spec_key, "count": exp_result.get("result", {}).get("unexpected_count", 0),
                                    "kwargs": exp_config.get("kwargs", {}),
                                    "examples": exp_result.get("result", {}).get("partial_unexpected_list", [])
                                })
    return failed_items

def get_effective_metrics(results, waived_rules=None):
    if waived_rules is None: waived_rules = set()
    success, fail, error = 0, 0, 0
    for run_data in results:
        if run_data.get('status') == "ERROR":
            error += 1
            continue
        if run_data.get('status') == "SUCCESS":
            success += 1
            continue
        
        has_unwaived = False
        has_tests = False
        run_results = run_data.get("log_data", {}).get("run_results", {})
        for vr_key, vr_val in run_results.items():
            results_list = vr_val.get("validation_result", {}).get("results", [])
            if results_list: has_tests = True
            for exp_result in results_list:
                if not exp_result.get("success", False):
                    exp_config = exp_result.get("expectation_config", {})
                    exp_type = exp_config.get("expectation_type", "").replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")
                    col = exp_config.get("kwargs", {}).get("column", "-")
                    if "expect_table" in exp_config.get("expectation_type", ""):
                        col = "-"
                    if f"{col} ({exp_type})" not in waived_rules:
                        has_unwaived = True
                        break
            if has_unwaived: break
                
        if not has_tests: error += 1
        elif has_unwaived: fail += 1
        else: success += 1
    return success, fail, error

def get_effective_tracker_data(tracker, results, waived_rules=None):
    if waived_rules is None: waived_rules = set()
    effective_tracker = []
    sorted_tracker = sorted(tracker.values(), key=lambda v: (v.get('Placement ID', ''), v.get('Event Type', '')))
    for v in sorted_tracker:
        v_copy = dict(v)
        spec_str = f"{v.get('Event Type', '')} | {v.get('Placement ID', '')}"
        matching_run = next((r for r in results if f"{r.get('ev_type', '')} | {r.get('plc_id', '')}" == spec_str), None)
        
        if matching_run and matching_run.get('checkpoint_result'):
            total_exp, passed_exp = 0, 0
            run_results = matching_run.get("log_data", {}).get("run_results", {})
            for vr_key, vr_val in run_results.items():
                for exp_result in vr_val.get("validation_result", {}).get("results", []):
                    total_exp += 1
                    if exp_result.get("success", False):
                        passed_exp += 1
                    else:
                        exp_config = exp_result.get("expectation_config", {})
                        exp_type = exp_config.get("expectation_type", "").replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")
                        if f"{exp_config.get('kwargs', {}).get('column', '-')} ({exp_type})" in waived_rules:
                            passed_exp += 1
            if total_exp > 0:
                v_copy["Pass/Fail"] = f"{passed_exp}/{total_exp} ({round(passed_exp / total_exp * 100)}%)"
                v_copy["Result"] = "✅ Passed" if passed_exp == total_exp else "❌ Mismatch"
            else:
                v_copy["Pass/Fail"] = "0/0 (0%)"
                v_copy["Result"] = "🚨 Error (No Rules)"
        elif matching_run and matching_run.get("status") == "ERROR":
            v_copy["Result"] = "🚨 Error"
        
        if v_copy["Result"] == "✅ Passed": v_copy["_status_class"] = "pass"
        elif v_copy["Result"] == "❌ Mismatch": v_copy["_status_class"] = "fail"
        elif "Error" in v_copy["Result"]: v_copy["_status_class"] = "error"
        else: v_copy["_status_class"] = "skip"
        
        effective_tracker.append(v_copy)
    return effective_tracker

def generate_batch_report_pdf(batch_data, waived_rules=None):
    """สร้าง PDF รวม: Summary Table + Data Docs ทุกตัว"""
    batch_id = batch_data.get("id", "unknown")[:8]
    spec_file = batch_data.get("spec_file", "batch")
    date_queried = batch_data.get("date_queried", "")
    timestamp = batch_data.get("timestamp", "")
    results = batch_data.get("results", [])
    tracker = batch_data.get("tracker", {})
    
    # === 1. สร้าง Summary HTML ===
    if waived_rules is None: waived_rules = set()
    success_count, fail_count, error_count = get_effective_metrics(results, waived_rules)
    
    tracker_rows = ""
    effective_tracker = get_effective_tracker_data(tracker, results, waived_rules)
    for v in effective_tracker:
        status_class = v.pop('_status_class', 'skip')
        tracker_rows += f"""<tr class="{status_class}">
            <td>{v.get('Event Type','')}</td>
            <td>{v.get('Placement ID','')}</td>
            <td>{v.get('Data Check','')}</td>
            <td>{v.get('AI','')}</td>
            <td>{v.get('Rules','')}</td>
            <td>{v.get('GX','')}</td>
            <td>{v.get('Pass/Fail','')}</td>
            <td>{v.get('Result','')}</td>
        </tr>"""
    
    # Sort results by plc_id first, then ev_type (group same placement together)
    sorted_results = sorted(results, key=lambda r: (r.get('plc_id', ''), r.get('ev_type', '')))
    
    details_html = ""
    for run_data in sorted_results:
        # สร้าง label ที่อ่านง่าย: event_type + placement details
        _ev = run_data.get('ev_type', '')
        _plc = run_data.get('plc_id', '')
        _plc_parts = str(_plc).split('/')
        _screen = _plc_parts[0] if len(_plc_parts) > 0 else ''
        _category = _plc_parts[1] if len(_plc_parts) > 1 else ''
        _target = _plc_parts[2] if len(_plc_parts) > 2 else ''
        
        _label_parts = []
        if _ev: _label_parts.append(f'<b>Event:</b> {_ev}')
        if _screen: _label_parts.append(f'<b>Screen:</b> {_screen}')
        if _category: _label_parts.append(f'<b>Category:</b> {_category}')
        if _target: _label_parts.append(f'<b>Target:</b> {_target}')
        placement_label = ' &nbsp;|&nbsp; '.join(_label_parts) if _label_parts else run_data.get('name', '')
        suite_ref = run_data.get('name', '')
        
        if run_data.get("status") == "ERROR":
            details_html += f"""<div class="spec-section">
                <h3>{placement_label}</h3>
                <p class="suite-ref">Suite: {suite_ref}</p>
                <p>❌ ERROR: {run_data.get('error','')}</p>
            </div>"""
            continue
        
        log_data = run_data.get("log_data", {})
        run_results = log_data.get("run_results", {})
        
        for vr_key, vr_val in run_results.items():
            validation = vr_val.get("validation_result", {})
            expectation_results = validation.get("results", [])
            
            if not expectation_results:
                details_html += f"""<div class="spec-section">
                <h3>{placement_label}</h3>
                <p class="suite-ref">Suite: {suite_ref}</p>
                <p>❌ ERROR: No validation rules were generated, or the suite was empty.</p>
            </div>"""
                continue
            all_rows = ""
            pass_count = 0
            fail_count_detail = 0
            c_waive_count = 0
            
            for exp_result in expectation_results:
                is_success = exp_result.get("success", False)
                exp_config = exp_result.get("expectation_config", {})
                exp_type = exp_config.get("expectation_type", "")
                kwargs = exp_config.get("kwargs", {})
                col = kwargs.get("column", "-")
                result_data = exp_result.get("result", {})
                
                short_type = exp_type.replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")
                
                extra_info = ""
                if "regex" in kwargs:
                    extra_info = f'regex: <code>{kwargs["regex"]}</code>'
                elif "value_set" in kwargs:
                    extra_info = f'set: {kwargs["value_set"]}'
                elif "column_set" in kwargs:
                    extra_info = f'{len(kwargs["column_set"])} columns, exact={kwargs.get("exact_match","?")}'
                elif "json_schema" in kwargs:
                    extra_info = f'schema: {kwargs["json_schema"]}'
                
                if is_success:
                    pass_count += 1
                    status_icon = "✅"
                    row_class = "row-pass"
                    detail_cell = f'<span class="pass-text">Pass</span>'
                    if extra_info:
                        detail_cell += f' — <span class="extra-info">{extra_info}</span>'
                else:
                    rule_key = f"{col} ({short_type})"
                    is_waived = rule_key in waived_rules
                    
                    if is_waived: c_waive_count += 1
                    else: fail_count_detail += 1
                    
                    status_icon = "⚠️" if is_waived else "❌"
                    row_class = "row-waived" if is_waived else "row-fail"
                    
                    parts = []
                    if is_waived:
                        parts.append('<b><span style="color:#d35400">[WAIVED]</span></b>')
                        
                    el_count = result_data.get("element_count", "")
                    unexp_count = result_data.get("unexpected_count", "")
                    unexp_pct = result_data.get("unexpected_percent", "")
                    
                    if unexp_pct != "":
                        parts.append(f'<b>{unexp_pct}% failed</b> ({unexp_count}/{el_count} rows)')
                    
                    observed = result_data.get("observed_value")
                    if observed and isinstance(observed, list):
                        parts.append(f'<b>Observed:</b> {", ".join(observed[:15])}')
                    
                    details = result_data.get("details", {})
                    mismatched = details.get("mismatched", {})
                    unexpected_cols = mismatched.get("unexpected", [])
                    if unexpected_cols:
                        parts.append(f'<b>Extra columns:</b> <span class="unexpected-val">{", ".join(unexpected_cols)}</span>')
                    
                    examples = result_data.get("partial_unexpected_list", [])
                    if examples:
                        unique_examples = list(dict.fromkeys([str(x) for x in examples[:8]]))
                        parts.append(f'<b>Unexpected values:</b> <span class="unexpected-val">{", ".join(unique_examples)}</span>')
                    
                    if extra_info:
                        parts.append(f'<b>Rule:</b> {extra_info}')
                    
                    detail_cell = "<br>".join(parts) if parts else "Failed"
                
                all_rows += f"""<tr class="{row_class}">
                    <td class="status-cell">{status_icon}</td>
                    <td class="col-cell">{col}</td>
                    <td class="exp-cell">{short_type}</td>
                    <td class="detail-cell">{detail_cell}</td>
                </tr>"""
            
            effective_success = (fail_count_detail == 0)
            status_label = "PASS ✅" if effective_success else "FAILURE ❌"
            
            total_exp = pass_count + fail_count_detail + c_waive_count
            summary_badges = f"✅ {pass_count} passed | ❌ {fail_count_detail} failed"
            if c_waive_count > 0: summary_badges += f" | ⚠️ {c_waive_count} waived"
            summary_badges += f" | Total: {total_exp} expectations"
            
            details_html += f"""<div class="spec-section">
                <h3>{placement_label} — {status_label}</h3>
                <p class="suite-ref">Suite: {suite_ref}</p>
                <p class="summary-line">{summary_badges}</p>
                <table class="detail-table"><thead><tr>
                    <th style="width:24px">⬤</th><th style="width:100px">Column</th><th style="width:100px">Expectation</th><th>Detail</th>
                </tr></thead>
                <tbody>{all_rows}</tbody></table>
            </div>"""
    
    summary_html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
  @page {{ size: A4; margin: 1.2cm; }}
  body {{ font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 10px; color: #2c3e50; }}
  h1 {{ color: #2c3e50; margin-bottom: 4px; font-size: 18px; }}
  h2 {{ font-size: 14px; margin-top: 20px; border-bottom: 1px solid #e2e8f0; padding-bottom: 4px; color: #34495e; }}
  .meta {{ color: #7f8c8d; font-size: 9px; margin-bottom: 12px; }}
  .metrics {{ display: flex; gap: 16px; margin-bottom: 20px; }}
  .metric {{ background: #f8f9fa; border-radius: 6px; padding: 12px 20px; text-align: center; flex: 1; border: 1px solid #e2e8f0; }}
  .metric .num {{ font-size: 22px; font-weight: bold; }}
  .metric.pass .num {{ color: #27ae60; }}
  .metric.fail .num {{ color: #c0392b; }}
  .metric.error .num {{ color: #d35400; }}
  .metric.total .num {{ color: #2980b9; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 16px; font-size: 9px; }}
  th {{ background: #34495e; color: white; padding: 5px 6px; text-align: left; font-size: 8px; font-weight: 600; }}
  td {{ padding: 4px 6px; border-bottom: 1px solid #ecf0f1; font-size: 8px; word-wrap: break-word; color: #2c3e50; }}
  tr.pass {{ background: #f9fbf9; }}
  tr.fail {{ background: #fdf5f5; }}
  tr.skip {{ background: #f8f9fa; }}
  .spec-section {{ margin-bottom: 20px; padding-bottom: 16px; border-bottom: 2px solid #ecf0f1; page-break-inside: avoid; }}
  .spec-section h3 {{ margin: 0 0 4px 0; font-size: 11px; color: #2c3e50; }}
  .summary-line {{ font-size: 9px; margin: 4px 0 8px 0; color: #7f8c8d; }}
  .suite-ref {{ font-size: 9px; color: #95a5a6; margin: 0 0 6px 0; font-style: italic; }}
  .detail-table {{ border: 1px solid #e2e8f0; width: 100%; border-collapse: collapse; }}
  .detail-table th {{ background: #7f8c8d; color: white; font-size: 7px; padding: 3px 4px; text-align: left; }}
  .detail-table td {{ border: 1px solid #ecf0f1; padding: 3px 4px; font-size: 7px; vertical-align: top; word-break: break-word; }}
  .row-pass {{ background: #fdfefe; }}
  .row-fail {{ background: #fdf5f5; }}
  .row-waived {{ background: #fff8e1; }}
  .status-cell {{ text-align: center; width: 24px; }}
  .col-cell {{ font-weight: bold; color: #34495e; }}
  .exp-cell {{ font-family: monospace; font-size: 7px; color: #7f8c8d; }}
  .detail-cell {{ font-size: 7px; line-height: 1.3; }}
  .pass-text {{ color: #27ae60; }}
  .extra-info {{ color: #95a5a6; }}
  .unexpected-val {{ color: #c0392b; font-family: monospace; word-break: break-all; }}
  .page-break {{ page-break-before: always; }}
  .failed-summary-table {{ width: 100%; border-collapse: collapse; margin-bottom: 16px; }}
  .failed-summary-table th {{ background: #c0392b; color: white; padding: 5px 6px; text-align: left; font-size: 8px; font-weight: 600; }}
  .failed-summary-table td {{ border-bottom: 1px solid #ecf0f1; padding: 4px 6px; font-size: 8px; vertical-align: top; word-break: break-word; }}
  .col-group-header {{ background: #f4f6f7; font-weight: bold; }}
  .col-group-header td {{ font-size: 9px; padding: 5px 6px; border-bottom: 2px solid #bdc3c7; color: #2c3e50; }}
</style></head><body>
<h1>Tracking Validation Report</h1>
<p class="meta">
  Spec File: <strong>{spec_file}</strong> | Date: <strong>{date_queried}</strong><br>
  Batch ID: <code>{batch_id}</code> | Generated: {timestamp}
</p>

<div class="metrics">
  <div class="metric pass"><div class="num">{success_count}</div><div>Pass</div></div>
  <div class="metric fail"><div class="num">{fail_count}</div><div>Fail</div></div>
  <div class="metric error"><div class="num">{error_count}</div><div>Error</div></div>
  <div class="metric total"><div class="num">{len(results)}</div><div>Total</div></div>
</div>

<h2>Summary</h2>
<table>
  <thead><tr><th>Event Type</th><th>Placement ID</th><th>Data Check</th><th>AI</th><th>Rules</th><th>GX</th><th>Pass/Fail</th><th>Result</th></tr></thead>
  <tbody>{tracker_rows}</tbody>
</table>

"""

    # --- 1.5. สรุป Placement ID ที่ไม่ผ่านใน PDF ---
    failed_summary_html = ""
    failed_items = extract_failed_items(sorted_results, waived_rules)
    
    if failed_items:
        # Step 1: Group by (col, exp_type) → each rule
        rule_groups = {}
        for item in failed_items:
            rk = (item["col"], item["exp_type"])
            if rk not in rule_groups:
                rule_groups[rk] = {"col": item["col"], "exp_type": item["exp_type"], "total_count": 0, "placements": set()}
            rule_groups[rk]["total_count"] += item["count"]
            rule_groups[rk]["placements"].add(item["placement"])
        
        # Step 2: Group rules by column → sum errors per column
        col_totals = {}
        for rk, rg in rule_groups.items():
            c = rg["col"]
            col_totals[c] = col_totals.get(c, 0) + rg["total_count"]
        
        # Step 3: Sort columns by total errors desc, then within each column sort rules by errors desc
        sorted_cols = sorted(col_totals.keys(), key=lambda c: col_totals[c], reverse=True)
        
        rank = 0
        failed_rows = ""
        for col_name in sorted_cols:
            col_total = col_totals[col_name]
            col_rules = [rg for rg in rule_groups.values() if rg["col"] == col_name]
            col_rules.sort(key=lambda x: x["total_count"], reverse=True)
            
            # Column group header row
            rank += 1
            failed_rows += f'<tr class="col-group-header"><td style="text-align:center;">{rank}</td><td colspan="2"><b>{col_name}</b></td><td style="text-align:center;color:#c0392b;"><b>{col_total}</b></td><td></td></tr>'
            
            # Individual rule rows under this column
            for rule in col_rules:
                placements_str = ", ".join(sorted(rule["placements"]))
                failed_rows += f'<tr class="fail"><td></td><td></td><td><span style="color:#7f8c8d;font-family:monospace;">{rule["exp_type"]}</span></td><td style="text-align:center;color:#c0392b;">{rule["total_count"]}</td><td style="font-size:7px;color:#7f8c8d;">{placements_str}</td></tr>'

        failed_summary_html = f"""
<h2 style="color: #c0392b;">🚨 Failed Rules Summary (Ranked by Column Errors)</h2>
<table class="failed-summary-table">
<thead><tr><th>Rank</th><th>Column</th><th>Rule</th><th>Errors</th><th>Impacted Placements</th></tr></thead>
<tbody>{failed_rows}</tbody>
</table>
"""
        
    waived_rules_html = ""
    if waived_rules:
        waived_rows = ""
        # Extract reasons/details from failed items logic to show in Waived Rules table
        waived_items_info = extract_failed_items(results, set()) # Get ALL failed
        waived_dict = {}
        for w in waived_items_info:
            rk = w["rule_key"]
            if rk in waived_rules:
                if rk not in waived_dict: 
                    waived_dict[rk] = {"count": 0, "placements": set(), "kwargs": w.get("kwargs", {}), "examples": w.get("examples", [])}
                else:
                    if not waived_dict[rk]["examples"] and w.get("examples"):
                        waived_dict[rk]["examples"] = w.get("examples")
                waived_dict[rk]["count"] += w["count"]
                waived_dict[rk]["placements"].add(w["placement"])
                
        for rule in sorted(list(waived_rules)):
            parts = rule.split(' (', 1)
            w_col = parts[0]
            w_exp = parts[1].rstrip(')') if len(parts) > 1 else ""
            
            info = waived_dict.get(rule, {"count": 0, "placements": set(), "kwargs": {}, "examples": []})
            plc_str = ", ".join(sorted(list(info["placements"])))
            
            kwargs = info["kwargs"]
            extra_info = ""
            if "regex" in kwargs: extra_info = f'regex: <code>{kwargs["regex"]}</code>'
            elif "value_set" in kwargs: extra_info = f'set: {kwargs["value_set"]}'
            elif "column_set" in kwargs: extra_info = f'{len(kwargs["column_set"])} cols'
            elif "json_schema" in kwargs: extra_info = f'schema'
            
            examples = info["examples"]
            ex_str = ""
            if examples: ex_str = f'<span style="color:#c0392b;">{str(examples[0])}</span>'
            
            detail_html = "-"
            if extra_info or ex_str:
                d_parts = []
                if extra_info: d_parts.append(f"<span style='font-size:8px;color:#7f8c8d;'>Expect: {extra_info}</span>")
                if ex_str: d_parts.append(f"<span style='font-size:8px;color:#7f8c8d;'>Ex: {ex_str}</span>")
                detail_html = "<br>".join(d_parts)
            
            waived_rows += f'<tr class="row-waived"><td><b>{w_col}</b></td><td><span style="color:#7f8c8d;font-family:monospace;">{w_exp}</span></td><td style="word-wrap: break-word; word-break: break-all; max-width: 250px;">{detail_html}</td><td style="text-align:center;color:#d35400;">{info["count"]}</td><td style="font-size:7px;color:#7f8c8d;">{plc_str}</td></tr>'
            
        waived_rules_html = f"""
<h2 style="color: #d35400;">🛑 Waived Rules</h2>
<table class="failed-summary-table" style="margin-bottom: 20px;">
    <thead><tr><th style="width: 20%;">Column</th><th style="width: 20%;">Expectation Rule</th><th style="width: 25%;">Details</th><th style="width: 10%;">Errors Hidden</th><th>Impacted Placements</th></tr></thead>
    <tbody>{waived_rows}</tbody>
</table>
"""

    summary_html += failed_summary_html + waived_rules_html + f"""
<h2>Validation Details</h2>
{details_html}
</body></html>"""

    # === 2. สร้าง PDF (เฉพาะ Summary + Validation Details) ===
    summary_pdf_bytes = weasyprint.HTML(string=summary_html).write_pdf()
    
    # === 3. Save PDF ===
    filename = f"report_{spec_file.replace(' ', '_')}_{date_queried}_{batch_id}.pdf"
    filepath = os.path.join(REPORTS_DIR, filename)
    with open(filepath, "wb") as f:
        f.write(summary_pdf_bytes)
    
    return filepath


# --- 2. UI Rendering Functions ---

def open_report_in_browser(path):
    if path: webbrowser.open(path)

def display_run_results(run_data):
    st.header(f"📊 Validation Results for: {run_data['name']}")
    st.caption(f"Run ID: `{run_data['id']}` | Timestamp: `{run_data['timestamp']}`")

    docs_info = run_data.get("docs") or {}
    local_path = docs_info.get("local_path")
    segment_docs_urls = docs_info.get("segments", {})

    st.subheader("📄 Data Docs Reports")
    if local_path:
        reports = []
        reports.append({"label": "📈 Open Overall Report", "path": local_path, "type": "primary", "key_suffix": "overall"})
        for segment_name, report_url in sorted(segment_docs_urls.items()):
            reports.append({"label": f"📑 Report for '{segment_name}'", "path": report_url, "type": "secondary", "key_suffix": segment_name})
        
        num_buttons = len(reports)
        if num_buttons > 0:
            col_specs = [1] * num_buttons + [10 - num_buttons]
            cols = st.columns(col_specs)
            for i, report in enumerate(reports):
                with cols[i]:
                    st.button(report["label"], on_click=open_report_in_browser, args=[report["path"]], type=report["type"], key=f"button_{run_data['id']}_{report['key_suffix']}")
    else:
        st.warning("No report path was generated for this run. Please check the validation process log for errors.")
    st.markdown("---") 

    if run_data.get("run_name"):
        st.subheader("🔍 Run Identifier")
        st.text(f"Great Expectations Run Name: {run_data['run_name']}")

    if run_data['status'] == "SUCCESS":
        st.success("🎉 **Overall Status: SUCCESS**")
    else:
        st.error(f"🚨 **Overall Status: {run_data['status']}**")

    if run_data.get("ai_prompt"):
        with st.expander("👀 แสดง Prompt ที่ส่งให้ AI ในรอบนี้"):
            st.text(run_data["ai_prompt"])

    if run_data.get("validation_process_log"):
        with st.expander("Show Validation Process Log"):
            for log_entry in run_data["validation_process_log"]:
                st.markdown(log_entry, unsafe_allow_html=True)

    if run_data.get('checkpoint_result'):
        display_validation_errors(run_data['checkpoint_result'])

    if run_data.get('data_samples'):
        st.header("🔬 Data Samples")
        for segment_name, sample_df in run_data['data_samples'].items():
            with st.expander(f"Data for Segment: {segment_name}"):
                st.dataframe(sample_df, use_container_width=True)

    with st.expander("View Full JSON Result (Log)"):
        st.json(run_data['log_data'])

def manual_ui():
    st.title("🔩 Manual Data Validation")
    st.info("Configure SQL and Expectation rules manually.")
    st.header("📅 Validation Parameters")
    c1, c2 = st.columns(2)
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    db_query_date = c1.text_input("Query Date (YYYYMMDD)", value=yesterday.strftime('%Y%m%d'))
    segment_by_columns_str = c2.text_input("Columns to Segment By", help="Comma-separated. Leave empty to validate all data as one batch.")
    SEGMENT_BY_COLUMNS = [col.strip() for col in segment_by_columns_str.split(',') if col.strip()]
    val_params = (db_query_date, SEGMENT_BY_COLUMNS)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📄 SQL Query")
        sql_files = list_files(SQL_DIR, "sql")
        sql_file_options = ["--- Create New ---"] + sql_files
        selected_sql_file = st.selectbox("Select existing SQL file or create new:", sql_file_options, key="manual_sql_selector")
        sql_content = ""
        if selected_sql_file != "--- Create New ---":
            with open(os.path.join(SQL_DIR, selected_sql_file), 'r', encoding='utf-8') as f: sql_content = f.read()
        manual_sql_input = st.text_area("SQL Query Template:", value=sql_content, height=350, key="manual_sql_text_area")
        if selected_sql_file == "--- Create New ---":
            new_sql_filename = st.text_input("New SQL Filename:", key="manual_new_sql_name", placeholder="my_query.sql")
            if st.button("Save New SQL File", key="manual_save_sql"):
                if new_sql_filename and new_sql_filename.endswith(".sql"):
                    with open(os.path.join(SQL_DIR, new_sql_filename), 'w', encoding='utf-8') as f: f.write(manual_sql_input)
                    st.success(f"Saved file '{new_sql_filename}'!"); st.rerun()
                else: st.error("Please provide a filename ending with .sql")
    with col2:
        st.subheader("Expectation Rules (JSON)")
        exp_files = list_files(EXPECTATIONS_DIR, "json")
        exp_file_options = ["--- Create New ---"] + exp_files
        selected_exp_file = st.selectbox("Select existing Expectation file or create new:", exp_file_options, key="manual_exp_selector")
        exp_content = ""
        if selected_exp_file != "--- Create New ---":
            with open(os.path.join(EXPECTATIONS_DIR, selected_exp_file), 'r', encoding='utf-8') as f: exp_content = f.read()
        manual_expectations_input = st.text_area("Expectations JSON:", value=exp_content, height=350, key="manual_exp_text_area")
        if selected_exp_file == "--- Create New ---":
            new_exp_filename = st.text_input("New Expectation Filename:", key="manual_new_exp_name", placeholder="my_rules.json")
            if st.button("Save New Expectation File", key="manual_save_exp"):
                if new_exp_filename and new_exp_filename.endswith(".json"):
                    try:
                        json.loads(manual_expectations_input)
                        with open(os.path.join(EXPECTATIONS_DIR, new_exp_filename), 'w', encoding='utf-8') as f: f.write(manual_expectations_input)
                        st.success(f"Saved file '{new_exp_filename}'!"); st.rerun()
                    except json.JSONDecodeError: st.error("The content is not valid JSON.")
                else: st.error("Please provide a filename ending with .json")
    st.markdown("---")
    if st.button("🚀 Start Validation (Manual)", type="primary"):
        if SEGMENT_BY_COLUMNS and "{extra_where_conditions}" not in manual_sql_input:
            st.error("Segmentation is enabled, but the required '{extra_where_conditions}' placeholder is missing in your SQL query."); st.stop()
        suite_name = os.path.splitext(selected_exp_file)[0] if selected_exp_file != "--- Create New ---" else "manual_suite"
        checkpoint_result, data_samples, docs_info, process_log, run_name, docs_error = run_validation_process(manual_sql_input, manual_expectations_input, suite_name, DB_CONFIG, val_params)
        run_id = str(uuid.uuid4())
        run_data = {
            "id": run_id, "name": suite_name, "timestamp": datetime.datetime.now().isoformat(),
            "status": "SUCCESS" if checkpoint_result and checkpoint_result.success else "FAILURE",
            "run_name": run_name,
            "checkpoint_result": checkpoint_result, "data_samples": data_samples, "docs": docs_info,
            "log_data": checkpoint_result.to_json_dict() if checkpoint_result else {"error": "Process failed"},
            "validation_process_log": process_log,
            "docs_error": docs_error
        }
        # ✅ [ UPLOAD MODE ]: Set log filepath with descriptive name
        safe_ev = str(run_data.get('ev_type', 'upload')).replace('/', '_')
        safe_plc = str(run_data.get('plc_id', 'manual')).replace('/', '_')
        run_short_id = run_data.get('id', 'unknown')[:8]
        log_filename = f"run_upload_{safe_ev}_{safe_plc}_{run_short_id}.json"
        log_filepath = os.path.join(LOGS_DIR, log_filename)
        run_data["log_filepath"] = log_filepath # Save the path to the run_data so it can be updated by Waive Rules
        with open(log_filepath, 'w', encoding='utf-8') as f: json.dump(run_data['log_data'], f, indent=4)
        st.session_state.history.insert(0, run_data)
        st.session_state.active_run_id = run_id
        st.rerun()

def bulk_validation_ui():
    """Renders the Bulk Validation Dashboard UI."""
    st.title("Tracking Validation Dashboard")
    st.caption("Bulk Spec Validation — อัปโหลด Spec → เลือก → รัน → ดูผล")

    # ============================================================
    # Stage 1: Upload Spec
    # ============================================================
    if st.session_state.stage in ["awaiting_spec", "awaiting_config"] and not st.session_state.get("full_df_uploaded"):
        st.markdown("### 📁 Step 1: อัปโหลด Data Spec")
        uploaded_file = st.file_uploader("ลากไฟล์ CSV หรือ Excel มาวางที่นี่", type=["csv", "xlsx"], label_visibility="collapsed")
        
        if uploaded_file is not None:
            if st.button("📥 ประมวลผลไฟล์", type="primary"):
                with st.spinner("กำลังแยก Spec ตาม Event Type และ Placement ID..."):
                    try:
                        if uploaded_file.name.endswith('.csv'): df = pd.read_csv(uploaded_file)
                        else: df = pd.read_excel(uploaded_file)
                        
                        df.dropna(how='all', inplace=True)
                        df.dropna(axis=1, how='all', inplace=True)
                        
                        if 'event_type' in df.columns: df.rename(columns={'event_type': 'event type'}, inplace=True)
                        if 'placement id' in df.columns: df.rename(columns={'placement id': 'placement_id'}, inplace=True)
                        
                        if 'event type' not in df.columns or 'placement_id' not in df.columns:
                            st.error("❌ ไฟล์ต้องมีคอลัมน์ 'event type' และ 'placement_id'")
                            st.stop()
                        
                        rename_map = {'M': 'Mandatory', 'Data type': 'Type', 'value': 'Allowed Values', 'key': 'Field Name'}
                        df.rename(columns=rename_map, inplace=True)
                        
                        unique_specs = df[['event type', 'placement_id']].drop_duplicates().dropna().to_dict('records')
                        
                        st.session_state.full_df = df
                        st.session_state.unique_specs = unique_specs
                        st.session_state.full_df_uploaded = True
                        st.session_state.uploaded_filename = uploaded_file.name
                        st.session_state.stage = "awaiting_config"
                        st.rerun()
                    except Exception as e:
                        st.error(f"เกิดข้อผิดพลาดในการอ่านไฟล์: {e}")

    # ============================================================
    # Stage 2: Configuration
    # ============================================================
    if st.session_state.stage == "awaiting_config" and st.session_state.get("full_df_uploaded"):
        st.markdown(f"### ⚙️ Step 2: ตั้งค่าและเลือก Specs")
        st.info(f"📄 ไฟล์: **{st.session_state.get('uploaded_filename', 'uploaded')}** — พบ **{len(st.session_state.unique_specs)}** คู่ Event/Placement")
        
        spec_options = [f"{s['event type']} | {s['placement_id']}" for s in st.session_state.unique_specs]
        selected_spec_strings = st.multiselect("📌 เลือก Specs ที่ต้องการรัน:", options=spec_options, default=spec_options)
        
        col1, col2 = st.columns(2)
        with col1:
            yesterday = datetime.date.today() - datetime.timedelta(days=1)
            db_query_date = st.text_input("📅 Query Date (YYYYMMDD)", value=yesterday.strftime('%Y%m%d'))
        with col2:
            segment_by_columns_str = st.text_input("🔀 Segment By (comma-separated)", value="", help="ใส่ชื่อคอลัมน์ที่จะแบ่ง segment")
        
        sql_source_options = ["Write Custom SQL Manually", "Select Existing SQL File"]
        sql_source = st.radio("🗄️ SQL Source", sql_source_options, horizontal=True)

        if sql_source == "Select Existing SQL File":
            sql_files = [""] + list_files(SQL_DIR, "sql")
            selected_manual_sql = st.selectbox("เลือกไฟล์ SQL:", sql_files)
            custom_sql_input = ""
        else: 
            selected_manual_sql = ""
            custom_sql_input = st.text_area("SQL Query:", value="SELECT * FROM my_database.raw_events WHERE dt = '{dt}'", height=120)
        
        if st.button("🚀 เริ่มรัน Validation", type="primary", use_container_width=True):
            if not selected_spec_strings:
                st.error("กรุณาเลือก Spec อย่างน้อย 1 รายการ")
                st.stop()
                
            st.session_state.val_params = {"date": db_query_date, "segments": [col.strip() for col in segment_by_columns_str.split(',') if col.strip()]}
            st.session_state.sql_config = {"source": sql_source, "file": selected_manual_sql, "custom_sql": custom_sql_input}
            st.session_state.selected_spec_strings = selected_spec_strings
            st.session_state.stage = "processing_pipeline"
            st.rerun()

    # ============================================================
    # Stage 3: Processing Pipeline (3-stage: DB → AI → GX)
    # ============================================================
    if st.session_state.stage == "processing_pipeline":
        
        # Initialize background threads (one-time)
        if "pipeline_init" not in st.session_state:
            st.session_state.pipeline_init = True
            
            # สร้าง Batch ID สั้นๆ สำหรับ run ทั้ง batch
            batch_id = str(uuid.uuid4())[:8]
            
            initial_tracker = {}
            for spec in st.session_state.selected_spec_strings:
                ev, plc = spec.split(" | ")
                initial_tracker[spec] = {
                    "Event Type": ev,
                    "Placement ID": plc,
                    "Data Check": "⏳ Queued",
                    "AI": "-",
                    "Rules": "-",
                    "GX": "-",
                    "Pass/Fail": "-",
                    "Result": "-",
                    "Docs": ""
                }

            st.session_state.shared_state = {
                "queue": [],
                "ai_queue": [],
                "status_text": "กำลังเตรียมเชื่อมต่อ Database...",
                "is_done": False,
                "ai_is_done": False,
                "checked_count": 0,
                "found_count": 0,
                "tracker": initial_tracker,
                "batch_id": batch_id,
                "gx_results": [],
                "gx_active_count": 0,
                "retry_count": {},
                "last_gx_error": {}
            }
            st.session_state.overall_results = []
            
            # ===== Thread 1: DB Checker =====
            def db_checker_worker(specs, sql_cfg, val_params, db_config, shared):
                trino_host, trino_port, trino_username, trino_password = db_config
                
                base_sql = ""
                if sql_cfg["source"] == "Select Existing SQL File":
                    with open(os.path.join(SQL_DIR, sql_cfg["file"]), 'r', encoding='utf-8') as f: base_sql = f.read()
                else:
                    base_sql = sql_cfg["custom_sql"]

                try:
                    engine = create_engine(f'trino://{trino_username}:{urllib.parse.quote(trino_password)}@{trino_host}:{trino_port}/', connect_args={'http_scheme': 'https'})
                except Exception as e:
                    shared["status_text"] = f"🚨 DB Connection Error: {e}"
                    shared["is_done"] = True
                    return

                for spec_str in specs:
                    ev_type, plc_id = spec_str.split(" | ")
                    shared["status_text"] = f"🔍 กำลังหาข้อมูล: `{ev_type}` ({plc_id})"
                    shared["tracker"][spec_str]["Data Check"] = "🔄 เช็ค DB..."
                    
                    final_sql = prepare_spec_sql(base_sql, ev_type, plc_id)
                    formatted_sql = final_sql.format(dt=val_params["date"], extra_where_conditions="")
                    check_sql = f"SELECT 1 FROM ({formatted_sql}) AS check_query LIMIT 1"
                    
                    # Debug: บันทึก SQL ที่ generate ออกมาเป็นไฟล์
                    safe_name = f"{ev_type}_{plc_id}".replace("/", "_").replace("\\", "_").replace(" ", "_")
                    sql_log_path = os.path.join(LOGS_DIR, f"gen_sql_{safe_name}.sql")
                    with open(sql_log_path, 'w', encoding='utf-8') as sql_f:
                        sql_f.write(f"-- event_type: {ev_type}\n")
                        sql_f.write(f"-- placement_id: {plc_id}\n")
                        sql_f.write(f"-- check_sql:\n{check_sql}\n\n")
                        sql_f.write(f"-- formatted_sql:\n{formatted_sql}\n")
                    
                    try:
                        with engine.connect() as conn:
                            res = conn.execute(text(check_sql)).fetchone()
                            has_data = bool(res)
                    except Exception as e:
                        print(f"[DB Check] ❌ ERROR for {ev_type}/{plc_id}: {e}")
                        has_data = False
                    
                    shared["checked_count"] += 1
                    if has_data:
                        shared["found_count"] += 1
                        shared["queue"].append(spec_str)
                        shared["tracker"][spec_str]["Data Check"] = "✅ Found"
                        shared["tracker"][spec_str]["AI"] = "⏳ Queued"
                    else:
                        shared["tracker"][spec_str]["Data Check"] = "⏭️ No Data"
                        shared["tracker"][spec_str]["Result"] = "⏭️ Skipped"
                        
                    time.sleep(0.1) 
                
                shared["status_text"] = f"✅ สแกน DB ครบแล้ว พบข้อมูล {shared['found_count']}/{len(specs)} รายการ"
                shared["is_done"] = True

            # ===== Thread 2: AI Worker =====
            def ai_worker(shared, full_df):
                while True:
                    if shared["queue"]:
                        spec_str = shared["queue"].pop(0)
                        ev_type, plc_id = spec_str.split(" | ")
                        suite_name = f"suite_{ev_type}_{plc_id}".replace(" ", "_").replace("/", "_").replace("\\", "_")
                        shared["tracker"][spec_str]["AI"] = "🔄 กำลังสร้าง..."
                        
                        try:
                            df_filtered = full_df[(full_df['event type'] == ev_type) & (full_df['placement_id'] == plc_id)].copy()
                            
                            # สร้าง flatten_map จาก Spec
                            flatten_map = {}
                            if 'group' in df_filtered.columns:
                                for _, row in df_filtered.iterrows():
                                    grp = row.get('group')
                                    field = row.get('Field Name')
                                    if pd.notna(grp) and str(grp).strip() and pd.notna(field):
                                        g = str(grp).strip()
                                        f = str(field).strip()
                                        if g not in flatten_map:
                                            flatten_map[g] = []
                                        if f not in flatten_map[g]:
                                            flatten_map[g].append(f)
                            
                            if 'group' in df_filtered.columns:
                                df_filtered['Field Name'] = df_filtered.apply(lambda row: f"{row['group']}_{row['Field Name']}" if pd.notna(row['group']) and str(row['group']).strip() != "" else row['Field Name'], axis=1)
                            
                            important_cols = ['Field Name', 'Type', 'Mandatory', 'Allowed Values']
                            cols_to_keep = [col for col in important_cols if col in df_filtered.columns]
                            spec_md = df_filtered[cols_to_keep].to_markdown(index=False)
                            
                            with open("prompt_template.txt", "r", encoding="utf-8") as f: prompt_template = f.read()
                            prompt = prompt_template.format(auto_spec_input=spec_md)
                            
                            model_name = os.getenv('GEMINI_MODEL_ID', 'gemini-2.5-flash')
                            model = genai.GenerativeModel(model_name)
                            
                            retries = shared.get("retry_count", {}).get(spec_str, 0)
                            if retries > 0:
                                last_err = shared.get("last_gx_error", {}).get(spec_str, "Unknown Error")
                                prompt += f"\n\n🚨 [SYSTEM WARNING]: Your previous JSON generated a Great Expectations Exception:\n{last_err}\n\nPlease fix your JSON (e.g. check parameter usage or column existence) so it runs successfully."
                            
                            # === Retry Logic + Exponential Backoff ===
                            max_retries = 5
                            base_delay = 5 # วิ
                            response = None
                            
                            for attempt in range(max_retries):
                                try:
                                    response = model.generate_content(prompt)
                                    break
                                except Exception as e:
                                    err_str = str(e)
                                    if "429" in err_str or "ResourceExhausted" in err_str or "quota" in err_str.lower():
                                        if attempt < max_retries - 1:
                                            wait_time = base_delay * (2 ** attempt)
                                            shared["tracker"][spec_str]["AI"] = f"⏳ โดน Limit... รอ {wait_time}s"
                                            time.sleep(wait_time)
                                            shared["tracker"][spec_str]["AI"] = "🔄 กำลังสร้าง..."
                                        else:
                                            raise e
                                    else:
                                        raise e
                            # ========================================

                            log_md_filename = os.path.join(LOGS_DIR, f"ai_log_{suite_name}_{int(time.time())}.md")
                            try:
                                match = re.search(r'\[.*\]', response.text, re.DOTALL)
                                if match:
                                    final_expectations = match.group(0)
                                    try:
                                        parsed_json = json.loads(final_expectations)
                                    except json.JSONDecodeError as je:
                                        if "escape" in str(je).lower() or "\\\\s" in final_expectations:
                                            # AI ใส่ \s, \d, \w ใน regex โดยไม่ escape เป็น \\s, \\d, \\w
                                            fixed = re.sub(r'(?<!\\)\\([sdwSDWbB])', r'\\\\\\1', final_expectations)
                                            parsed_json = json.loads(fixed)
                                            final_expectations = fixed
                                        else:
                                            raise
                                    
                                    exp_filepath = os.path.join(EXPECTATIONS_DIR, f"{suite_name}.json")
                                    with open(exp_filepath, 'w', encoding='utf-8') as exp_f:
                                        json.dump(parsed_json, exp_f, indent=4, ensure_ascii=False)
                                    
                                    shared["tracker"][spec_str]["AI"] = "✅ Done"
                                    shared["tracker"][spec_str]["Rules"] = f"{len(parsed_json)}"
                                    shared["tracker"][spec_str]["GX"] = "⏳ Queued"
                                    
                                    shared["ai_queue"].append({
                                        "spec_str": spec_str,
                                        "ev_type": ev_type,
                                        "plc_id": plc_id,
                                        "suite_name": suite_name,
                                        "expectations": final_expectations,
                                        "flatten_map": flatten_map,
                                        "prompt": prompt,
                                        "ai_response": response.text if response else 'No response',
                                        "rule_count": len(parsed_json)
                                    })
                                else:
                                    shared["tracker"][spec_str]["AI"] = "🚨 Error"
                                    shared["tracker"][spec_str]["Result"] = "🚨 AI ไม่ตอบ JSON"
                                
                                # Save success log
                                with open(log_md_filename, "w", encoding="utf-8") as log_f:
                                    log_resp = response.text if (response and hasattr(response, 'text')) else 'No response'
                                    log_f.write(f"# AI Generation Log\n\n**Suite:** `{suite_name}`\n\n## Prompt\n```text\n{prompt}\n```\n\n## Response\n```json\n{log_resp}\n```\n")

                            except Exception as e:
                                import traceback
                                print("=== AI Worker Error ===")
                                traceback.print_exc()
                                print("=======================")
                                err_name = type(e).__name__
                                err_msg = str(e)[:30] + "..." if len(str(e)) > 30 else str(e)
                                shared["tracker"][spec_str]["AI"] = f"🚨 {err_name}"
                                shared["tracker"][spec_str]["Result"] = f"🚨 Error: {err_msg}"
                                
                                # Save error log
                                with open(log_md_filename, "w", encoding="utf-8") as log_f:
                                    log_resp = response.text if (response and hasattr(response, 'text')) else (str(err_str) if 'err_str' in locals() else 'No response/Unknown error')
                                    log_f.write(f"# AI Generation Error Log\n\n**Suite:** `{suite_name}`\n\n**Error:** {str(e)}\n\n## Prompt\n```text\n{prompt}\n```\n\n## Original Response (Raw)\n```text\n{log_resp}\n```\n")
                        except Exception as e:
                            import traceback
                            print("=== AI Worker Outer Error ===")
                            traceback.print_exc()
                            print("=============================")
                            err_name = type(e).__name__
                            err_msg = str(e)[:30] + "..." if len(str(e)) > 30 else str(e)
                            shared["tracker"][spec_str]["AI"] = f"🚨 {err_name}"
                            shared["tracker"][spec_str]["Result"] = f"🚨 Error: {err_msg}"
                            # Save error log for outer errors (API, retry, etc.)
                            try:
                                log_md_filename = os.path.join(LOGS_DIR, f"ai_log_{suite_name}_{int(time.time())}.md")
                                with open(log_md_filename, "w", encoding="utf-8") as log_f:
                                    log_f.write(f"# AI Generation Error Log\n\n**Suite:** `{suite_name}`\n\n**Error:** {str(e)}\n\n## Prompt\n```text\n{prompt if 'prompt' in locals() else 'N/A'}\n```\n")
                            except: pass
                        
                        time.sleep(0.1)
                    elif shared["is_done"] and len(shared["queue"]) == 0 and len(shared["ai_queue"]) == 0 and shared.get("gx_active_count", 0) == 0:
                        break
                    else:
                        time.sleep(0.5)
                
                shared["ai_is_done"] = True

            # ===== Thread 3: GX Worker =====
            def gx_worker(shared, val_params_tuple, sql_cfg, db_config):
                while True:
                    item = None
                    try:
                        if shared["ai_queue"]:
                            item = shared["ai_queue"].pop(0)
                        
                        if item:
                            shared["gx_active_count"] += 1
                            spec_str = item["spec_str"]
                            ev_type = item["ev_type"]
                            plc_id = item["plc_id"]
                            suite_name = item["suite_name"]
                            final_expectations = item["expectations"]
                            flatten_map = item["flatten_map"]
                            prompt = item["prompt"]
                            
                            shared["tracker"][spec_str]["GX"] = "🔄 รันตรวจ Data..."
                            
                            base_sql = ""
                            if sql_cfg["source"] == "Select Existing SQL File":
                                with open(os.path.join(SQL_DIR, sql_cfg["file"]), 'r', encoding='utf-8') as f: base_sql = f.read()
                            else:
                                base_sql = sql_cfg["custom_sql"]
                            final_sql = prepare_spec_sql(base_sql, ev_type, plc_id)

                            checkpoint_result, data_samples, docs_info, process_log, run_name, docs_error = run_validation_process(
                                final_sql, final_expectations, suite_name, db_config, val_params_tuple,
                                flatten_map=flatten_map, batch_id=shared.get("batch_id"), background=True
                            )
                            
                            run_id = str(uuid.uuid4())
                            _docs_segs = (docs_info or {}).get("segments", {})
                            best_docs_url = list(_docs_segs.values())[0] if _docs_segs else (docs_info or {}).get("local_path", "")
                            
                            run_data = {
                                "id": run_id, "name": suite_name, "timestamp": datetime.datetime.now().isoformat(),
                                "ev_type": ev_type, "plc_id": plc_id,
                                "status": "SUCCESS" if checkpoint_result and checkpoint_result.success else ("ERROR" if checkpoint_result is None else "FAILURE"),
                                "run_name": run_name, "batch_id": shared.get("batch_id", ""),
                                "checkpoint_result": checkpoint_result, "data_samples": data_samples, "docs": docs_info,
                                "docs_url": best_docs_url,
                                "log_data": checkpoint_result.to_json_dict() if checkpoint_result else {"error": "Process failed or returned empty data"},
                                "validation_process_log": process_log, "docs_error": docs_error,
                                "ai_prompt": prompt, "ai_response": item.get("ai_response", ""),
                                "spec_str": spec_str
                            }
                            shared["gx_results"].append(run_data)
                            shared["gx_active_count"] -= 1

                        elif shared["ai_is_done"]:
                            break
                        else:
                            time.sleep(1)
                            
                    except Exception as e:
                        if item:
                            spec_str = item["spec_str"]
                            retries = shared["retry_count"].get(spec_str, 0)
                            if retries < 1:
                                shared["retry_count"][spec_str] = retries + 1
                                shared["last_gx_error"][spec_str] = f"{type(e).__name__}: {str(e)}"
                                shared["tracker"][spec_str]["GX"] = "🚨 Error (Retrying AI...)"
                                shared["queue"].insert(0, spec_str)
                            else:
                                tb_str = traceback.format_exc()
                                error_msg = f"{type(e).__name__}: {str(e)}"
                                shared["tracker"][spec_str]["GX"] = "🚨 Error"
                                shared["tracker"][spec_str]["Result"] = f"🚨 Error: {type(e).__name__}"
                                
                                error_run_id = str(uuid.uuid4())
                                error_log_data = {
                                    "status": "ERROR", "error_message": error_msg, "traceback": tb_str, "failed_at_suite": suite_name
                                }
                                # ✅ [ AUTOMATE ERROR ]: Descriptive naming
                                safe_ev = str(spec.get('event type', 'error')).replace('/', '_')
                                safe_plc = str(spec.get('placement_id', 'unknown')).replace('/', '_')
                                error_short_id = error_run_id[:8]
                                batch_id_disp = spec.get('batch_id', 'nobatch')
                                log_filename = f"run_{batch_id_disp}_{safe_ev}_{safe_plc}_{error_short_id}.json"
                                log_filepath = os.path.join(LOGS_DIR, log_filename)
                                error_log_data["log_filepath"] = log_filepath
                                with open(log_filepath, 'w', encoding='utf-8') as f: json.dump(error_log_data, f, indent=4)
                                
                                run_data = {
                                    "id": error_run_id, "name": suite_name,
                                    "ev_type": ev_type, "plc_id": plc_id,
                                    "status": "ERROR", "error": error_msg, "traceback": tb_str,
                                    "spec_str": spec_str, "validation_process_log": [f"🚨 Exception: {error_msg}"]
                                }
                                shared["gx_results"].append(run_data)
                            shared["gx_active_count"] -= 1
                        time.sleep(1)

            # เริ่มสั่ง Thread หลังบ้าน (DB Checker, AI, และ 3 GX)
            t1 = threading.Thread(target=db_checker_worker, args=(
                st.session_state.selected_spec_strings, 
                st.session_state.sql_config, 
                st.session_state.val_params, 
                DB_CONFIG,
                st.session_state.shared_state 
            ))
            t2 = threading.Thread(target=ai_worker, args=(
                st.session_state.shared_state,
                st.session_state.full_df.copy()
            ))
            t1.start()
            t2.start()
            for _ in range(3):
                threading.Thread(target=gx_worker, args=(
                    st.session_state.shared_state,
                    (st.session_state.val_params["date"], st.session_state.val_params["segments"]),
                    st.session_state.sql_config,
                    DB_CONFIG
                )).start()
            st.rerun()

        # =======================================================
        # Main Thread: UI Render + GX Processing (Consumer)
        # =======================================================
        st.markdown(f"### 🚀 Pipeline ({len(st.session_state.selected_spec_strings)} คิว)")
        
        shared = st.session_state.shared_state 
        
        processing_active = not shared["is_done"] or shared["gx_active_count"] > 0 or len(shared["gx_results"]) > 0
        
        if processing_active:
            st.spinner("⏳ กำลังตรวจสอบคุณภาพข้อมูล... กรุณารอจนกว่าจะเสร็จสิ้น")
        
        # Status bar
        if not shared["is_done"]:
            st.info(f"**DB Checker:** {shared['status_text']} | พบข้อมูลแล้ว: **{shared['found_count']}**")
        else:
            st.success(f"**DB Checker:** {shared['status_text']}")
            
        # Live Tracker Table
        status_df = pd.DataFrame(list(shared["tracker"].values()))
        display_cols = [c for c in status_df.columns if c != "Docs"]
        st.dataframe(status_df[display_cols], use_container_width=True, hide_index=True)
        
        # Data Docs buttons (file:// URLs ต้องเปิดผ่าน webbrowser)
        docs_buttons = [(k, v.get("Docs", "")) for k, v in shared["tracker"].items() if v.get("Docs")]
        if docs_buttons:
            st.markdown("**📄 Data Docs:**")
            cols = st.columns(min(len(docs_buttons), 4))
            for i, (spec_str, docs_url) in enumerate(docs_buttons):
                with cols[i % min(len(docs_buttons), 4)]:
                    ev, plc = spec_str.split(" | ")
                    st.button(f"📄 {ev} / {plc}", on_click=open_report_in_browser, args=[docs_url], key=f"tracker_docs_{i}", use_container_width=True)

        # ดึงผลลัพธ์จาก GX Worker ใน GX Results Queue มาอัพเดท UI
        if len(shared["gx_results"]) > 0:
            run_data = shared["gx_results"].pop(0)
            spec_str = run_data.get("spec_str")
            
            if "log_data" in run_data:
                # ✅ [ AUTOMATE SUCCESS ]: Descriptive naming
                safe_ev = str(run_data.get('ev_type', 'unknown')).replace('/', '_')
                safe_plc = str(run_data.get('plc_id', 'unknown')).replace('/', '_')
                run_short_id = run_data.get('id', 'unknown')[:8]
                batch_id_disp = run_data.get('batch_id', 'nobatch')
                log_filename = f"run_{batch_id_disp}_{safe_ev}_{safe_plc}_{run_short_id}.json"
                log_filepath = os.path.join(LOGS_DIR, log_filename)
                
                run_data["log_filepath"] = log_filepath # Save it so Waive Rules UI can find it later
                with open(log_filepath, 'w', encoding='utf-8') as f: json.dump(run_data['log_data'], f, indent=4)
            
            st.session_state.history.insert(0, run_data)
            st.session_state.overall_results.append(run_data)

            if run_data.get('status') == "ERROR":
                pass # Already set in worker if Exception, otherwise set below
            else:
                checkpoint_result = run_data.get('checkpoint_result')
                if checkpoint_result:
                    total_expectations = 0
                    passed_expectations = 0
                    for _, vr_dict in checkpoint_result.run_results.items():
                        for result in vr_dict['validation_result'].results:
                            total_expectations += 1
                            if result.success:
                                passed_expectations += 1
                    pct = round(passed_expectations / total_expectations * 100) if total_expectations > 0 else 0
                    shared["tracker"][spec_str]["Pass/Fail"] = f"{passed_expectations}/{total_expectations} ({pct}%)"
                    shared["tracker"][spec_str]["GX"] = "✅ Done"
                else:
                    shared["tracker"][spec_str]["GX"] = "🚨 Error"

            docs_url = run_data.get("docs_url")
            if docs_url:
                shared["tracker"][spec_str]["Docs"] = docs_url
            
            if run_data['status'] == "SUCCESS":
                shared["tracker"][spec_str]["Result"] = "✅ Passed"
            elif run_data['status'] == "ERROR":
                shared["tracker"][spec_str]["Result"] = "🚨 Error"
            else:
                shared["tracker"][spec_str]["Result"] = "❌ Mismatch"
                
            st.rerun() 
            
        elif not shared["ai_is_done"] or shared["gx_active_count"] > 0 or len(shared["ai_queue"]) > 0:
            active = shared["gx_active_count"]
            queued = len(shared["ai_queue"])
            with st.spinner(f"⏳ กำลังรัน: AI Queue = {queued}, GX Active = {active}"):
                time.sleep(1)
                st.rerun()
                
        else:
            # ============================================================
            # Stage 4: Results Dashboard
            # ============================================================
            if shared["found_count"] == 0:
                st.error("🚨 ไม่พบข้อมูลใน Database เลยแม้แต่รายการเดียว กรุณาตรวจสอบวันที่หรือเงื่อนไข SQL ครับ")
            else:
                results = st.session_state.overall_results
                
                # สร้าง Batch ทันทีที่รันเสร็จ เพื่อให้ผูก waived_rules ไว้กับ Batch นี้ได้เลย
                if "current_batch" not in st.session_state:
                    st.session_state.current_batch = {
                        "id": str(uuid.uuid4()),
                        "timestamp": datetime.datetime.now().isoformat(),
                        "spec_file": st.session_state.get("uploaded_filename", ""),
                        "date_queried": st.session_state.val_params.get("date", ""),
                        "spec_count": len(st.session_state.selected_spec_strings),
                        "results": results,
                        "tracker": dict(shared["tracker"]),
                        "waived_rules": set()
                    }
                    if "batch_history" not in st.session_state:
                        st.session_state.batch_history = []
                    st.session_state.batch_history.insert(0, st.session_state.current_batch)
                    
                    try:
                        pdf_path = generate_batch_report_pdf(st.session_state.current_batch, set())
                        st.session_state.current_batch["pdf_path"] = pdf_path
                    except Exception as e:
                        st.session_state.current_batch["pdf_error"] = str(e)
                
                batch = st.session_state.current_batch
                batch_id_display = shared.get("batch_id", "N/A")
                
                # Summary metrics
                render_summary_metrics(batch, batch_id_display)
                
                # รวมปุ่ม Data Docs ทั้งหมดไว้ที่เดียว
                docs_list = [(r.get('name', ''), r.get('docs_url', '')) for r in results if r.get('docs_url')]
                if docs_list:
                    st.markdown("### 📄 Data Docs Reports")
                    cols = st.columns(min(len(docs_list), 3))
                    for i, (name, url) in enumerate(docs_list):
                        with cols[i % min(len(docs_list), 3)]:
                            st.button(f"📄 {name}", on_click=open_report_in_browser, args=[url], key=f"docs_btn_{i}", type="primary", use_container_width=True)
                
                st.markdown("---")
                
                # --- สรุป Failed Rules & Waive Feature ---
                render_waive_ui(batch)
                # -----------------------------------

                st.markdown("### 📋 รายละเอียดแต่ละ Spec")
                
                # Per-spec detail cards
                for run_data in results:
                    suite = run_data.get('name', 'Unknown')
                    eff_success, eff_fail, eff_err = get_effective_metrics([run_data], batch.get("waived_rules", set()))
                    eff_status = "SUCCESS" if eff_success > 0 else ("ERROR" if eff_err > 0 else "FAILURE")
                    
                    status_icon = "✅" if eff_status == "SUCCESS" else ("🚨" if eff_status == "ERROR" else "❌")
                    
                    with st.expander(f"{status_icon} **{suite}** — {eff_status}", expanded=(eff_status != "SUCCESS")):
                        if run_data['status'] == "ERROR":
                            st.error(f"**Error:** {run_data.get('error', 'Unknown')}")
                            if run_data.get('traceback'):
                                st.code(run_data['traceback'], language='text')
                            continue
                        
                        # Quick info row
                        ic1, ic2, ic3 = st.columns(3)
                        ic1.caption(f"🕐 {run_data.get('timestamp', '-')}")
                        ic2.caption(f"📊 Run: {run_data.get('run_name', '-')}")
                        
                        # Data Docs button — ใช้ docs_url (segment-specific)
                        best_url = run_data.get("docs_url", "")
                        if best_url:
                            ic3.button("📄 เปิด Data Docs", on_click=open_report_in_browser, args=[best_url], key=f"docs_{run_data['id']}", type="primary")
                        
                        # Validation errors
                        if run_data.get('checkpoint_result'):
                            display_validation_errors(run_data['checkpoint_result'], batch.get("waived_rules", set()))
                        
                        # AI Prompt & Response
                        if run_data.get("ai_prompt"):
                            with st.expander("🧠 AI Prompt"):
                                st.code(run_data["ai_prompt"], language='text')
                        if run_data.get("ai_response"):
                            with st.expander("🤖 AI Response"):
                                st.code(run_data["ai_response"], language='json')
                        
                        # Process Log
                        if run_data.get("validation_process_log"):
                            with st.expander("📝 Process Log"):
                                for log_entry in run_data["validation_process_log"]:
                                    st.markdown(log_entry, unsafe_allow_html=True)
                        
                        # Data Samples
                        if run_data.get('data_samples'):
                            with st.expander("🔬 Data Samples"):
                                for segment_name, sample_df in run_data['data_samples'].items():
                                    st.caption(f"Segment: {segment_name}")
                                    st.dataframe(sample_df, use_container_width=True)
            
            del st.session_state.pipeline_init
            st.session_state.stage = "results_done"
            st.session_state.viewing_batch = st.session_state.get("current_batch")
            
            if st.button("🔄 โหลด Spec ใหม่ / เริ่มรันวันอื่น", use_container_width=True):
                st.session_state.stage = "awaiting_spec"
                st.session_state.full_df_uploaded = False
                if "current_batch" in st.session_state:
                    del st.session_state.current_batch
                st.rerun()

    # ============================================================
    # Stage 4 (revisit): Show results when coming back from sidebar
    # ============================================================
    if st.session_state.stage == "results_done":
        if st.session_state.get("viewing_batch"):
            batch = st.session_state.viewing_batch
            
            st.markdown(f"### 📋 Batch Results — {batch.get('spec_file', '')} ({batch.get('date_queried', '')})")
            
            render_summary_metrics(batch, batch.get('id', 'N/A')[:8])
            
            # Show tracker table
            results = batch.get("results", [])
            if batch.get("tracker") and results:
                effective_tracker = get_effective_tracker_data(batch["tracker"], results, batch.get("waived_rules", set()))
                tracker_df = pd.DataFrame(effective_tracker)
                display_cols = [c for c in tracker_df.columns if c != "Docs" and not c.startswith("_")]
                st.dataframe(tracker_df[display_cols], use_container_width=True, hide_index=True)
            
            st.divider()
            
            # รวมปุ่ม Data Docs ทั้งหมด
            docs_list = [(r.get('name', ''), r.get('docs_url', '')) for r in results if r.get('docs_url')]
            if docs_list:
                st.markdown("### 📄 Data Docs Reports")
                cols = st.columns(min(len(docs_list), 3))
                for i, (name, url) in enumerate(docs_list):
                    with cols[i % min(len(docs_list), 3)]:
                        st.button(f"📄 {name}", on_click=open_report_in_browser, args=[url], key=f"bh_docs_btn_{i}", type="primary", use_container_width=True)
                st.divider()
            
            # --- สรุป Failed Rules & Waive Feature ---
            render_waive_ui(batch)
            # -----------------------------------
            
            for run_data in results:
                suite = run_data.get('name', 'Unknown')
                eff_success, eff_fail, eff_err = get_effective_metrics([run_data], batch.get("waived_rules", set()))
                eff_status = "SUCCESS" if eff_success > 0 else ("ERROR" if eff_err > 0 else "FAILURE")
                
                status_icon = "✅" if eff_status == "SUCCESS" else ("🚨" if eff_status == "ERROR" else "❌")
                
                with st.expander(f"{status_icon} **{suite}** — {eff_status}"):
                    if run_data['status'] == "ERROR":
                        st.error(f"**Error:** {run_data.get('error', 'Unknown')}")
                        continue
                    
                    best_url = run_data.get("docs_url", "")
                    if best_url:
                        st.button("📄 เปิด Data Docs", on_click=open_report_in_browser, args=[best_url], key=f"batch_docs_{run_data['id']}", type="primary")
                    
                    if run_data.get('checkpoint_result'):
                        display_validation_errors(run_data['checkpoint_result'])
                    
                    if run_data.get("ai_prompt"):
                        with st.expander("🧠 AI Prompt"):
                            st.code(run_data["ai_prompt"], language='text')
                    if run_data.get("ai_response"):
                        with st.expander("🤖 AI Response"):
                            st.code(run_data["ai_response"], language='json')
            
            if st.button("🔄 รันใหม่", use_container_width=True):
                st.session_state.stage = "awaiting_spec"
                st.session_state.full_df_uploaded = False
                if "viewing_batch" in st.session_state:
                    del st.session_state.viewing_batch
                st.rerun()
        elif not st.session_state.get("pipeline_init"):
            # Just finished processing, show results
            pass

# --- 3. Main App Logic & State Initialization ---
if "history" not in st.session_state: st.session_state.history = []
if "active_run_id" not in st.session_state: st.session_state.active_run_id = None
if "stage" not in st.session_state: st.session_state.stage = "awaiting_spec"
if "val_params" not in st.session_state: st.session_state.val_params = {}
if "sql_config" not in st.session_state: st.session_state.sql_config = {}
if "batch_history" not in st.session_state: st.session_state.batch_history = []

# --- Sidebar ---
is_pipeline_running = (st.session_state.get("stage") == "processing_pipeline" and st.session_state.get("pipeline_init"))

with st.sidebar:
    st.header("Tracking Validation")
    if is_pipeline_running:
        st.warning("⚠️ Pipeline กำลังทำงาน กรุณารอจนเสร็จ")
        st.button("➕ New Run", use_container_width=True, disabled=True)
    else:
        if st.button("➕ New Run", use_container_width=True):
            st.session_state.active_run_id = None
            st.session_state.stage = "awaiting_spec"
            st.session_state.full_df_uploaded = False
            st.session_state.val_params = {}
            st.session_state.sql_config = {}
            if 'current_batch' in st.session_state:
                del st.session_state.current_batch
            if 'viewing_batch' in st.session_state:
                del st.session_state.viewing_batch
            if 'pipeline_init' in st.session_state:
                del st.session_state.pipeline_init
            st.rerun()

    st.markdown("---")
    st.header("📜 Batch History")
    if not st.session_state.batch_history:
        st.caption("ยังไม่มีประวัติ")
    else:
        for batch in st.session_state.batch_history:
            results = batch.get("results", [])
            success = sum(1 for r in results if r.get('status') == 'SUCCESS')
            total = len(results)
            icon = "✅" if success == total and total > 0 else ("⚠️" if success > 0 else "❌")
            label = f"{icon} {batch.get('spec_file', 'batch')} ({batch.get('date_queried', '')})"
            if is_pipeline_running:
                st.button(label, key=f"batch_{batch['id']}", use_container_width=True, disabled=True)
            else:
                if st.button(label, key=f"batch_{batch['id']}", use_container_width=True):
                    st.session_state.viewing_batch = batch
                    st.session_state.stage = "results_done"
                    st.rerun()

# --- Main Panel ---
bulk_validation_ui()