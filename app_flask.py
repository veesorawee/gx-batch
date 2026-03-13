import os, sys, json, re, glob, uuid, time, datetime, threading, traceback, urllib
import urllib.parse as urlparse
import pandas as pd
import great_expectations as gx
from dotenv import load_dotenv
from sqlalchemy.engine import create_engine
from sqlalchemy import text
from flask import (Flask, render_template, request, redirect, url_for,
                   session, flash, send_file, Response, jsonify)
import google.generativeai as genai
import io

# Fix WeasyPrint Library Loading on macOS (Homebrew)
if sys.platform == "darwin":
    homebrew_lib_path = "/opt/homebrew/lib"
    if homebrew_lib_path not in os.environ.get("DYLD_FALLBACK_LIBRARY_PATH", ""):
        os.environ["DYLD_FALLBACK_LIBRARY_PATH"] = f"{homebrew_lib_path}:{os.environ.get('DYLD_FALLBACK_LIBRARY_PATH', '')}"

import weasyprint
from pypdf import PdfWriter, PdfReader

# ─── App Setup ──────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.urandom(24)

SQL_DIR = "sql"
EXPECTATIONS_DIR = "expectations"
LOGS_DIR = "logs"
REPORTS_DIR = "reports"
for d in [SQL_DIR, EXPECTATIONS_DIR, LOGS_DIR, REPORTS_DIR]:
    os.makedirs(d, exist_ok=True)

load_dotenv()
GEMINI_AVAILABLE = False
api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    try:
        genai.configure(api_key=api_key)
        GEMINI_AVAILABLE = True
    except Exception:
        pass

TRINO_HOST = os.getenv('TRINO_HOST')
TRINO_PORT = os.getenv('TRINO_PORT')
TRINO_USERNAME = os.getenv('TRINO_USERNAME')
TRINO_PASSWORD = os.getenv('TRINO_PASSWORD')
DB_CONFIG = (TRINO_HOST, TRINO_PORT, TRINO_USERNAME, TRINO_PASSWORD)

# ─── In-Memory State ────────────────────────────────────────
# Stores all batch data (keyed by batch_id)
app_state = {
    "batches": {},          # batch_id -> batch dict
    "batch_history": [],    # list of batch summary dicts
    "current_batch_id": None,
    "full_df": None,
    "uploaded_filename": None,
    "unique_specs": [],
    "pipeline_shared": None,  # shared state for the running pipeline
    "sse_events": [],       # SSE event queue
    "sse_lock": threading.Lock(),
}


def push_sse(event_type, data):
    """Push an SSE event to the queue and persist tracker updates."""
    with app_state["sse_lock"]:
        app_state["sse_events"].append((event_type, json.dumps(data)))
    # Also update the shared tracker so finalized batch has correct status
    if event_type == "tracker_update":
        shared = app_state.get("pipeline_shared")
        if shared and "tracker" in shared:
            spec = data.get("spec")
            field = data.get("field")
            value = data.get("value")
            if spec and field and spec in shared["tracker"]:
                shared["tracker"][spec][field] = value


def list_files(directory, extension):
    return [os.path.basename(f) for f in glob.glob(os.path.join(directory, f"*.{extension}"))]


# ─── Business Logic (ported from app.py) ────────────────────

def prepare_spec_sql(base_sql, current_event_type, current_placement_id):
    has_event_type_placeholder = '{event_type}' in base_sql
    if has_event_type_placeholder:
        base_sql = base_sql.replace('{event_type}', current_event_type)
    sql_lower = base_sql.lower()
    final_sql = base_sql
    parts = str(current_placement_id).split('/')
    p_screen = parts[0] if len(parts) > 0 else None
    p_category = parts[1] if len(parts) > 1 else None
    p_target = parts[2] if len(parts) > 2 else None
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
    if 'raw' in sql_lower:
        final_sql = f"SELECT * FROM (\n{base_sql}\n) AS filtered_raw \nWHERE {dynamic_where_clause}"
    elif has_event_type_placeholder:
        if where_conditions:
            final_sql = base_sql.rstrip() + "\nAND " + "\nAND ".join(where_conditions)
    return final_sql


def flatten_json_columns(df, flatten_map):
    for parent_col, keys_to_flatten in flatten_map.items():
        if parent_col not in df.columns or not keys_to_flatten:
            continue
        def safe_get(json_string, key):
            if isinstance(json_string, dict):
                return json_string.get(key)
            if isinstance(json_string, str):
                try:
                    parsed = json.loads(json_string)
                    if isinstance(parsed, dict):
                        return parsed.get(key)
                except json.JSONDecodeError:
                    pass
            return None
        for key in keys_to_flatten:
            df[f'{parent_col}_{key}'] = df[parent_col].apply(safe_get, key=key)
    return df


def run_validation_process(sql_input, expectations_input, suite_name, db_config,
                           val_params, flatten_map=None, batch_id=None):
    trino_host, trino_port, trino_username, trino_password = db_config
    db_query_date, SEGMENT_BY_COLUMNS = val_params
    context = gx.get_context(project_root_dir=os.getcwd())
    checkpoint_result, data_samples = None, None
    docs_info = {"main": None, "segments": {}, "local_path": None}
    process_log = []
    run_name = f"run_{suite_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

    try:
        expectations_list = json.loads(expectations_input)
    except json.JSONDecodeError:
        process_log.append("🚨 ERROR: Invalid Expectations JSON format.")
        return None, None, None, process_log, run_name, None

    if flatten_map:
        keys_to_flatten_map = {p: list(k) for p, k in flatten_map.items()}
        object_columns = sorted(list(keys_to_flatten_map.keys()))
    else:
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

    process_log.append(f"🔗 Connecting to DB...")
    try:
        engine = create_engine(
            f'trino://{trino_username}:{urllib.parse.quote(trino_password)}@{trino_host}:{trino_port}/',
            connect_args={'http_scheme': 'https', 'source': 'gx-flask-app'})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        process_log.append("✅ Connection successful.")
    except Exception as e:
        process_log.append(f"🚨 ERROR: Could not connect: {e}")
        return None, None, None, process_log, run_name, None

    segments_to_run = []
    if SEGMENT_BY_COLUMNS:
        try:
            base_q = sql_input.format(dt=db_query_date, extra_where_conditions="")
            disc_sql = f"SELECT {', '.join(SEGMENT_BY_COLUMNS)} FROM ({base_q}) AS dq GROUP BY {', '.join(SEGMENT_BY_COLUMNS)}"
            segments_df = pd.read_sql(text(disc_sql), engine)
            segments_to_run = segments_df.to_dict('records')
        except Exception as e:
            process_log.append(f"🚨 ERROR discovering segments: {e}")
            return None, None, None, process_log, run_name, None
        if not segments_to_run:
            process_log.append(f"⚠️ No segments found for date {db_query_date}.")
            return None, None, None, process_log, run_name, None
    else:
        segments_to_run = [{}]

    context = gx.get_context()
    context.add_or_update_expectation_suite(suite_name)
    validations_to_run = []
    data_samples = {}

    for i, segment_filters in enumerate(segments_to_run):
        seg_name = "-".join(str(v) for v in segment_filters.values()).replace(".", "_") if segment_filters else "full_dataset"
        extra_where = " ".join([f"AND {c} = '{v}'" for c, v in segment_filters.items()])
        final_query = sql_input.format(dt=db_query_date, extra_where_conditions=extra_where) + " LIMIT 500"
        try:
            df = pd.read_sql(text(final_query), engine)
            if df.empty:
                continue
            for col_name in object_columns:
                if col_name in df.columns:
                    def parse_to_dict(val):
                        if isinstance(val, dict): return val
                        if isinstance(val, str):
                            try:
                                parsed = json.loads(val)
                                if isinstance(parsed, str): parsed = json.loads(parsed)
                                if isinstance(parsed, dict): return parsed
                            except: pass
                        return None
                    df[col_name] = df[col_name].apply(parse_to_dict)
            df = flatten_json_columns(df, keys_to_flatten_map)
            for col_name in object_columns:
                if col_name in df.columns:
                    df[col_name] = df[col_name].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)
            for parent_col, keys in keys_to_flatten_map.items():
                for key in keys:
                    flat_col = f"{parent_col}_{key}"
                    if flat_col in df.columns:
                        df[flat_col] = df[flat_col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)
            spec_columns = set()
            for exp_config in expectations_list:
                if exp_config.get("expectation_type") == "expect_table_columns_to_match_set":
                    spec_columns = set(exp_config["kwargs"].get("column_set", []))
                    break
            if spec_columns:
                df = df[[c for c in df.columns if c in spec_columns]]

            ds_suffix = f"{batch_id}_{seg_name}" if batch_id else seg_name
            data_samples[seg_name] = df.head(5).to_dict('records')
            datasource = context.sources.add_or_update_pandas(f"pandas_ds_{ds_suffix}")
            data_asset = datasource.add_dataframe_asset(name=f"asset_{ds_suffix}")
            batch_request = data_asset.build_batch_request(dataframe=df)
            validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
            for exp_config in expectations_list:
                getattr(validator, exp_config['expectation_type'])(**exp_config["kwargs"])
            validator.save_expectation_suite(discard_failed_expectations=False)
            validations_to_run.append({"batch_request": batch_request, "expectation_suite_name": suite_name})
        except Exception as e:
            process_log.append(f"🚨 Segment {seg_name} failed: {e}")
            continue

    if not validations_to_run:
        process_log.append("🚨 No valid batches were created.")
        return None, None, None, process_log, run_name, None

    checkpoint = context.add_or_update_checkpoint(
        name=f"checkpoint_{run_name}", run_name_template=run_name,
        action_list=[
            {"name": "store_validation_result", "action": {"class_name": "StoreValidationResultAction"}},
            {"name": "store_evaluation_params", "action": {"class_name": "StoreEvaluationParametersAction"}},
            {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
        ])
    checkpoint_result = checkpoint.run(validations=validations_to_run)
    context.build_data_docs()
    docs_sites = context.get_docs_sites_urls()
    if docs_sites:
        docs_info["local_path"] = docs_sites[0]['site_url']

    return checkpoint_result, data_samples, docs_info, process_log, run_name, None


# ─── Result Extraction Helpers ──────────────────────────────

def extract_failed_from_log(log_data, waived_rules=None):
    """Extract failed expectations from a checkpoint result's JSON log."""
    if waived_rules is None:
        waived_rules = set()
    results = []
    run_results = log_data.get("run_results", {})
    for vr_key, vr_val in run_results.items():
        validation = vr_val.get("validation_result", {})
        for exp_result in validation.get("results", []):
            if not exp_result.get("success", False):
                exp_config = exp_result.get("expectation_config", {})
                kwargs = exp_config.get("kwargs", {})
                exp_type_full = exp_config.get("expectation_type", "")
                exp_type = exp_type_full.replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")
                col = kwargs.get("column", "-")
                if "expect_table" in exp_type_full:
                    col = "-"
                rule_key = f"{col}|{exp_type_full}"
                is_waived = rule_key in waived_rules
                res_data = exp_result.get("result", {})
                el_count = res_data.get("element_count", 0)
                unexp_count = res_data.get("unexpected_count", 0)
                error_pct = (unexp_count / el_count * 100) if el_count > 0 else 0
                expected = kwargs.get("value_set") or kwargs.get("regex") or kwargs.get("type_") or ""
                examples = [str(x) for x in res_data.get("partial_unexpected_list", [])[:5]]
                results.append({
                    "column": col, "exp_type": exp_type, "exp_type_full": exp_type_full,
                    "rule_key": rule_key, "is_waived": is_waived,
                    "expected": str(expected), "error_count": unexp_count,
                    "error_pct": error_pct, "examples": examples, "kwargs": kwargs,
                })
    return results


def get_effective_metrics_from_results(results, waived_rules=None):
    if waived_rules is None:
        waived_rules = set()
    passed, failed, error = 0, 0, 0
    for r in results:
        if r.get("status") == "ERROR":
            error += 1
            continue
        if r.get("status") == "SUCCESS":
            passed += 1
            continue
        has_unwaived = False
        log_data = r.get("log_data", {})
        for vr_key, vr_val in log_data.get("run_results", {}).items():
            for exp_result in vr_val.get("validation_result", {}).get("results", []):
                if not exp_result.get("success", False):
                    ec = exp_result.get("expectation_config", {})
                    et_full = ec.get("expectation_type", "")
                    c = ec.get("kwargs", {}).get("column", "-")
                    if "expect_table" in et_full:
                        c = "-"
                    if f"{c}|{et_full}" not in waived_rules:
                        has_unwaived = True
                        break
            if has_unwaived:
                break
        if has_unwaived:
            failed += 1
        else:
            passed += 1
    return {"passed": passed, "failed": failed, "error": error, "total": len(results)}


def get_tracker_rows(tracker, results, waived_rules=None):
    if waived_rules is None:
        waived_rules = set()
    rows = []
    for v in sorted(tracker.values(), key=lambda x: (x.get('Placement ID', ''), x.get('Event Type', ''))):
        v_copy = dict(v)
        spec_str = f"{v.get('Event Type', '')} | {v.get('Placement ID', '')}"
        matching = next((r for r in results if f"{r.get('ev_type', '')} | {r.get('plc_id', '')}" == spec_str), None)
        if matching and matching.get("log_data"):
            total_exp, passed_exp = 0, 0
            for vr_val in matching["log_data"].get("run_results", {}).values():
                for er in vr_val.get("validation_result", {}).get("results", []):
                    total_exp += 1
                    if er.get("success", False):
                        passed_exp += 1
                    else:
                        ec = er.get("expectation_config", {})
                        et_full = ec.get("expectation_type", "")
                        c = ec.get("kwargs", {}).get("column", "-")
                        if "expect_table" in et_full:
                            c = "-"
                        if f"{c}|{et_full}" in waived_rules:
                            passed_exp += 1
            if total_exp > 0:
                v_copy["Pass/Fail"] = f"{passed_exp}/{total_exp} ({round(passed_exp/total_exp*100)}%)"
                v_copy["Result"] = "Passed" if passed_exp == total_exp else "Mismatch"
            else:
                v_copy["Pass/Fail"] = "0/0"
                v_copy["Result"] = "Error (No Rules)"
        if "Passed" in v_copy.get("Result", ""):
            v_copy["_status_class"] = "row-pass"
        elif "Mismatch" in v_copy.get("Result", ""):
            v_copy["_status_class"] = "row-fail"
        elif "Error" in v_copy.get("Result", ""):
            v_copy["_status_class"] = "row-error"
        else:
            v_copy["_status_class"] = "row-skip"
        rows.append(v_copy)
    return rows


def get_failed_summary(results, waived_rules=None):
    if waived_rules is None:
        waived_rules = set()
    summary = {}
    for r in results:
        if r.get("status") != "FAILURE" or not r.get("log_data"):
            continue
        spec_str = f"{r.get('ev_type', '')} | {r.get('plc_id', '')}"
        for vr_val in r["log_data"].get("run_results", {}).values():
            for exp_result in vr_val.get("validation_result", {}).get("results", []):
                if not exp_result.get("success", False):
                    ec = exp_result.get("expectation_config", {})
                    kwargs = ec.get("kwargs", {})
                    et = ec.get("expectation_type", "").replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")
                    et_full = ec.get("expectation_type", "")
                    et = et_full.replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")
                    col = kwargs.get("column", "-")
                    if "expect_table" in et_full:
                        col = "-"
                    rk = f"{col}|{et_full}"
                    if rk not in summary:
                        extra = ""
                        if "regex" in kwargs:
                            extra = f"regex: {kwargs['regex']}"
                        elif "value_set" in kwargs:
                            extra = f"set: {kwargs['value_set']}"
                        summary[rk] = {"col": col, "exp_type": et, "rule_key": rk,
                                       "detail": extra, "count": 0, "placement_set": set()}
                    summary[rk]["count"] += exp_result.get("result", {}).get("unexpected_count", 0)
                    summary[rk]["placement_set"].add(spec_str)
    items = sorted(summary.values(), key=lambda x: x["count"], reverse=True)
    for item in items:
        item["placements"] = len(item["placement_set"])
        del item["placement_set"]
    return items


def generate_batch_pdf(batch):
    """Generate a full PDF report with detail sections per placement."""
    results = batch.get("results", [])
    waived = batch.get("waived_rules", set())
    metrics = get_effective_metrics_from_results(results, waived)
    tracker = batch.get("tracker", {})
    tracker_rows = get_tracker_rows(tracker, results, waived)

    def render_pdf_status(v):
        if not v: return ""
        v_str = str(v)
        if any(w in v_str for w in ['Found', 'Done', 'Passed', 'SUCCESS']): return f"✅ {v_str}"
        if any(w in v_str for w in ['Mismatch', 'Failed', 'FAILURE']): return f"❌ {v_str}"
        if any(w in v_str for w in ['Error', 'No DB', 'No JSON']): return f"❌ {v_str}"
        if any(w in v_str for w in ['Skipped', 'No Data']): return f"⏭️ {v_str}"
        if any(w in v_str for w in ['Queued', 'Running', 'Generating', 'Checking', 'Retrying']): return f"⏳ {v_str}"
        return v_str

    # --- Summary table rows ---
    summary_rows_html = ""
    for v in tracker_rows:
        sc = v.pop("_status_class", "")
        summary_rows_html += f"""<tr class="{sc}">
            <td>{v.get('Event Type','')}</td><td>{v.get('Placement ID','')}</td>
            <td>{render_pdf_status(v.get('Data Check',''))}</td><td>{render_pdf_status(v.get('AI',''))}</td>
            <td>{v.get('Rules','')}</td><td>{render_pdf_status(v.get('GX',''))}</td>
            <td>{v.get('Pass/Fail','')}</td><td>{render_pdf_status(v.get('Result',''))}</td></tr>"""

    # --- Per-placement detail sections ---
    sorted_results = sorted(results, key=lambda r: (r.get('plc_id', ''), r.get('ev_type', '')))
    details_html = ""
    for run_data in sorted_results:
        ev = run_data.get('ev_type', '')
        plc = run_data.get('plc_id', '')
        plc_parts = str(plc).split('/')
        label_parts = []
        if ev: label_parts.append(f'<b>Event:</b> {ev}')
        if plc_parts[0]: label_parts.append(f'<b>Screen:</b> {plc_parts[0]}')
        if len(plc_parts) > 1: label_parts.append(f'<b>Category:</b> {plc_parts[1]}')
        if len(plc_parts) > 2: label_parts.append(f'<b>Target:</b> {plc_parts[2]}')
        placement_label = ' &nbsp;|&nbsp; '.join(label_parts)
        suite_ref = run_data.get('name', '')

        if run_data.get("status") == "ERROR":
            details_html += f"""<div class="spec-section">
                <h3>{placement_label}</h3>
                <p class="suite-ref">Suite: {suite_ref}</p>
                <p><b>ERROR:</b> {run_data.get('error','')}</p></div>"""
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
                    <p><b>ERROR:</b> No validation rules generated.</p></div>"""
                continue

            # Collect rows grouped by column
            from collections import OrderedDict
            pass_count, fail_count_detail, waive_count = 0, 0, 0
            col_groups = OrderedDict()  # col -> list of row dicts
            for exp_result in expectation_results:
                is_success = exp_result.get("success", False)
                exp_config = exp_result.get("expectation_config", {})
                exp_type_full = exp_config.get("expectation_type", "")
                kwargs = exp_config.get("kwargs", {})
                col = kwargs.get("column", "-")
                if "expect_table" in exp_type_full: col = "-"
                result_data = exp_result.get("result", {})
                short_type = exp_type_full.replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")

                expected = ""
                if "regex" in kwargs: expected = kwargs["regex"]
                elif "value_set" in kwargs: expected = str(kwargs["value_set"])
                elif "column_set" in kwargs: expected = f'{len(kwargs["column_set"])} cols'
                elif "json_schema" in kwargs: expected = 'schema'

                row_info = {"short_type": short_type, "expected": expected, "is_success": is_success}
                if is_success:
                    pass_count += 1
                    row_info["status_text"] = "✅ Pass"
                    row_info["row_class"] = "row-pass"
                    row_info["unexp_count"] = "-"
                    row_info["pct_str"] = "-"
                    row_info["ex_str"] = "-"
                else:
                    rule_key = f"{col}|{exp_type_full}"
                    is_waived = rule_key in waived
                    if is_waived: waive_count += 1
                    else: fail_count_detail += 1
                    row_info["status_text"] = '⚠️ Waived' if is_waived else '❌ Failed'
                    row_info["row_class"] = 'row-waived' if is_waived else 'row-fail'
                    row_info["is_waived"] = is_waived
                    unexp_count = result_data.get("unexpected_count", 0)
                    unexp_pct = result_data.get("unexpected_percent", 0)
                    row_info["unexp_count"] = unexp_count
                    row_info["pct_str"] = f"{unexp_pct:.1f}%" if isinstance(unexp_pct, (int, float)) else str(unexp_pct)
                    examples = result_data.get("partial_unexpected_list", [])
                    if examples:
                        unique_ex = list(dict.fromkeys([str(x) for x in examples[:6]]))
                        row_info["ex_str"] = ', '.join(unique_ex)
                    else:
                        row_info["ex_str"] = ""

                if col not in col_groups:
                    col_groups[col] = []
                col_groups[col].append(row_info)

            # Build merged-cell rows
            all_rows = ""
            for col, rows in col_groups.items():
                has_fail = any(not r["is_success"] and not r.get("is_waived", False) for r in rows)
                col_style = ' style="background:#fdf0f0;color:#c0392b;font-weight:bold;"' if has_fail else ''
                rowspan = len(rows)
                for i, r in enumerate(rows):
                    all_rows += f'<tr class="{r["row_class"]}">'
                    if i == 0:
                        all_rows += f'<td class="col-cell" rowspan="{rowspan}"{col_style}>{col}</td>'
                    all_rows += f'<td class="status-cell">{r["status_text"]}</td><td class="exp-cell">{r["short_type"]}</td><td class="expected-cell">{r["expected"]}</td><td class="num-cell">{r["unexp_count"]}</td><td class="num-cell">{r["pct_str"]}</td><td class="examples-cell">{r["ex_str"]}</td></tr>'

            total_exp = pass_count + fail_count_detail + waive_count
            status_label = "✅ PASS" if fail_count_detail == 0 else "❌ FAILURE"
            summary_badges = f"✅ {pass_count} passed | ❌ {fail_count_detail} failed"
            if waive_count > 0: summary_badges += f" | ⚠️ {waive_count} waived"
            summary_badges += f" | Total: {total_exp}"

            details_html += f"""<div class="spec-section">
                <h3>{placement_label} — {status_label}</h3>
                <p class="suite-ref">Suite: {suite_ref}</p>
                <p class="summary-line">{summary_badges}</p>
                <table class="detail-table"><thead><tr>
                    <th>Column</th><th>Status</th><th>Expectation</th><th>Expected</th><th>Errors</th><th>Error %</th><th>Examples</th>
                </tr></thead><tbody>{all_rows}</tbody></table></div>"""

    # --- Failed Rules Summary (ranked by column errors) ---
    failed_summary_html = ""
    rule_groups = {}
    for r in sorted_results:
        if r.get("status") == "ERROR" or not r.get("log_data"): continue
        spec_str = f"{r.get('ev_type','')} | {r.get('plc_id','')}"
        for vr_val in r["log_data"].get("run_results", {}).values():
            for exp_result in vr_val.get("validation_result", {}).get("results", []):
                if not exp_result.get("success", False):
                    ec = exp_result.get("expectation_config", {})
                    kwargs = ec.get("kwargs", {})
                    et = ec.get("expectation_type", "").replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")
                    col = kwargs.get("column", "-")
                    if "expect_table" in ec.get("expectation_type", ""): col = "-"
                    rk = f"{col} ({et})"
                    if rk in waived: continue
                    if rk not in rule_groups:
                        rule_groups[rk] = {"col": col, "exp_type": et, "total_count": 0, "placements": set()}
                    rule_groups[rk]["total_count"] += exp_result.get("result", {}).get("unexpected_count", 0)
                    rule_groups[rk]["placements"].add(spec_str)

    if rule_groups:
        col_totals = {}
        for rg in rule_groups.values():
            col_totals[rg["col"]] = col_totals.get(rg["col"], 0) + rg["total_count"]
        sorted_cols = sorted(col_totals.keys(), key=lambda c: col_totals[c], reverse=True)
        rank = 0
        failed_rows = ""
        for col_name in sorted_cols:
            col_rules = [rg for rg in rule_groups.values() if rg["col"] == col_name]
            col_rules.sort(key=lambda x: x["total_count"], reverse=True)
            rank += 1
            failed_rows += f'<tr class="col-group-header"><td style="text-align:center;">{rank}</td><td colspan="2"><b>{col_name}</b></td><td style="text-align:center;color:#c0392b;"><b>{col_totals[col_name]}</b></td><td></td></tr>'
            for rule in col_rules:
                plc_str = ", ".join(sorted(rule["placements"]))
                failed_rows += f'<tr class="row-fail"><td></td><td></td><td><span style="color:#7f8c8d;font-family:monospace;">{rule["exp_type"]}</span></td><td style="text-align:center;color:#c0392b;">{rule["total_count"]}</td><td style="font-size:7px;color:#7f8c8d;">{plc_str}</td></tr>'
        failed_summary_html = f"""<h2 style="color:#c0392b;">🚨 Failed Rules Summary</h2>
            <table class="failed-summary-table"><thead><tr><th>Rank</th><th>Column</th><th>Rule</th><th>Errors</th><th>Impacted Placements</th></tr></thead>
            <tbody>{failed_rows}</tbody></table>"""

    # --- Waived Rules Section ---
    waived_rules_html = ""
    if waived:
        waived_rows = ""
        for rule in sorted(list(waived)):
            parts = rule.split('|', 1)
            w_col = parts[0]
            w_exp = parts[1] if len(parts) > 1 else ""
            w_exp = w_exp.replace("expect_column_values_to_", "").replace("expect_table_columns_to_", "table_")
            waived_rows += f'<tr class="row-waived"><td><b>{w_col}</b></td><td><span style="color:#7f8c8d;font-family:monospace;">{w_exp}</span></td></tr>'
        waived_rules_html = f"""<h2 style="color:#d35400;">🛑 Waived Rules</h2>
            <table class="failed-summary-table"><thead><tr><th>Column</th><th>Expectation Rule</th></tr></thead>
            <tbody>{waived_rows}</tbody></table>"""

    # --- Assemble final HTML ---
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <style>
    @page {{ size: A4 portrait; margin: 1.2cm; }}
    body {{ font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 10px; color: #2c3e50; }}
    h1 {{ font-size: 18px; margin-bottom: 4px; }} h2 {{ font-size: 14px; margin-top: 20px; border-bottom: 1px solid #e2e8f0; padding-bottom: 4px; }}
    .meta {{ color: #7f8c8d; font-size: 9px; margin-bottom: 12px; }}
    .metrics {{ display: flex; gap: 16px; margin-bottom: 20px; }}
    .metric {{ background: #f8f9fa; border-radius: 6px; padding: 12px 20px; text-align: center; flex: 1; border: 1px solid #e2e8f0; }}
    .metric .num {{ font-size: 22px; font-weight: bold; }}
    .metric.pass .num {{ color: #27ae60; }} .metric.fail .num {{ color: #c0392b; }}
    .metric.error .num {{ color: #d35400; }} .metric.total .num {{ color: #2980b9; }}
    table {{ width: 100%; border-collapse: collapse; margin-bottom: 16px; font-size: 9px; }}
    th {{ background: #34495e; color: white; padding: 5px 6px; text-align: left; font-size: 8px; }}
    td {{ padding: 4px 6px; border-bottom: 1px solid #ecf0f1; font-size: 8px; word-wrap: break-word; }}
    tr.row-pass, tr.pass {{ background: #f9fbf9; }} tr.row-fail, tr.fail {{ background: #fdf5f5; }}
    tr.row-error {{ background: #fef9e7; }} tr.row-waived {{ background: #fff8e1; }}
    .spec-section {{ margin-bottom: 20px; padding-bottom: 16px; border-bottom: 2px solid #ecf0f1; page-break-inside: avoid; }}
    .spec-section h3 {{ margin: 0 0 4px 0; font-size: 11px; }}
    .summary-line {{ font-size: 9px; margin: 4px 0 8px 0; color: #7f8c8d; }}
    .suite-ref {{ font-size: 9px; color: #95a5a6; margin: 0 0 6px 0; font-style: italic; }}
    .detail-table {{ border: 1px solid #e2e8f0; }}
    .detail-table th {{ background: #7f8c8d; font-size: 7px; padding: 3px 4px; }}
    .detail-table td {{ border: 1px solid #ecf0f1; padding: 3px 4px; font-size: 7px; vertical-align: top; word-break: break-word; }}
    .status-cell {{ width: 50px; font-size: 7px; }}
    .col-cell {{ font-weight: bold; color: #34495e; vertical-align: middle; border-right: 2px solid #bdc3c7; border-bottom: 2px solid #bdc3c7; }}
    .exp-cell {{ font-family: monospace; font-size: 6px; color: #7f8c8d; }}
    .expected-cell {{ font-size: 6px; color: #7f8c8d; max-width: 100px; overflow: hidden; }}
    .num-cell {{ text-align: center; }}
    .examples-cell {{ font-family: monospace; font-size: 6px; color: #c0392b; max-width: 120px; word-break: break-all; }}
    .row-pass {{ background: #f0faf0; }}
    .row-fail {{ background: #fdf0f0; }}
    .row-waived {{ background: #fdf6e8; }}
    .failed-summary-table {{ width: 100%; border-collapse: collapse; margin-bottom: 16px; }}
    .failed-summary-table th {{ background: #c0392b; color: white; padding: 5px 6px; text-align: left; font-size: 8px; }}
    .failed-summary-table td {{ border-bottom: 1px solid #ecf0f1; padding: 4px 6px; font-size: 8px; vertical-align: top; }}
    .col-group-header {{ background: #f4f6f7; font-weight: bold; }}
    .col-group-header td {{ font-size: 9px; padding: 5px 6px; border-bottom: 2px solid #bdc3c7; }}
    </style></head><body>
    <h1>Tracking Validation Report</h1>
    <p class="meta">Spec: <b>{batch.get('spec_file','')}</b> | Date: <b>{batch.get('date_queried','')}</b><br>
    Batch: <code>{batch.get('id','')[:8]}</code> | Generated: {batch.get('timestamp','')}</p>
    <div class="metrics">
    <div class="metric pass"><div class="num">{metrics['passed']}</div><div>Pass</div></div>
    <div class="metric fail"><div class="num">{metrics['failed']}</div><div>Fail</div></div>
    <div class="metric error"><div class="num">{metrics['error']}</div><div>Error</div></div>
    <div class="metric total"><div class="num">{metrics['total']}</div><div>Total</div></div>
    </div>
    <h2>Summary</h2>
    <table><thead><tr><th>Event</th><th>Placement</th><th>Data Check</th><th>AI</th><th>Rules</th><th>GX</th><th>Pass/Fail</th><th>Result</th></tr></thead>
    <tbody>{summary_rows_html}</tbody></table>
    {failed_summary_html}
    {waived_rules_html}
    <h2>Validation Details</h2>
    {details_html}
    </body></html>"""

    pdf_bytes = weasyprint.HTML(string=html).write_pdf()
    bid = batch.get("id", "unknown")[:8]
    fname = f"report_{batch.get('spec_file','batch').replace(' ','_')}_{batch.get('date_queried','')}_{bid}.pdf"
    fpath = os.path.join(REPORTS_DIR, fname)
    with open(fpath, "wb") as f:
        f.write(pdf_bytes)
    return fpath


# ─── Pipeline Workers ───────────────────────────────────────

def db_checker_worker(specs, sql_cfg, val_params, db_config, shared):
    trino_host, trino_port, trino_username, trino_password = db_config
    base_sql = ""
    if sql_cfg["source"] == "file":
        with open(os.path.join(SQL_DIR, sql_cfg["file"]), 'r', encoding='utf-8') as f:
            base_sql = f.read()
    else:
        base_sql = sql_cfg["custom_sql"]
    try:
        engine = create_engine(
            f'trino://{trino_username}:{urllib.parse.quote(trino_password)}@{trino_host}:{trino_port}/',
            connect_args={'http_scheme': 'https'})
    except Exception as e:
        push_sse("status", {"text": f"🚨 DB Connection Error: {e}"})
        shared["is_done"] = True
        return

    # Test connection first
    push_sse("status", {"text": "🔗 Testing Trino connection..."})
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        push_sse("status", {"text": "Trino connected!"})
    except Exception as e:
        push_sse("status", {"text": f"Cannot connect to Trino: {e}"})
        for spec_str in specs:
            push_sse("tracker_update", {"spec": spec_str, "field": "Data Check", "value": "No DB"})
            push_sse("tracker_update", {"spec": spec_str, "field": "Result", "value": "DB Error"})
            shared["completed_count"] += 1
            push_sse("progress", {"completed": shared["completed_count"]})
        shared["is_done"] = True
        return

    for spec_str in specs:
        ev_type, plc_id = spec_str.split(" | ")
        push_sse("status", {"text": f"🔍 Checking: {ev_type} ({plc_id})"})
        push_sse("tracker_update", {"spec": spec_str, "field": "Data Check", "value": "Checking..."})
        final_sql = prepare_spec_sql(base_sql, ev_type, plc_id)
        formatted_sql = final_sql.format(dt=val_params["date"], extra_where_conditions="")
        check_sql = f"SELECT 1 FROM ({formatted_sql}) AS cq LIMIT 1"
        try:
            with engine.connect() as conn:
                res = conn.execute(text(check_sql)).fetchone()
                has_data = bool(res)
        except Exception as e:
            has_data = False
            push_sse("tracker_update", {"spec": spec_str, "field": "Data Check", "value": f"🚨 Query Error"})

        shared["checked_count"] += 1
        if has_data:
            shared["found_count"] += 1
            shared["queue"].append(spec_str)
            push_sse("tracker_update", {"spec": spec_str, "field": "Data Check", "value": "Found"})
            push_sse("tracker_update", {"spec": spec_str, "field": "AI", "value": "Queued"})
        else:
            push_sse("tracker_update", {"spec": spec_str, "field": "Data Check", "value": "No Data"})
            push_sse("tracker_update", {"spec": spec_str, "field": "Result", "value": "Skipped"})
            shared["completed_count"] += 1
            push_sse("progress", {"completed": shared["completed_count"]})
        time.sleep(0.1)

    push_sse("status", {"text": f"DB scan done. Found {shared['found_count']}/{len(specs)}"})
    shared["is_done"] = True


def ai_worker(shared, full_df):
    while True:
        if shared["queue"]:
            spec_str = shared["queue"].pop(0)
            ev_type, plc_id = spec_str.split(" | ")
            suite_name = f"suite_{ev_type}_{plc_id}".replace(" ", "_").replace("/", "_").replace("\\", "_")
            push_sse("tracker_update", {"spec": spec_str, "field": "AI", "value": "Generating..."})
            try:
                df_filtered = full_df[(full_df['event type'] == ev_type) & (full_df['placement_id'] == plc_id)].copy()
                flatten_map = {}
                if 'group' in df_filtered.columns:
                    for _, row in df_filtered.iterrows():
                        grp = row.get('group')
                        field = row.get('Field Name')
                        if pd.notna(grp) and str(grp).strip() and pd.notna(field):
                            g, f_name = str(grp).strip(), str(field).strip()
                            if g not in flatten_map:
                                flatten_map[g] = []
                            if f_name not in flatten_map[g]:
                                flatten_map[g].append(f_name)
                    df_filtered['Field Name'] = df_filtered.apply(
                        lambda row: f"{row['group']}_{row['Field Name']}" if pd.notna(row.get('group')) and str(row.get('group','')).strip() else row['Field Name'], axis=1)

                important_cols = ['Field Name', 'Type', 'Mandatory', 'Allowed Values']
                cols_to_keep = [c for c in important_cols if c in df_filtered.columns]
                spec_md = df_filtered[cols_to_keep].to_markdown(index=False)
                with open("prompt_template.txt", "r", encoding="utf-8") as f:
                    prompt_template = f.read()
                prompt = prompt_template.format(auto_spec_input=spec_md)

                retries = shared.get("retry_count", {}).get(spec_str, 0)
                if retries > 0:
                    last_err = shared.get("last_gx_error", {}).get(spec_str, "Unknown")
                    prompt += f"\n\n🚨 [SYSTEM WARNING]: Previous JSON error:\n{last_err}\nPlease fix."

                model_name = os.getenv('GEMINI_MODEL_ID', 'gemini-2.5-flash')
                model = genai.GenerativeModel(model_name)
                response = None
                for attempt in range(5):
                    try:
                        response = model.generate_content(prompt)
                        break
                    except Exception as e:
                        if "429" in str(e) or "ResourceExhausted" in str(e):
                            wait_time = 5 * (2 ** attempt)
                            push_sse("tracker_update", {"spec": spec_str, "field": "AI", "value": f"⏳ Rate limited... {wait_time}s"})
                            time.sleep(wait_time)
                        else:
                            raise

                match = re.search(r'\[.*\]', response.text, re.DOTALL)
                if match:
                    final_expectations = match.group(0)
                    try:
                        parsed = json.loads(final_expectations)
                    except json.JSONDecodeError as je:
                        if "escape" in str(je).lower():
                            fixed = re.sub(r'(?<!\\)\\([sdwSDWbB])', r'\\\\\\1', final_expectations)
                            parsed = json.loads(fixed)
                            final_expectations = fixed
                        else:
                            raise
                    exp_path = os.path.join(EXPECTATIONS_DIR, f"{suite_name}.json")
                    with open(exp_path, 'w', encoding='utf-8') as ef:
                        json.dump(parsed, ef, indent=4, ensure_ascii=False)

                    push_sse("tracker_update", {"spec": spec_str, "field": "AI", "value": "Done"})
                    push_sse("tracker_update", {"spec": spec_str, "field": "Rules", "value": str(len(parsed))})
                    push_sse("tracker_update", {"spec": spec_str, "field": "GX", "value": "Queued"})
                    shared["ai_queue"].append({
                        "spec_str": spec_str, "ev_type": ev_type, "plc_id": plc_id,
                        "suite_name": suite_name, "expectations": final_expectations,
                        "flatten_map": flatten_map, "prompt": prompt,
                        "ai_response": response.text if response else "", "rule_count": len(parsed),
                    })
                else:
                    push_sse("tracker_update", {"spec": spec_str, "field": "AI", "value": "No JSON"})
                    push_sse("tracker_update", {"spec": spec_str, "field": "Result", "value": "AI Error"})
                    shared["completed_count"] += 1
                    push_sse("progress", {"completed": shared["completed_count"]})
            except Exception as e:
                err_msg = f"{type(e).__name__}: {str(e)[:40]}"
                push_sse("tracker_update", {"spec": spec_str, "field": "AI", "value": f"{type(e).__name__}"})
                push_sse("tracker_update", {"spec": spec_str, "field": "Result", "value": f"{err_msg}"})
                shared["completed_count"] += 1
                push_sse("progress", {"completed": shared["completed_count"]})
            time.sleep(0.1)
        elif shared["is_done"] and not shared["queue"]:
            # Don't exit yet if GX workers are still active — they might retry items back
            if shared["gx_active_count"] == 0 and not shared["ai_queue"]:
                break
            else:
                time.sleep(0.5)
    shared["ai_is_done"] = True


def gx_worker(shared, val_params_tuple, sql_cfg, db_config):
    while True:
        item = None
        try:
            if shared["ai_queue"]:
                item = shared["ai_queue"].pop(0)
            if item:
                shared["gx_active_count"] += 1
                spec_str = item["spec_str"]
                ev_type, plc_id = item["ev_type"], item["plc_id"]
                suite_name = item["suite_name"]
                push_sse("tracker_update", {"spec": spec_str, "field": "GX", "value": "Running..."})

                base_sql = ""
                if sql_cfg["source"] == "file":
                    with open(os.path.join(SQL_DIR, sql_cfg["file"]), 'r', encoding='utf-8') as f:
                        base_sql = f.read()
                else:
                    base_sql = sql_cfg["custom_sql"]
                final_sql = prepare_spec_sql(base_sql, ev_type, plc_id)

                cp_result, data_samples, docs_info, proc_log, run_name, _ = run_validation_process(
                    final_sql, item["expectations"], suite_name, db_config, val_params_tuple,
                    flatten_map=item["flatten_map"], batch_id=shared.get("batch_id"))

                run_id = str(uuid.uuid4())
                status = "SUCCESS" if cp_result and cp_result.success else ("ERROR" if cp_result is None else "FAILURE")
                log_data = cp_result.to_json_dict() if cp_result else {"error": "Process failed"}

                run_data = {
                    "id": run_id, "name": suite_name,
                    "timestamp": datetime.datetime.now().isoformat(),
                    "ev_type": ev_type, "plc_id": plc_id,
                    "status": status, "run_name": run_name,
                    "log_data": log_data, "process_log": proc_log,
                    "ai_prompt": item.get("prompt", ""),
                    "ai_response": item.get("ai_response", ""),
                    "spec_str": spec_str,
                }
                # Log to file
                safe_ev = ev_type.replace('/', '_')
                safe_plc = plc_id.replace('/', '_')
                log_fname = f"run_{shared.get('batch_id','x')}_{safe_ev}_{safe_plc}_{run_id[:8]}.json"
                log_fpath = os.path.join(LOGS_DIR, log_fname)
                # Enrich log with spec metadata
                uploaded_fn = shared.get("uploaded_filename", "")
                # Derive file path: "linepay_user_web_topup_insufficient - mini.csv" → "linepay_user/web/topup_insufficient"
                spec_file_path = ""
                if uploaded_fn:
                    base = uploaded_fn.rsplit('.', 1)[0]  # remove .csv/.xlsx
                    base = base.split(' - ')[0].strip()    # remove " - mini" etc
                    parts = base.split('_')
                    # Rebuild with / separators (first part is product, rest map to hierarchy)
                    if len(parts) >= 3:
                        spec_file_path = '/'.join(parts)
                    else:
                        spec_file_path = base
                log_data["spec_file_path"] = spec_file_path
                log_data["spec_str"] = spec_str
                log_data["ev_type"] = ev_type
                log_data["plc_id"] = plc_id
                log_data["uploaded_filename"] = uploaded_fn
                with open(log_fpath, 'w', encoding='utf-8') as f:
                    json.dump(log_data, f, indent=4)
                run_data["log_filepath"] = log_fpath

                shared["results"].append(run_data)

                # Update tracker via SSE
                if cp_result:
                    total_e, passed_e = 0, 0
                    for _, vr in cp_result.run_results.items():
                        for r in vr['validation_result'].results:
                            total_e += 1
                            if r.success:
                                passed_e += 1
                    pct = round(passed_e / total_e * 100) if total_e > 0 else 0
                    push_sse("tracker_update", {"spec": spec_str, "field": "Pass/Fail", "value": f"{passed_e}/{total_e} ({pct}%)"})
                    push_sse("tracker_update", {"spec": spec_str, "field": "GX", "value": "Done"})
                else:
                    push_sse("tracker_update", {"spec": spec_str, "field": "GX", "value": "Error"})

                result_text = "Passed" if status == "SUCCESS" else ("Error" if status == "ERROR" else "Mismatch")
                push_sse("tracker_update", {"spec": spec_str, "field": "Result", "value": result_text})
                shared["completed_count"] += 1
                push_sse("progress", {"completed": shared["completed_count"]})
                shared["gx_active_count"] -= 1

            elif shared["ai_is_done"] and not shared["ai_queue"]:
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
                    push_sse("tracker_update", {"spec": spec_str, "field": "GX", "value": "Retrying AI..."})
                    shared["queue"].insert(0, spec_str)
                else:
                    push_sse("tracker_update", {"spec": spec_str, "field": "GX", "value": "Error"})
                    push_sse("tracker_update", {"spec": spec_str, "field": "Result", "value": f"{type(e).__name__}"})
                    shared["results"].append({
                        "id": str(uuid.uuid4()), "name": item["suite_name"],
                        "ev_type": item["ev_type"], "plc_id": item["plc_id"],
                        "status": "ERROR", "error": str(e), "traceback": traceback.format_exc(),
                        "spec_str": spec_str, "log_data": {},
                    })
                    shared["completed_count"] += 1
                    push_sse("progress", {"completed": shared["completed_count"]})
                shared["gx_active_count"] -= 1
            time.sleep(1)


# ─── Template Context ───────────────────────────────────────

@app.context_processor
def inject_globals():
    # Build batch history for sidebar
    history_items = []
    for b in app_state["batch_history"]:
        results = b.get("results", [])
        s = sum(1 for r in results if r.get("status") == "SUCCESS")
        history_items.append({
            "id": b["id"], "spec_file": b.get("spec_file", "batch"),
            "date_queried": b.get("date_queried", ""),
            "all_pass": s == len(results) and len(results) > 0,
            "has_pass": s > 0,
        })
    return {
        "batch_history": history_items,
        "active_page": getattr(request, '_active_page', ''),
        "active_batch_id": getattr(request, '_active_batch_id', ''),
    }


# ─── Routes ─────────────────────────────────────────────────

@app.route('/', methods=['GET', 'POST'])
def upload_page():
    request._active_page = 'upload'
    if request.method == 'POST':
        f = request.files.get('spec_file')
        if not f or f.filename == '':
            flash("Please select a file", "error")
            return redirect(url_for('upload_page'))
        try:
            if f.filename.endswith('.csv'):
                df = pd.read_csv(f)
            else:
                df = pd.read_excel(f)
            df.dropna(how='all', inplace=True)
            df.dropna(axis=1, how='all', inplace=True)
            if 'event_type' in df.columns:
                df.rename(columns={'event_type': 'event type'}, inplace=True)
            if 'placement id' in df.columns:
                df.rename(columns={'placement id': 'placement_id'}, inplace=True)
            if 'event type' not in df.columns or 'placement_id' not in df.columns:
                flash("File must have 'event type' and 'placement_id' columns", "error")
                return redirect(url_for('upload_page'))
            rename_map = {'M': 'Mandatory', 'Data type': 'Type', 'value': 'Allowed Values', 'key': 'Field Name'}
            df.rename(columns=rename_map, inplace=True)
            specs = df[['event type', 'placement_id']].drop_duplicates().dropna()
            spec_strings = [f"{r['event type']} | {r['placement_id']}" for _, r in specs.iterrows()]

            app_state["full_df"] = df
            app_state["uploaded_filename"] = f.filename
            app_state["unique_specs"] = spec_strings
            session["configured"] = True
            return redirect(url_for('configure_page'))
        except Exception as e:
            flash(f"Error reading file: {e}", "error")
            return redirect(url_for('upload_page'))
    return render_template('upload.html')


@app.route('/configure')
def configure_page():
    request._active_page = 'configure'
    if not app_state.get("unique_specs"):
        return redirect(url_for('upload_page'))
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    return render_template('configure.html',
                           filename=app_state.get("uploaded_filename", ""),
                           specs=app_state["unique_specs"],
                           default_date=yesterday,
                           sql_files=list_files(SQL_DIR, "sql"))


@app.route('/start', methods=['POST'])
def start_pipeline():
    selected = request.form.getlist('selected_specs')
    if not selected:
        flash("Please select at least one spec", "error")
        return redirect(url_for('configure_page'))

    query_date = request.form.get('query_date', '')
    segment_by = request.form.get('segment_by', '')
    sql_source = request.form.get('sql_source', 'custom')
    sql_file = request.form.get('sql_file', '')
    custom_sql = request.form.get('custom_sql', '')

    batch_id = str(uuid.uuid4())[:8]
    segments = [s.strip() for s in segment_by.split(',') if s.strip()]

    shared = {
        "queue": [], "ai_queue": [], "results": [],
        "is_done": False, "ai_is_done": False,
        "checked_count": 0, "found_count": 0, "completed_count": 0,
        "gx_active_count": 0, "batch_id": batch_id,
        "retry_count": {}, "last_gx_error": {},
        "tracker": {},
        "uploaded_filename": app_state.get("uploaded_filename", ""),
    }
    for spec in selected:
        ev, plc = spec.split(" | ")
        shared["tracker"][spec] = {
            "Event Type": ev, "Placement ID": plc,
            "Data Check": "Queued", "AI": "-", "Rules": "-",
            "GX": "-", "Pass/Fail": "-", "Result": "-",
        }

    sql_cfg = {
        "source": sql_source,
        "file": sql_file,
        "custom_sql": custom_sql,
    }
    val_params = {"date": query_date, "segments": segments}

    app_state["pipeline_shared"] = shared
    app_state["current_batch_id"] = batch_id
    app_state["sse_events"] = []  # Reset SSE queue
    app_state["_pipeline_meta"] = {
        "selected_specs": selected, "sql_cfg": sql_cfg, "val_params": val_params,
    }

    # Start background threads
    t1 = threading.Thread(target=db_checker_worker, args=(selected, sql_cfg, val_params, DB_CONFIG, shared), daemon=True)
    t2 = threading.Thread(target=ai_worker, args=(shared, app_state["full_df"].copy()), daemon=True)
    t1.start()
    t2.start()
    for _ in range(3):
        threading.Thread(target=gx_worker, args=(shared, (query_date, segments), sql_cfg, DB_CONFIG), daemon=True).start()

    # Monitor thread: finalizes batch when done
    def finalize_when_done():
        while not shared["ai_is_done"] or shared["gx_active_count"] > 0 or shared["ai_queue"]:
            time.sleep(1)
        time.sleep(1)  # extra wait for last results
        batch = {
            "id": batch_id,
            "timestamp": datetime.datetime.now().isoformat(),
            "spec_file": app_state.get("uploaded_filename", ""),
            "date_queried": query_date,
            "spec_count": len(selected),
            "results": list(shared["results"]),
            "tracker": dict(shared["tracker"]),
            "waived_rules": set(),
        }
        try:
            pdf_path = generate_batch_pdf(batch)
            batch["pdf_path"] = pdf_path
        except Exception as e:
            batch["pdf_error"] = str(e)

        app_state["batches"][batch_id] = batch
        app_state["batch_history"].insert(0, batch)
        push_sse("done", {"batch_id": batch_id})

    threading.Thread(target=finalize_when_done, daemon=True).start()

    session["pipeline_started"] = True
    return redirect(url_for('pipeline_page'))


@app.route('/pipeline')
def pipeline_page():
    request._active_page = 'pipeline'
    shared = app_state.get("pipeline_shared")
    if not shared:
        return redirect(url_for('upload_page'))
    specs = list(shared["tracker"].keys())
    return render_template('pipeline.html', total_specs=len(specs), specs=specs)


@app.route('/api/stream')
def pipeline_stream():
    """SSE endpoint for real-time pipeline updates."""
    def event_stream():
        last_idx = 0
        while True:
            with app_state["sse_lock"]:
                events = app_state["sse_events"][last_idx:]
                last_idx = len(app_state["sse_events"])
            for event_type, data in events:
                yield f"event: {event_type}\ndata: {data}\n\n"
                if event_type == "done":
                    return
            time.sleep(0.3)
    return Response(event_stream(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


# --- Migrate old waived rules format to new format ---
_SHORT_TO_FULL = {
    "match_regex": "expect_column_values_to_match_regex",
    "not_match_regex": "expect_column_values_to_not_match_regex",
    "be_in_set": "expect_column_values_to_be_in_set",
    "not_be_null": "expect_column_values_to_not_be_null",
    "be_null": "expect_column_values_to_be_null",
    "match_json_schema": "expect_column_values_to_match_json_schema",
    "be_unique": "expect_column_values_to_be_unique",
    "be_between": "expect_column_values_to_be_between",
    "be_of_type": "expect_column_values_to_be_of_type",
    "table_match_set": "expect_table_columns_to_match_set",
}

def _migrate_waived_rules(waived):
    """Convert old format 'col (short)' to new format 'col|full_type'."""
    migrated = set()
    changed = False
    for rule in waived:
        if '|' in rule:
            migrated.add(rule)  # already new format
        elif ' (' in rule and rule.endswith(')'):
            parts = rule.rsplit(' (', 1)
            col = parts[0]
            short = parts[1].rstrip(')')
            full = _SHORT_TO_FULL.get(short, f"expect_column_values_to_{short}")
            migrated.add(f"{col}|{full}")
            changed = True
        else:
            migrated.add(rule)
    return migrated, changed


@app.route('/results/<batch_id>')
def results_page(batch_id):
    request._active_page = 'results'
    request._active_batch_id = batch_id
    batch = app_state["batches"].get(batch_id)
    if not batch:
        flash("Batch not found", "error")
        return redirect(url_for('upload_page'))

    results = batch.get("results", [])
    waived = batch.get("waived_rules", set())

    # Auto-migrate old format waived rules
    waived, migrated = _migrate_waived_rules(waived)
    if migrated:
        batch["waived_rules"] = waived
        # Also update JSON log files
        for r in results:
            log_path = r.get("log_filepath")
            if log_path and os.path.exists(log_path):
                try:
                    with open(log_path, 'r', encoding='utf-8') as f:
                        log_data = json.load(f)
                    log_data["waived_rules"] = list(waived)
                    with open(log_path, 'w', encoding='utf-8') as f:
                        json.dump(log_data, f, indent=2, ensure_ascii=False)
                except Exception:
                    pass

    metrics = get_effective_metrics_from_results(results, waived)
    tracker_rows = get_tracker_rows(batch.get("tracker", {}), results, waived)
    failed_items = get_failed_summary(results, waived)

    # Enrich each result with failed expectations for display
    for r in results:
        if r.get("log_data"):
            r["failed_expectations"] = extract_failed_from_log(r["log_data"], waived)
            eff = get_effective_metrics_from_results([r], waived)
            r["eff_status"] = "SUCCESS" if eff["passed"] > 0 else ("ERROR" if eff["error"] > 0 else "FAILURE")
        else:
            r["failed_expectations"] = []
            r["eff_status"] = r.get("status", "ERROR")

    return render_template('results.html', batch=batch, metrics=metrics,
                           tracker_rows=tracker_rows, results=results,
                           failed_items=failed_items, waived_rules=waived)


@app.route('/waive/<batch_id>', methods=['POST'])
def waive_rules_action(batch_id):
    batch = app_state["batches"].get(batch_id)
    if not batch:
        flash("Batch not found", "error")
        return redirect(url_for('upload_page'))

    waive_list = request.form.getlist('waive_rules')
    batch["waived_rules"] = set(waive_list)

    # Persist waived rules to JSON log files
    for r in batch.get("results", []):
        log_path = r.get("log_filepath")
        if log_path and os.path.exists(log_path):
            try:
                with open(log_path, 'r', encoding='utf-8') as f:
                    log_data = json.load(f)
                log_data["waived_rules"] = list(waive_list)
                with open(log_path, 'w', encoding='utf-8') as f:
                    json.dump(log_data, f, indent=2, ensure_ascii=False)
            except Exception:
                pass

    # Regenerate PDF
    try:
        pdf_path = generate_batch_pdf(batch)
        batch["pdf_path"] = pdf_path
        flash("Waived rules updated and report regenerated!", "success")
    except Exception as e:
        flash(f"Waived rules saved but PDF failed: {e}", "warning")

    return redirect(url_for('results_page', batch_id=batch_id))


@app.route('/download/<batch_id>')
def download_pdf(batch_id):
    batch = app_state["batches"].get(batch_id)
    if not batch or not batch.get("pdf_path"):
        flash("PDF not available", "error")
        return redirect(url_for('results_page', batch_id=batch_id))
    pdf_path = os.path.abspath(batch["pdf_path"])
    if not os.path.exists(pdf_path):
        flash("PDF file not found on disk", "error")
        return redirect(url_for('results_page', batch_id=batch_id))
    filename = os.path.basename(pdf_path)
    with open(pdf_path, 'rb') as f:
        pdf_data = f.read()
    response = Response(pdf_data, mimetype='application/pdf')
    response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    response.headers['Content-Length'] = len(pdf_data)
    return response


# ─── Run ────────────────────────────────────────────────────

if __name__ == '__main__':
    print("🚀 Starting Tracking Validation Dashboard...")
    print("   Open http://localhost:5050 in your browser")
    app.run(host='0.0.0.0', port=5050, debug=True, threaded=True)
