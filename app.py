"""
DataFixer - Automated Data Quality Assessment and Repair System Using Apache Spark
=================================================================================
Run with:  streamlit run app.py

Pages: Upload -> Dashboard -> Data Repair -> Download

How Spark is used
-----------------
* Uploaded files are parsed once with pandas (CSV / XLSX / XLS), then converted
  into a Spark DataFrame with an explicit schema (so mixed types and NaN values
  are handled safely).
* ALL quality analysis (null counts, blank counts, distinct counts, duplicates)
  and ALL cleaning (trim, lowercase, dedupe, median/"Unknown" fills) are real
  PySpark DataFrame operations.
* Data is only brought back to pandas for small previews/charts, or when the
  user explicitly prepares a CSV / Excel download.
"""

import html
import io
import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    NumericType,
    StringType,
    StructField,
    StructType,
)

# ============================================================
# CONFIGURATION
# ============================================================

ACCENT = "#FF5436"
MAX_INPUT_ROWS = 1_000_000      # refuse files larger than this (they are parsed in memory by pandas)
EXPORT_ROW_LIMIT = 250_000      # max rows converted to pandas for CSV/Excel download
EXCEL_HARD_LIMIT = 1_048_575    # Excel's own row limit
PREVIEW_ROWS = 100
HISTOGRAM_SAMPLE = 50_000
SCORE_WEIGHTS = {"completeness": 0.60, "uniqueness": 0.40}
BLANK_PATTERN = r"[\s\u00A0]"   # whitespace characters (incl. non-breaking space)
ILLEGAL_EXCEL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

st.set_page_config(page_title="DataFixer", page_icon="🛠️", layout="wide")


class DataFixerError(Exception):
    """Friendly, user-facing error."""


# ============================================================
# STYLING
# ============================================================

def inject_css() -> None:
    st.markdown(
        f"""
        <style>
        .stApp {{ background-color: #0E1117; }}
        [data-testid="stSidebar"] {{ background-color: #141922; border-right: 1px solid #262c38; }}
        h1, h2, h3, h4 {{ letter-spacing: -0.01em; }}
        .hero {{
            padding: 1.4rem 1.6rem; border-radius: 16px; margin-bottom: 1.2rem;
            background: linear-gradient(135deg, #1b2130 0%, #141922 60%, {ACCENT}22 100%);
            border: 1px solid #262c38;
        }}
        .hero h1 {{ margin: 0; font-size: 2rem; }}
        .hero p {{ margin: .3rem 0 0 0; color: #a9b1c1; }}
        .accent {{ color: {ACCENT}; }}
        .kpi-card {{
            background: #171c27; border: 1px solid #262c38; border-left: 4px solid {ACCENT};
            border-radius: 12px; padding: 14px 16px; height: 100%;
        }}
        .kpi-label {{ color: #a9b1c1; font-size: .78rem; text-transform: uppercase; letter-spacing: .06em; }}
        .kpi-value {{ color: #ffffff; font-size: 1.8rem; font-weight: 700; line-height: 1.2; }}
        .kpi-sub {{ color: #7d8699; font-size: .78rem; }}
        div.stButton > button[kind="primary"], div.stDownloadButton > button[kind="primary"] {{
            background-color: {ACCENT}; border-color: {ACCENT}; color: white; font-weight: 600;
        }}
        div.stButton > button[kind="primary"]:hover, div.stDownloadButton > button[kind="primary"]:hover {{
            background-color: #e0432a; border-color: #e0432a;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def kpi_card(label: str, value, sub: str = "") -> None:
    st.markdown(
        f'<div class="kpi-card"><div class="kpi-label">{html.escape(label)}</div>'
        f'<div class="kpi-value">{html.escape(str(value))}</div>'
        f'<div class="kpi-sub">{html.escape(sub)}&nbsp;</div></div>',
        unsafe_allow_html=True,
    )


def style_figure(fig: go.Figure, height: int = 360) -> go.Figure:
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=height,
        margin=dict(l=10, r=10, t=50, b=10),
        font=dict(color="#d7dbe4"),
    )
    return fig


# ============================================================
# SPARK SESSION
# ============================================================

@st.cache_resource(show_spinner=False)
def get_spark() -> SparkSession:
    """Create (once) a local Spark session. Raises DataFixerError with help text on failure."""
    # Make Spark workers use the same Python as Streamlit (important on Windows + .venv).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        raise DataFixerError(
            "Java was not found. Apache Spark needs a JDK (17 is recommended). "
            "Install it (e.g. Temurin 17), restart VS Code, and make sure `java -version` works in the terminal."
        )
    try:
        return (
            SparkSession.builder.appName("DataFixer")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "2g")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as error:  # noqa: BLE001 - we want to show any startup failure nicely
        raise DataFixerError(f"Apache Spark could not start: {error}") from error


# ============================================================
# FILE LOADING  (pandas parse -> Spark DataFrame)
# ============================================================

def sanitize_columns(columns) -> list:
    """Make column names safe for Spark: non-empty, no dots, unique (case-insensitive)."""
    seen, cleaned = set(), []
    for position, raw in enumerate(columns, start=1):
        name = str(raw).strip().replace(".", "_")
        if not name or name.lower().startswith("unnamed:"):
            name = f"column_{position}"
        candidate, counter = name, 1
        while candidate.lower() in seen:
            counter += 1
            candidate = f"{name}_{counter}"
        seen.add(candidate.lower())
        cleaned.append(candidate)
    return cleaned


def read_uploaded_file(uploaded_file) -> pd.DataFrame:
    """Parse CSV / XLSX / XLS / JSON into a pandas DataFrame with friendly errors."""
    extension = Path(uploaded_file.name).suffix.lower()
    raw_bytes = uploaded_file.getvalue()
    if not raw_bytes:
        raise DataFixerError("The uploaded file is empty.")

    try:
        if extension == ".csv":
            pdf = None
            for encoding in ("utf-8-sig", "utf-8", "latin-1"):
                try:
                    pdf = pd.read_csv(io.BytesIO(raw_bytes), encoding=encoding, low_memory=False)
                    if pdf.shape[1] == 1:  # maybe ; or tab delimited -> let pandas sniff it
                        pdf = pd.read_csv(io.BytesIO(raw_bytes), encoding=encoding, sep=None, engine="python")
                    break
                except UnicodeDecodeError:
                    continue
            if pdf is None:
                raise DataFixerError("Could not decode the CSV file (tried UTF-8 and Latin-1).")
        elif extension == ".xlsx":
            pdf = pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl")
        elif extension == ".xls":
            try:
                pdf = pd.read_excel(io.BytesIO(raw_bytes), engine="xlrd")
            except ImportError as error:
                raise DataFixerError(
                    "Reading .xls files needs the `xlrd` package. Run: pip install xlrd"
                ) from error
        elif extension == ".json":
            pdf = pd.read_json(io.BytesIO(raw_bytes))
        else:
            raise DataFixerError(f"Unsupported file type '{extension}'. Upload CSV, XLSX, XLS or JSON.")
    except DataFixerError:
        raise
    except pd.errors.EmptyDataError as error:
        raise DataFixerError("The file contains no data.") from error
    except Exception as error:  # noqa: BLE001
        raise DataFixerError(f"The file could not be read. It may be corrupted or invalid. ({error})") from error

    if pdf.shape[1] == 0 or pdf.shape[0] == 0:
        raise DataFixerError("The dataset is empty (no rows or no columns).")
    if pdf.shape[0] > MAX_INPUT_ROWS:
        raise DataFixerError(
            f"The dataset has {pdf.shape[0]:,} rows; this app accepts up to {MAX_INPUT_ROWS:,} rows. "
            "Please upload a smaller sample."
        )
    pdf.columns = sanitize_columns(pdf.columns)
    return pdf


def pandas_to_spark(spark: SparkSession, pdf: pd.DataFrame) -> DataFrame:
    """
    Convert pandas -> Spark with an explicit schema:
      bool -> Boolean, integer -> Long, float -> Double, everything else -> String.
    NaN / NaT / pd.NA become real NULLs. Mixed-type and unsupported columns become strings.
    """
    fields, prepared = [], {}
    for column in pdf.columns:
        series = pdf[column]
        if pd.api.types.is_bool_dtype(series):
            fields.append(StructField(column, BooleanType(), True))
            prepared[column] = series
        elif pd.api.types.is_integer_dtype(series):
            fields.append(StructField(column, LongType(), True))
            prepared[column] = series
        elif pd.api.types.is_float_dtype(series):
            fields.append(StructField(column, DoubleType(), True))
            prepared[column] = series
        else:
            fields.append(StructField(column, StringType(), True))
            null_mask = series.isna()
            prepared[column] = series.astype(str).where(~null_mask, None)

    prepared_pdf = pd.DataFrame(prepared)
    records = prepared_pdf.astype(object).where(prepared_pdf.notna(), None).values.tolist()
    # Cast numpy scalars to plain Python values so Spark's type verification accepts them
    records = [
        [value.item() if isinstance(value, np.generic) else value for value in row] for row in records
    ]
    return spark.createDataFrame(records, schema=StructType(fields)).cache()


# ============================================================
# QUALITY ANALYSIS  (Spark)
# ============================================================

def q(name: str) -> str:
    """Back-quote a column name for safe use in F.col()."""
    return "`" + name.replace("`", "``") + "`"


def blank_expr(column):
    """True when a string cell is non-null but contains only whitespace."""
    return column.isNotNull() & (F.length(F.regexp_replace(column, BLANK_PATTERN, "")) == 0)


def analyze_quality(df: DataFrame) -> dict:
    """
    Compute dataset- and column-level quality metrics with Spark aggregations.

    Score = 0.60 * Completeness + 0.40 * Uniqueness
      Completeness = (1 - (missing + empty cells) / total cells) * 100
      Uniqueness   = (distinct rows / total rows) * 100
    """
    fields = df.schema.fields
    aggregations = [F.count(F.lit(1)).alias("rows")]
    for index, field in enumerate(fields):
        column = F.col(q(field.name))
        aggregations.append(F.sum(F.when(column.isNull(), 1).otherwise(0)).alias(f"m{index}"))
        if isinstance(field.dataType, StringType):
            aggregations.append(F.sum(F.when(blank_expr(column), 1).otherwise(0)).alias(f"e{index}"))
        aggregations.append(F.approx_count_distinct(column).alias(f"u{index}"))

    stats = df.agg(*aggregations).collect()[0].asDict()
    rows = int(stats["rows"])
    if rows == 0:
        raise DataFixerError("The dataset has no rows.")

    unique_rows = df.dropDuplicates().count()
    duplicates = rows - unique_rows

    column_rows, missing_total, empty_total = [], 0, 0
    for index, field in enumerate(fields):
        missing = int(stats.get(f"m{index}") or 0)
        empty = int(stats.get(f"e{index}") or 0)
        missing_total += missing
        empty_total += empty
        column_rows.append(
            {
                "Column": field.name,
                "Type": field.dataType.simpleString(),
                "Missing": missing,
                "Empty": empty,
                "Missing %": round(missing / rows * 100, 2),
                "Completeness %": round((1 - (missing + empty) / rows) * 100, 2),
                "Approx. Unique": int(stats.get(f"u{index}") or 0),
            }
        )

    total_cells = rows * len(fields)
    completeness = round((1 - (missing_total + empty_total) / total_cells) * 100, 2)
    uniqueness = round(unique_rows / rows * 100, 2)
    score = round(SCORE_WEIGHTS["completeness"] * completeness + SCORE_WEIGHTS["uniqueness"] * uniqueness, 2)

    column_table = pd.DataFrame(column_rows)
    return {
        "rows": rows,
        "columns": len(fields),
        "total_cells": total_cells,
        "missing_cells": missing_total,
        "empty_cells": empty_total,
        "duplicates": duplicates,
        "completeness": completeness,
        "uniqueness": uniqueness,
        "score": score,
        "empty_columns": column_table.loc[column_table["Completeness %"] == 0, "Column"].tolist(),
        "column_table": column_table,
    }



def analyze_advanced_issues(df: DataFrame, age_min: int = 0, age_max: int = 120,
                            rating_min: float = 0.0, rating_max: float = 10.0,
                            minimum_money: float = 0.0) -> pd.DataFrame:
    """Use Spark expressions to flag likely semantic errors and IQR outliers."""
    rows = []
    for field in df.schema.fields:
        name, lname = field.name, field.name.lower()
        col = F.col(q(name))
        condition, issue_type = None, None
        if any(token in lname for token in ("email", "e_mail", "mail_address")):
            issue_type = "Invalid email"
            value = col.cast("string")
            condition = value.isNotNull() & (F.trim(value) != "") & (~value.rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"))
        elif "age" in lname:
            issue_type = "Age outside allowed range"
            value = col.cast("double")
            condition = col.isNotNull() & value.isNotNull() & ((value < age_min) | (value > age_max))
        elif "rating" in lname or "score" in lname:
            issue_type = "Rating/score outside allowed range"
            value = col.cast("double")
            condition = col.isNotNull() & value.isNotNull() & ((value < rating_min) | (value > rating_max))
        elif any(token in lname for token in ("salary", "price", "amount", "cost", "revenue", "income", "payment")):
            issue_type = "Negative/low monetary value"
            value = col.cast("double")
            condition = col.isNotNull() & value.isNotNull() & (value < minimum_money)
        elif any(token in lname for token in ("date", "dob", "timestamp")):
            issue_type = "Invalid date"
            value = col.cast("string")
            condition = col.isNotNull() & (F.trim(value) != "") & F.to_timestamp(value).isNull()

        if condition is not None:
            try:
                matches = df.where(condition)
                count = matches.count()
                if count:
                    samples = [str(r[0]) for r in matches.select(col.cast("string")).limit(5).collect()]
                    rows.append({"Issue Type": issue_type, "Severity": "High", "Column": name,
                                 "Affected Rows": int(count), "Sample Values": ", ".join(samples),
                                 "Lower Bound": None, "Upper Bound": None})
            except Exception:
                pass

        if isinstance(field.dataType, NumericType) and not isinstance(field.dataType, BooleanType):
            try:
                quantiles = df.approxQuantile(name, [0.25, 0.75], 0.01)
                if len(quantiles) == 2:
                    q1, q3 = quantiles
                    iqr = q3 - q1
                    if iqr > 0:
                        low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                        outliers = df.where(col.isNotNull() & ((col < F.lit(low)) | (col > F.lit(high))))
                        count = outliers.count()
                        if count:
                            samples = [str(r[0]) for r in outliers.select(col.cast("string")).limit(5).collect()]
                            rows.append({"Issue Type": "Potential numeric outlier", "Severity": "Review",
                                         "Column": name, "Affected Rows": int(count),
                                         "Sample Values": ", ".join(samples),
                                         "Lower Bound": round(float(low), 4), "Upper Bound": round(float(high), 4)})
            except Exception:
                pass
    return pd.DataFrame(rows, columns=["Issue Type", "Severity", "Column", "Affected Rows",
                                       "Sample Values", "Lower Bound", "Upper Bound"])


def make_multisheet_report(cleaned_pdf: pd.DataFrame, issues: pd.DataFrame,
                           before: dict, after: dict) -> bytes:
    """Create an Excel report with cleaned data, issue list, and quality summary."""
    buffer = io.BytesIO()
    summary = pd.DataFrame([
        {"Stage": "Before", "Rows": before["rows"], "Columns": before["columns"],
         "Missing Cells": before["missing_cells"], "Empty Cells": before["empty_cells"],
         "Duplicates": before["duplicates"], "Completeness %": before["completeness"],
         "Uniqueness %": before["uniqueness"], "Quality Score": before["score"]},
        {"Stage": "After", "Rows": after["rows"], "Columns": after["columns"],
         "Missing Cells": after["missing_cells"], "Empty Cells": after["empty_cells"],
         "Duplicates": after["duplicates"], "Completeness %": after["completeness"],
         "Uniqueness %": after["uniqueness"], "Quality Score": after["score"]},
    ])
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        cleaned_pdf.to_excel(writer, index=False, sheet_name="Cleaned Data")
        issues.to_excel(writer, index=False, sheet_name="Detected Issues")
        summary.to_excel(writer, index=False, sheet_name="Quality Summary")
    return buffer.getvalue()


def score_label(score: float) -> str:
    if score >= 90:
        return "Excellent"
    if score >= 75:
        return "Good"
    if score >= 50:
        return "Needs repair"
    return "Poor"


# ============================================================
# DATA REPAIR  (Spark)
# ============================================================

def clean_dataset(
    df: DataFrame,
    remove_duplicates: bool,
    fill_missing: bool,
    trim_text: bool,
    standardize_case: bool,
) -> tuple:
    """
    Apply the selected repairs using Spark transformations. Returns (cleaned_df, details).
    Order matters: trim/lowercase first so near-duplicates become exact duplicates,
    then dedupe, then fill (so medians are computed on the final rows).
    """
    details = {"medians": {}, "skipped_numeric_columns": []}
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    numeric_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, NumericType) and not isinstance(f.dataType, BooleanType)
    ]
    result = df

    if trim_text and string_cols:
        result = result.select(
            *[
                F.regexp_replace(F.col(q(f.name)), r"^[\s\u00A0]+|[\s\u00A0]+$", "").alias(f.name)
                if f.name in string_cols else F.col(q(f.name))
                for f in df.schema.fields
            ]
        )

    if standardize_case and string_cols:
        result = result.select(
            *[
                F.lower(F.col(q(f.name))).alias(f.name) if f.name in string_cols else F.col(q(f.name))
                for f in df.schema.fields
            ]
        )

    if remove_duplicates:
        result = result.dropDuplicates()

    if fill_missing:
        result = result.cache()
        row_count = result.count()
        if row_count > 0:
            # Treat blank strings as missing so they can be filled too
            if string_cols:
                result = result.select(
                    *[
                        F.when(blank_expr(F.col(q(f.name))), F.lit(None).cast(StringType()))
                        .otherwise(F.col(q(f.name))).alias(f.name)
                        if f.name in string_cols else F.col(q(f.name))
                        for f in result.schema.fields
                    ]
                )
            # Median via Spark's approxQuantile (exact for <=100k rows, 0.1% error above that)
            if numeric_cols:
                error = 0.0 if row_count <= 100_000 else 0.001
                quantiles = result.approxQuantile(numeric_cols, [0.5], error)
                for name, quantile in zip(numeric_cols, quantiles):
                    if quantile:  # an all-null column returns [] -> nothing to fill with
                        details["medians"][name] = quantile[0]
                    else:
                        details["skipped_numeric_columns"].append(name)

            fill_exprs = []
            for field in result.schema.fields:
                column = F.col(q(field.name))
                if field.name in details["medians"]:
                    # cast keeps the original type (integer columns get a whole-number median)
                    fill_exprs.append(
                        F.coalesce(column, F.lit(details["medians"][field.name]).cast(field.dataType)).alias(field.name)
                    )
                elif field.name in string_cols:
                    fill_exprs.append(F.coalesce(column, F.lit("Unknown")).alias(field.name))
                else:
                    fill_exprs.append(column)
            result = result.select(*fill_exprs)

    return result.cache(), details


# ============================================================
# EXPORT HELPERS
# ============================================================

def make_report_bytes(original: dict, cleaned: dict, filename: str, options: dict) -> bytes:
    def summarize(quality):
        return {k: quality[k] for k in (
            "rows", "columns", "missing_cells", "empty_cells", "duplicates",
            "completeness", "uniqueness", "score",
        )}

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_file": filename,
        "scoring_formula": "score = 0.60 * completeness + 0.40 * uniqueness",
        "cleaning_options": options,
        "before": summarize(original),
        "after": summarize(cleaned),
        "columns_before": original["column_table"].to_dict(orient="records"),
        "columns_after": cleaned["column_table"].to_dict(orient="records"),
    }
    return json.dumps(report, indent=2, default=str).encode("utf-8")


def build_excel_bytes(pdf: pd.DataFrame) -> bytes:
    safe = pdf.copy()
    for column in safe.columns:  # strip control characters that openpyxl refuses to write
        if not pd.api.types.is_numeric_dtype(safe[column]):
            safe[column] = safe[column].map(lambda v: ILLEGAL_EXCEL_CHARS.sub("", v) if isinstance(v, str) else v)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        safe.to_excel(writer, index=False, sheet_name="Cleaned Data")
    return buffer.getvalue()


# ============================================================
# SHARED UI PIECES
# ============================================================

def render_comparison(before: dict, after: dict) -> None:
    metrics = ["Rows", "Missing cells", "Empty cells", "Duplicate rows", "Completeness (%)", "Uniqueness (%)", "Quality score"]
    keys = ["rows", "missing_cells", "empty_cells", "duplicates", "completeness", "uniqueness", "score"]
    table = pd.DataFrame(
        {
            "Metric": metrics,
            "Before": [before[k] for k in keys],
            "After": [after[k] for k in keys],
        }
    )
    table["Change"] = (table["After"] - table["Before"]).round(2)

    left, right = st.columns([1, 1])
    with left:
        st.dataframe(table, width="stretch", hide_index=True)
    with right:
        issue_keys = ["Missing cells", "Empty cells", "Duplicate rows"]
        issues = table[table["Metric"].isin(issue_keys)].melt(
            id_vars="Metric", value_vars=["Before", "After"], var_name="Stage", value_name="Count"
        )
        fig = px.bar(
            issues, x="Metric", y="Count", color="Stage", barmode="group", title="Issues before vs after",
            color_discrete_map={"Before": "#7d8699", "After": ACCENT},
        )
        st.plotly_chart(style_figure(fig, 300), width="stretch")

    score_fig = go.Figure(
        go.Bar(
            x=["Completeness", "Uniqueness", "Quality score"],
            y=[before["completeness"], before["uniqueness"], before["score"]],
            name="Before", marker_color="#7d8699",
        )
    )
    score_fig.add_trace(
        go.Bar(
            x=["Completeness", "Uniqueness", "Quality score"],
            y=[after["completeness"], after["uniqueness"], after["score"]],
            name="After", marker_color=ACCENT,
        )
    )
    score_fig.update_layout(barmode="group", title="Scores before vs after (out of 100)", yaxis=dict(range=[0, 105]))
    st.plotly_chart(style_figure(score_fig, 320), width="stretch")


def cleanup_cleaned_state() -> None:
    for key in ("cleaned_df", "cleaned_quality", "cleaning_options", "cleaning_details", "cleaned_preview", "exports"):
        st.session_state.pop(key, None)


# ============================================================
# APP START
# ============================================================

inject_css()

st.markdown(
    '<div class="hero"><h1>🛠️ Data<span class="accent">Fixer</span></h1>'
    "<p>Automated data quality assessment and repair, powered by Apache Spark.</p></div>",
    unsafe_allow_html=True,
)

# Start Spark (cached). Show a helpful message and stop if it fails.
try:
    with st.spinner("Starting Apache Spark..."):
        spark = get_spark()
except DataFixerError as spark_error:
    st.error("Apache Spark could not be initialized.")
    st.markdown(str(spark_error))
    st.stop()

page = st.sidebar.radio(
    "Navigation", ["Upload", "Dashboard", "Issue Report", "Data Repair", "Download"], label_visibility="collapsed"
)
st.sidebar.markdown("---")
st.sidebar.caption(f"Spark {spark.version} · local mode")

source_df = st.session_state.get("source_df")
quality = st.session_state.get("quality")
filename = st.session_state.get("filename", "dataset")

if source_df is not None and quality is not None:
    st.sidebar.markdown(f"**📄 {filename}**")
    st.sidebar.caption(f"{quality['rows']:,} rows · {quality['columns']} columns")
    if st.sidebar.button("🗑️ Clear dataset", width="stretch"):
        for key in ("source_df", "quality", "filename", "file_key", "source_preview", "advanced_issues", "advanced_issue_rules", "cleaning_history"):
            st.session_state.pop(key, None)
        cleanup_cleaned_state()
        st.rerun()

# ============================================================
# PAGE: UPLOAD
# ============================================================

if page == "Upload":
    st.markdown("### 📤 Upload Dataset")
    uploaded_file = st.file_uploader("Choose a CSV, XLSX, XLS or JSON file", type=["csv", "xlsx", "xls", "json"])

    if uploaded_file is not None:
        file_key = f"{uploaded_file.name}:{uploaded_file.size}"
        if st.session_state.get("file_key") != file_key:  # only re-process when the file changes
            try:
                with st.spinner("Reading file and loading it into Spark..."):
                    pandas_df = read_uploaded_file(uploaded_file)
                    spark_df = pandas_to_spark(spark, pandas_df)
                    new_quality = analyze_quality(spark_df)
                old_df = st.session_state.get("source_df")
                if old_df is not None:
                    old_df.unpersist()
                cleanup_cleaned_state()
                st.session_state["source_df"] = spark_df
                st.session_state["quality"] = new_quality
                st.session_state["filename"] = uploaded_file.name
                st.session_state["file_key"] = file_key
                st.session_state["source_preview"] = pandas_df.head(PREVIEW_ROWS)
                st.rerun()
            except DataFixerError as error:
                st.error(str(error))
            except Exception as error:  # noqa: BLE001
                st.error("Something went wrong while loading the dataset.")
                st.code(str(error))

    if st.session_state.get("source_df") is not None:
        quality = st.session_state["quality"]
        st.success(f"Loaded **{st.session_state['filename']}** into Spark.")
        if quality["rows"] > 200_000:
            st.warning("Large dataset: charts and previews use samples, and downloads are capped (see Download page).")
        a, b, c = st.columns(3)
        with a:
            kpi_card("Rows", f"{quality['rows']:,}")
        with b:
            kpi_card("Columns", quality["columns"])
        with c:
            kpi_card("Quality score", f"{quality['score']}/100", score_label(quality["score"]))
        st.markdown("#### Dataset preview")
        st.dataframe(st.session_state["source_preview"], width="stretch", hide_index=True)
        st.markdown("#### Column data types (as seen by Spark)")
        st.dataframe(quality["column_table"][["Column", "Type"]], width="stretch", hide_index=True)
        st.info("Open **Dashboard** in the sidebar to see the full quality analysis.")
    else:
        st.info("Upload a file to begin. Your data stays on your machine.")

# ============================================================
# PAGE: DASHBOARD
# ============================================================

elif page == "Dashboard":
    st.markdown("### 📊 Quality Dashboard")
    if source_df is None or quality is None:
        st.info("Go to **Upload** and load a dataset first.")
    else:
        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            kpi_card("Rows", f"{quality['rows']:,}", f"{quality['columns']} columns")
        with k2:
            kpi_card("Missing cells", f"{quality['missing_cells']:,}", "NULL / NaN values")
        with k3:
            kpi_card("Empty cells", f"{quality['empty_cells']:,}", "blank text values")
        with k4:
            kpi_card("Duplicate rows", f"{quality['duplicates']:,}", "exact repeats")
        with k5:
            kpi_card("Quality score", f"{quality['score']}/100", score_label(quality["score"]))

        st.write("")
        gauge_col, formula_col = st.columns([1, 1])
        with gauge_col:
            gauge = go.Figure(
                go.Indicator(
                    mode="gauge+number",
                    value=quality["score"],
                    title={"text": "Data Quality Score"},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": ACCENT},
                        "steps": [
                            {"range": [0, 50], "color": "#3a2022"},
                            {"range": [50, 75], "color": "#3a3120"},
                            {"range": [75, 100], "color": "#1f3a2a"},
                        ],
                    },
                )
            )
            st.plotly_chart(style_figure(gauge, 320), width="stretch")
        with formula_col:
            st.markdown("#### How the score is calculated")
            st.markdown(
                f"""
**Quality Score = 0.60 × Completeness + 0.40 × Uniqueness**

* **Completeness (%)** = (1 − (missing cells + empty cells) ÷ total cells) × 100
* **Uniqueness (%)** = (distinct rows ÷ total rows) × 100

**Your data:** completeness **{quality['completeness']}%**, uniqueness **{quality['uniqueness']}%**
→ 0.60 × {quality['completeness']} + 0.40 × {quality['uniqueness']} = **{quality['score']}**

*Missing = NULL/NaN. Empty = text cells containing only whitespace. Completeness is weighted higher
because gaps usually hurt analysis more than repeated rows. Distinct counts per column are approximate (HyperLogLog).*
"""
            )
            if quality["empty_columns"]:
                st.warning("Completely empty columns: " + ", ".join(quality["empty_columns"]))

        column_table = quality["column_table"]
        left, right = st.columns([3, 2])
        with left:
            problems = column_table[(column_table["Missing"] + column_table["Empty"]) > 0]
            if problems.empty:
                st.success("No missing or empty values were found. 🎉")
            else:
                chart_data = problems.melt(
                    id_vars="Column", value_vars=["Missing", "Empty"], var_name="Kind", value_name="Cells"
                )
                fig = px.bar(
                    chart_data, y="Column", x="Cells", color="Kind", orientation="h",
                    title="Missing and empty cells by column",
                    color_discrete_map={"Missing": ACCENT, "Empty": "#ffb199"},
                )
                fig.update_layout(barmode="stack", yaxis=dict(categoryorder="total ascending"))
                st.plotly_chart(style_figure(fig, max(320, 28 * len(problems) + 120)), width="stretch")
        with right:
            type_counts = column_table["Type"].value_counts().reset_index()
            type_counts.columns = ["Type", "Columns"]
            fig = px.pie(type_counts, names="Type", values="Columns", hole=0.55, title="Column data types",
                         color_discrete_sequence=[ACCENT, "#ff8a73", "#ffb199", "#7d8699", "#4a5266"])
            st.plotly_chart(style_figure(fig, 320), width="stretch")

        st.markdown("#### Column-level quality")
        st.dataframe(
            column_table,
            width="stretch",
            hide_index=True,
            column_config={
                "Completeness %": st.column_config.ProgressColumn(
                    "Completeness %", min_value=0, max_value=100, format="%.1f%%"
                ),
                "Missing %": st.column_config.NumberColumn("Missing %", format="%.2f%%"),
            },
        )

        st.markdown("#### Column explorer")
        selected = st.selectbox("Pick a column to explore", column_table["Column"].tolist())
        selected_type = column_table.loc[column_table["Column"] == selected, "Type"].iloc[0]
        try:
            column = F.col(q(selected))
            if selected_type in ("bigint", "double"):
                sample = (
                    source_df.select(column.alias("value")).where(column.isNotNull())
                    .limit(HISTOGRAM_SAMPLE).toPandas()
                )
                if sample.empty:
                    st.info("This column has no values to plot.")
                else:
                    fig = px.histogram(sample, x="value", nbins=40, title=f"Distribution of {selected} (sample)",
                                       color_discrete_sequence=[ACCENT])
                    st.plotly_chart(style_figure(fig), width="stretch")
            else:
                top = (
                    source_df.where(column.isNotNull()).groupBy(column.alias("value"))
                    .count().orderBy(F.desc("count")).limit(15).toPandas()
                )
                if top.empty:
                    st.info("This column has no values to plot.")
                else:
                    fig = px.bar(top, x="count", y="value", orientation="h", title=f"Top values in {selected}",
                                 color_discrete_sequence=[ACCENT])
                    fig.update_layout(yaxis=dict(autorange="reversed", type="category"))
                    st.plotly_chart(style_figure(fig), width="stretch")
        except Exception as error:  # noqa: BLE001
            st.error("This column could not be visualized.")
            st.code(str(error))

        with st.expander("Dataset preview"):
            st.dataframe(st.session_state["source_preview"], width="stretch", hide_index=True)

# ============================================================
# PAGE: ISSUE REPORT
# ============================================================

elif page == "Issue Report":
    st.markdown("### 🧪 Advanced Issue Report")
    if source_df is None or quality is None:
        st.info("Go to **Upload** and load a dataset first.")
    else:
        st.write("Detects likely invalid emails, dates, age/rating values, negative monetary values, and potential numeric outliers.")
        with st.expander("Validation rules", expanded=True):
            r1, r2, r3 = st.columns(3)
            with r1:
                age_min = st.number_input("Minimum age", min_value=0, max_value=150, value=0)
                age_max = st.number_input("Maximum age", min_value=1, max_value=200, value=120)
            with r2:
                rating_min = st.number_input("Minimum rating/score", value=0.0, step=0.5)
                rating_max = st.number_input("Maximum rating/score", value=10.0, step=0.5)
            with r3:
                minimum_money = st.number_input("Minimum allowed money value", value=0.0, step=100.0)
            st.caption("Checks are inferred from column names. Rename columns clearly (for example, email, age, rating, salary) for best results.")
        if age_max < age_min:
            st.error("Maximum age must be greater than or equal to minimum age.")
        elif rating_max < rating_min:
            st.error("Maximum rating must be greater than or equal to minimum rating.")
        else:
            if st.button("🔎 Scan dataset for advanced issues", type="primary", width="stretch"):
                try:
                    with st.spinner("Scanning with Apache Spark..."):
                        issue_df = analyze_advanced_issues(source_df, int(age_min), int(age_max),
                                                           float(rating_min), float(rating_max),
                                                           float(minimum_money))
                    st.session_state["advanced_issues"] = issue_df
                    st.session_state["advanced_issue_rules"] = {
                        "age_min": age_min, "age_max": age_max,
                        "rating_min": rating_min, "rating_max": rating_max,
                        "minimum_money": minimum_money,
                    }
                except Exception as error:
                    st.error("The issue scan failed.")
                    st.code(str(error))
            issue_df = st.session_state.get("advanced_issues")
            if issue_df is not None:
                if issue_df.empty:
                    st.success("No advanced issues were detected by the selected checks.")
                else:
                    total = int(issue_df["Affected Rows"].sum())
                    a, b, c = st.columns(3)
                    with a:
                        kpi_card("Issue categories", len(issue_df))
                    with b:
                        kpi_card("Affected-row counts", f"{total:,}", "Counts can overlap across issue types")
                    with c:
                        kpi_card("High severity", int((issue_df["Severity"] == "High").sum()))
                    f1, f2, f3 = st.columns(3)
                    with f1:
                        issue_types = st.multiselect("Issue type", sorted(issue_df["Issue Type"].unique()),
                                                     default=sorted(issue_df["Issue Type"].unique()))
                    with f2:
                        severities = st.multiselect("Severity", sorted(issue_df["Severity"].unique()),
                                                   default=sorted(issue_df["Severity"].unique()))
                    with f3:
                        column_search = st.text_input("Search column name")
                    filtered = issue_df[
                        issue_df["Issue Type"].isin(issue_types) & issue_df["Severity"].isin(severities)
                    ]
                    if column_search.strip():
                        filtered = filtered[filtered["Column"].str.contains(column_search.strip(), case=False, na=False)]
                    st.dataframe(filtered, width="stretch", hide_index=True)
                    st.download_button("⬇️ Download filtered issue report (CSV)",
                                       filtered.to_csv(index=False).encode("utf-8-sig"),
                                       file_name="datafixer_detected_issues.csv", mime="text/csv",
                                       width="stretch")
                    st.info("Outliers are only flagged for review. They are not automatically deleted because unusual values may be valid.")

# ============================================================
# PAGE: DATA REPAIR
# ============================================================

elif page == "Data Repair":
    st.markdown("### 🧹 Data Repair")
    if source_df is None or quality is None:
        st.info("Go to **Upload** and load a dataset first.")
    else:
        st.caption("Choose repairs, then apply them. Everything runs as Spark DataFrame transformations.")
        c1, c2 = st.columns(2)
        with c1:
            remove_duplicates = st.checkbox("Remove duplicate rows", value=True)
            trim_text = st.checkbox("Trim leading and trailing spaces", value=True)
            standardize_case = st.checkbox("Standardize text to lowercase", value=False)
        with c2:
            fill_missing = st.checkbox(
                "Fill missing values", value=True,
                help="Numeric columns use the median; text columns (including blank cells) use 'Unknown'.",
            )
            st.caption(
                "Medians use Spark `approxQuantile` (exact up to 100k rows, ~0.1% error above). "
                "Integer columns receive a whole-number median. Boolean columns are not filled."
            )

        if st.button("Apply Selected Repairs", type="primary", width="stretch"):
            if not any([remove_duplicates, fill_missing, trim_text, standardize_case]):
                st.warning("Select at least one repair option.")
            else:
                try:
                    with st.spinner("Applying repairs with Apache Spark..."):
                        cleaned_df, details = clean_dataset(
                            source_df, remove_duplicates, fill_missing, trim_text, standardize_case
                        )
                        cleaned_quality = analyze_quality(cleaned_df)
                        preview = cleaned_df.limit(PREVIEW_ROWS).toPandas()
                    previous = st.session_state.get("cleaned_df")
                    if previous is not None:
                        previous.unpersist()
                    st.session_state["cleaned_df"] = cleaned_df
                    st.session_state["cleaned_quality"] = cleaned_quality
                    st.session_state["cleaned_preview"] = preview
                    st.session_state["cleaning_details"] = details
                    st.session_state["cleaning_options"] = {
                        "remove_duplicates": remove_duplicates,
                        "fill_missing": fill_missing,
                        "trim_text": trim_text,
                        "standardize_case": standardize_case,
                    }
                    st.session_state.pop("exports", None)  # old exports are now stale
                    st.success("Selected cleaning operations completed.")
                except DataFixerError as error:
                    st.error(str(error))
                except Exception as error:  # noqa: BLE001
                    st.error("The selected cleaning operations could not be completed.")
                    st.code(str(error))

        cleaned_quality = st.session_state.get("cleaned_quality")
        if cleaned_quality is not None:
            r1, r2, r3 = st.columns(3)
            with r1:
                kpi_card("Rows", f"{cleaned_quality['rows']:,}", f"{cleaned_quality['rows'] - quality['rows']:+,} vs before")
            with r2:
                kpi_card("Missing + empty cells", f"{cleaned_quality['missing_cells'] + cleaned_quality['empty_cells']:,}",
                         f"was {quality['missing_cells'] + quality['empty_cells']:,}")
            with r3:
                kpi_card("New quality score", f"{cleaned_quality['score']}/100",
                         f"{cleaned_quality['score'] - quality['score']:+.2f} vs before")

            details = st.session_state.get("cleaning_details", {})
            if details.get("medians"):
                with st.expander("Median values used for numeric columns"):
                    st.dataframe(
                        pd.DataFrame({"Column": list(details["medians"]), "Median": list(details["medians"].values())}),
                        width="stretch", hide_index=True,
                    )
            if details.get("skipped_numeric_columns"):
                st.warning("No median available (column is entirely empty): " + ", ".join(details["skipped_numeric_columns"]))

            st.markdown("#### Before vs After")
            render_comparison(quality, cleaned_quality)

            st.markdown("#### Cleaned dataset preview")
            st.dataframe(st.session_state["cleaned_preview"], width="stretch", hide_index=True)
            st.markdown("#### Remaining issues")
            st.write(
                f"Missing cells: **{cleaned_quality['missing_cells']:,}** · "
                f"Empty cells: **{cleaned_quality['empty_cells']:,}** · "
                f"Duplicate rows: **{cleaned_quality['duplicates']:,}**"
            )
            st.info("Your cleaned data is ready on the **Download** page.")
            if st.button("↩️ Undo / discard cleaned result and restore original dataset", width="stretch"):
                cleanup_cleaned_state()
                st.success("Cleaned result discarded. Original dataset is unchanged.")
                st.rerun()
            history = st.session_state.get("cleaning_history", [])
            if st.session_state.get("cleaning_options") and (
                not history or history[-1].get("options") != st.session_state.get("cleaning_options")
            ):
                history.append({"time": datetime.now().isoformat(timespec="seconds"),
                                "options": st.session_state.get("cleaning_options", {}),
                                "quality_score": cleaned_quality["score"]})
                st.session_state["cleaning_history"] = history
            if history:
                with st.expander("🕘 Cleaning history"):
                    st.dataframe(pd.DataFrame(history), width="stretch", hide_index=True)
                    st.download_button("Download cleaning history (JSON)",
                                       json.dumps(history, indent=2, default=str).encode("utf-8"),
                                       file_name="datafixer_cleaning_history.json", mime="application/json")

# ============================================================
# PAGE: DOWNLOAD
# ============================================================

elif page == "Download":
    st.markdown("### 📥 Download Center")
    cleaned_df = st.session_state.get("cleaned_df")
    cleaned_quality = st.session_state.get("cleaned_quality")

    if source_df is None or quality is None:
        st.info("Go to **Upload** and load a dataset first.")
    elif cleaned_df is None or cleaned_quality is None:
        st.info("Go to **Data Repair** and apply at least one cleaning operation first.")
    else:
        st.success("Your cleaned dataset is ready.")

        # JSON report: small, so it is generated instantly
        report_bytes = make_report_bytes(
            quality, cleaned_quality, filename, st.session_state.get("cleaning_options", {})
        )
        st.download_button(
            "⬇️ Download Quality Report (JSON)", data=report_bytes,
            file_name="datafixer_quality_report.json", mime="application/json", width="stretch",
        )

        st.markdown("#### Cleaned data exports")
        rows = cleaned_quality["rows"]
        st.caption(
            f"CSV and Excel files are built in your computer's memory via pandas, so exports are capped at "
            f"{EXPORT_ROW_LIMIT:,} rows (Excel itself allows at most {EXCEL_HARD_LIMIT:,}). "
            "For bigger data, write the Spark DataFrame to Parquet/CSV with `df.write` in a script instead."
        )
        if rows > EXPORT_ROW_LIMIT:
            st.warning(f"The cleaned dataset has {rows:,} rows. Only the first {EXPORT_ROW_LIMIT:,} rows will be exported.")

        if st.button("Prepare CSV and Excel files", type="primary", width="stretch"):
            try:
                with st.spinner("Converting Spark data for download..."):
                    export_pdf = cleaned_df.limit(EXPORT_ROW_LIMIT).toPandas()
                    csv_bytes = export_pdf.to_csv(index=False).encode("utf-8-sig")
                    excel_bytes = build_excel_bytes(export_pdf)
                    issues = st.session_state.get("advanced_issues")
                    if issues is None:
                        issues = pd.DataFrame(columns=["Issue Type", "Severity", "Column", "Affected Rows", "Sample Values"])
                    report_xlsx = make_multisheet_report(export_pdf, issues, quality, cleaned_quality)
                st.session_state["exports"] = {
                    "csv": csv_bytes, "xlsx": excel_bytes, "report_xlsx": report_xlsx,
                    "rows": len(export_pdf), "truncated": rows > len(export_pdf),
                }
            except Exception as error:  # noqa: BLE001
                st.error("The export files could not be created.")
                st.code(str(error))

        exports = st.session_state.get("exports")
        if exports:
            if exports["truncated"]:
                st.warning(f"Exports contain the first {exports['rows']:,} of {rows:,} rows.")
            d1, d2 = st.columns(2)
            with d1:
                st.download_button(
                    "⬇️ Download Cleaned CSV", data=exports["csv"], file_name="datafixer_cleaned.csv",
                    mime="text/csv", type="primary", width="stretch",
                )
            with d2:
                st.download_button(
                    "⬇️ Download Cleaned Excel", data=exports["xlsx"], file_name="datafixer_cleaned.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary", width="stretch",
                )
            st.download_button(
                "📊 Download Full Excel Quality Report (3 sheets)",
                data=exports["report_xlsx"], file_name="datafixer_full_quality_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch",
            )

        st.markdown("#### Before vs After")
        render_comparison(quality, cleaned_quality)

# ============================================================
# FOOTER
# ============================================================

st.markdown("---")
st.caption("DataFixer · Streamlit + Apache Spark (PySpark) · Plotly · Pandas")