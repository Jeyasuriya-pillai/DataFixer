# 📊 DataFixer — Automated Data Quality Assessment & Repair System

### Detect. Fix. Improve. — A Data Quality Analysis and Cleaning Application Using Apache Spark

DataFixer is a web-based data quality assessment and repair application designed to identify data quality issues and simplify the data cleaning process. It helps users analyze datasets, detect missing values and duplicate records, evaluate data quality, and generate cleaned datasets through an interactive interface.

The application provides a dashboard for understanding dataset quality, reviewing detected issues, and comparing data before and after cleaning.

---

## 🌐 Live Demo

🚀 **Try DataFixer Online**

🔗 https://datafixer.streamlit.app/

---

## ✨ Features

- 📂 **Dataset Upload** — Upload CSV, Excel, and supported data files.
- 📊 **Data Quality Dashboard** — View dataset statistics and quality metrics.
- 🔍 **Missing Value Detection** — Identify missing and empty values.
- ♻️ **Duplicate Detection** — Find duplicate records in datasets.
- 🧹 **Automated Data Cleaning** — Apply selected cleaning operations.
- 🔢 **Missing Value Imputation** — Fill missing numeric values using the median and text values using a configurable replacement.
- ✂️ **Text Standardization** — Trim unnecessary spaces and standardize text values.
- 📈 **Interactive Visualizations** — Explore data quality metrics through charts.
- 🧪 **Issue Report** — Review detected data quality problems.
- 📉 **Before-and-After Comparison** — Compare dataset quality before and after cleaning.
- 📋 **Column-Level Analysis** — Examine quality metrics for individual columns.
- 📥 **Download Cleaned Data** — Export cleaned datasets for further use.
- 📑 **Quality Reports** — Generate downloadable reports containing quality information.
- ↩️ **Restore Original Dataset** — Return to the original dataset during the cleaning workflow.
- ⚡ **Apache Spark Integration** — Integrate distributed data processing capabilities into the application.
- ☁️ **Streamlit Cloud Deployment** — Access the application through a web browser.

---

## 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| Python | Application logic and data processing |
| Streamlit | Interactive web application interface |
| Apache Spark (PySpark) | Distributed data processing |
| Pandas | Data manipulation and analysis |
| NumPy | Numerical operations |
| Plotly | Interactive charts and visualizations |
| OpenPyXL | Excel file processing and report generation |
| Git | Version control |
| GitHub | Source code hosting |
| Streamlit Community Cloud | Application deployment |

---

## 🧠 How It Works

DataFixer follows a data quality assessment and repair workflow.

```text
             Upload Dataset
                   |
                   v
          Load Dataset for Analysis
                   |
                   v
          Data Quality Assessment
                   |
          +--------+--------+
          |        |        |
          v        v        v
       Missing  Duplicate  Invalid
        Values    Rows     Values
          |        |        |
          +--------+--------+
                   |
                   v
          Review Detected Issues
                   |
                   v
          Apply Cleaning Operations
                   |
                   v
        Recalculate Quality Metrics
                   |
                   v
       Compare Before and After Data
                   |
                   v
       Download Cleaned Dataset
             and Reports
```

### Workflow Explanation

1. **Upload Dataset** — The user uploads a supported dataset.
2. **Assess Data Quality** — The application calculates quality metrics and identifies supported data quality issues.
3. **Review Issues** — The user examines missing values, duplicate records, and other detected problems.
4. **Clean Dataset** — The user selects and applies the available cleaning operations.
5. **Compare Results** — The application presents before-and-after quality information.
6. **Export Results** — The cleaned dataset and supported reports can be downloaded.

---

## 📁 Project Structure

The project structure may vary depending on the features and modules included in the current version.

```text
DataFixer/
│
├── app.py
├── requirements.txt
├── README.md
│
└── .gitignore
```

### File Description

| File | Description |
|---|---|
| `app.py` | Main Streamlit application |
| `requirements.txt` | Python dependencies required by the application |
| `README.md` | Project documentation |
| `.gitignore` | Specifies files and folders excluded from Git tracking |

---

## ⚙️ Installation & Setup

Follow these steps to run DataFixer locally.

### 1. Clone the Repository

```bash
git clone https://github.com/Jeyasuriya-pillai/DataFixer.git
```

### 2. Navigate to the Project Directory

```bash
cd DataFixer
```

### 3. Create a Virtual Environment

```bash
python -m venv venv
```

### 4. Activate the Virtual Environment

**Windows — Git Bash:**

```bash
source venv/Scripts/activate
```

**Windows — Command Prompt:**

```bat
venv\Scripts\activate
```

### 5. Install Dependencies

```bash
python -m pip install -r requirements.txt
```

If you have not created a `requirements.txt` file, install the main dependencies with:

```bash
python -m pip install streamlit pandas numpy plotly openpyxl pyspark xlrd
```

### 6. Run the Application

```bash
python -m streamlit run app.py
```

### 7. Open the Application

Open the local URL displayed in your terminal. The default address is usually:

http://localhost:8501

---

## 📊 Data Quality Metrics

DataFixer helps users evaluate dataset quality through supported metrics and issue detection.

| Metric | Description |
|---|---|
| Missing Values | Number of missing values in the dataset |
| Duplicate Records | Number of duplicate rows detected |
| Completeness | Proportion of values that are present |
| Uniqueness | Uniqueness of values in the dataset |
| Data Quality Score | Overall quality indicator calculated by the application |
| Column-Level Quality | Quality information for individual columns |
| Detected Issues | Identified data quality problems that require review |

The exact metrics and calculations depend on the implementation of the current application version.

---

## 🧪 Example Use Case

Consider a dataset containing customer information:

| Customer ID | Name | Age | Email |
|---|---|---:|---|
| 101 | Rahul | 22 | rahul@example.com |
| 102 | Priya | *(Missing)* | priya@example.com |
| 103 | Amit | 25 | amit@example.com |
| 103 | Amit | 25 | amit@example.com |

### Potential Data Quality Issues

- A missing value in the `Age` column.
- A duplicate customer record.
- Potential inconsistencies that may require additional validation.

### Expected Workflow

1. Upload the dataset to DataFixer.
2. Review the quality dashboard.
3. Identify missing values and duplicate records.
4. Apply the appropriate cleaning operations.
5. Compare the original and cleaned datasets.
6. Download the cleaned dataset and available reports.

*This is an illustrative example. Actual results depend on the uploaded data and the cleaning options selected.*

---

## 📥 Output and Reports

Depending on the features enabled in the deployed version, DataFixer supports exporting results in formats such as:

- **CSV** — Cleaned dataset.
- **Excel** — Cleaned data and supported report sheets.
- **JSON** — Structured data quality report.
- **Issue Report CSV** — Export of detected data quality issues, if available in the deployed version.

These outputs can be used for further analysis, reporting, and data preparation workflows.

---

## 🔒 Data Handling and Limitations

- Review datasets before uploading them to a hosted application.
- Avoid uploading confidential, sensitive, or personally identifiable information unless the deployment is approved for that data.
- Review detected issues before applying cleaning operations.
- Unusual values are not necessarily incorrect values.
- Automated cleaning may affect downstream analysis if inappropriate rules are selected.
- A high data quality score does not guarantee that every value is correct.
- Processing performance depends on dataset size and available system resources.
- Apache Spark functionality depends on the application's implementation and deployment environment.

---

## 🚧 Project Status

**Status: Under Development**

### Core Components

- Dataset upload and processing.
- Data quality assessment.
- Missing value detection.
- Duplicate record detection.
- Data cleaning operations.
- Quality dashboard and visualizations.
- Before-and-after comparison.
- Cleaned data export.
- Streamlit deployment.

### Future Enhancements

- 🤖 Intelligent cleaning recommendations.
- 🧠 Machine learning-assisted anomaly detection.
- 📧 Advanced email and data format validation.
- 📊 More detailed column-level quality analytics.
- 📑 Comprehensive downloadable quality reports.
- 🕘 Detailed cleaning history and audit logs.
- ⚡ Improved distributed processing for large datasets.
- 🔐 User authentication and access control.
- 🗃️ Database integration.
- 📈 Advanced data profiling and validation rules.

---

## 👨‍💻 Developer Information

| Information | Details |
|---|---|
| Developer | Jeyasuriya Thangapandi |
| Course | B.Sc. Information Technology |
| Project | DataFixer — Automated Data Quality Assessment and Repair System |
| Primary Language | Python |
| Data Processing | Apache Spark / PySpark |
| Web Framework | Streamlit |
| Deployment | Streamlit Community Cloud |

---

## 🔗 Project Links

- **Live Application:** https://datafixer.streamlit.app/
- **GitHub Repository:** https://github.com/Jeyasuriya-pillai/DataFixer

---

## 📜 License

This project is developed for educational and academic purposes. Add a `LICENSE` file to the repository if you intend to distribute the project under a specific open-source license.

---

<p align="center">
  <b>DataFixer — Detect. Fix. Improve.</b>
  <br>
  Making data quality assessment and cleaning simpler.
</p>
