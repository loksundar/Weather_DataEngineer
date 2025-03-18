# 🌦️ Weather Pipeline Engineering

This repository showcases an **end-to-end automated Weather Data Engineering pipeline** created using Databricks and visualized in Tableau. The project demonstrates robust data engineering skills by ingesting data from multiple APIs, performing comprehensive data processing, and delivering automated visual analytics for actionable insights.

---

## 🚀 Project Overview

The project's objective was to:

- **Automate data ingestion** from two free weather APIs.
- **Integrate and clean** these datasets into a unified, high-quality dataset.
- **Aggregate data** into actionable insights.
- **Visualize** insights clearly using interactive Tableau dashboards.
- **Automate pipeline orchestration** using Databricks Workflows.

---

## ▶️ Project Demonstration Video

Watch a detailed demonstration of the project's workflow, Databricks automation, and Tableau visualizations clearly:

[▶️ Youtube Vedio Link](https://youtu.be/wcnz9rcQXLI)

---

## 📊 Tableau Dashboard Previews

### **Dashboard 1: Historical Weather Analytics**

![Dashboard1](https://github.com/user-attachments/assets/b2bb61fa-62ff-4d9f-9d2e-871b9f54fca1)

### **Dashboard 2: Today's Weather Insights**
![Dashboard2](https://github.com/user-attachments/assets/f746c7d5-f5b7-4c43-9d96-8c1115b9f081)


---

## 🛠️ Tech Stack

- **Data Engineering Platform:** Databricks
- **Storage Format:** Delta Lake
- **Data Processing:** PySpark
- **Visualization:** Tableau
- **API Integration:** Python Requests

---

## ⚙️ Pipeline Architecture

- **Bronze Layer:** Automated ingestion from APIs stored as Delta tables.
- **Silver Layer:** Data cleaning, deduplication, schema alignment, and merging.
- **Gold Layer:** Aggregations and analytical datasets for Tableau visualizations.

![pipeline](https://github.com/user-attachments/assets/8442701d-f6a8-43b6-9bd0-dc61ab268c0d)
![jobs](https://github.com/user-attachments/assets/188fe602-a09c-4d29-b3d0-a35cfa8be632)

---

## 📂 Repository Structure

````bash
Weather-Pipeline-Engineering/
├── Bronze_Ingestion/
│   ├── API_Ingestion_OpenWeather.py
│   └── API_weather_pull.py
│
├── Silver_Layer_Processing/
│   ├── silver_openweather.py
│   ├── silver_weatherapi.py
│   └── joining_silver_tables.py
│
├── Gold_Layer_Aggregations/
│   ├── gold_condition_stats.py
│   ├── gold_hourly_timeseries.py
│   ├── gold_wind_visibility_metrics.py
│   └── golden_daily_aggregate.py
│
├── Images/
│   ├── Tableau_Dashboard1.png
│   ├── Tableau_Dashboard2.png
│   └── Databricks_job_runs.png
│
├── README.md
└── requirements.txt

---

## 🛠️ Technology Stack

- **Data Engineering Platform:** Databricks
- **Storage:** Delta Lake
- **Languages & Libraries:** PySpark, Python
- **Visualization:** Tableau
- **Cloud Technologies:** Azure/AWS (Databricks), API integration

---

## 🚩 Setup Instructions

To set up this project:

1. **Clone Repository**:
```bash
git clone https://github.com/loksundar/Weather_DataEngineer.git
````

2. **Install Dependencies:**

```bash
pip install -r requirements.txt
```

3. **Set up Databricks:**

- Import notebooks into Databricks workspace.
- Configure Databricks cluster (runtime and instance).

4. **Run Pipeline:**

- Trigger Databricks Jobs workflow manually or schedule it.

5. **Tableau Dashboard:**

- Use extracts from the Gold tables in Databricks to populate Tableau dashboards.

---

## 📹 Video Demonstration

Clearly demonstrates pipeline working end-to-end with live updates and Tableau visualizations.

[📺 Watch the Project Demo](https://youtu.be/wcnz9rcQXLI)

---

## 📞 Contact & Links

- **Name:** Lok Sundar Ganthi
- **Email:** [loksundar000@gmail.com](mailto\:loksundar000@gmail.com)
- **LinkedIn:** [linkedin.com/in/ganthi-lok-sundar](https://www.linkedin.com/in/ganthi-lok-sundar/)
- **Portfolio:** [loksundar.com](https://loksundar.com)

---

Thank you for reviewing my project! Feel free to reach out for any additional details.

