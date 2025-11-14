import streamlit as st
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import pandas as pd
from query_cte import generate_cte_query


# =============================
# 🔧 SETUP SUPABASE
# =============================
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(page_title="Talent Benchmark", page_icon="🎯", layout="wide")

# =============================
# 🎯 SIDEBAR – CREATE BENCHMARK
# =============================
st.sidebar.header("🧩 Create Benchmark")

# Ambil daftar role dari dim_positions
try:
    roles_response = supabase.table("dim_positions").select("name").execute()
    role_names = sorted([r["name"] for r in roles_response.data])
except Exception as e:
    st.sidebar.error(f"Gagal mengambil daftar role: {e}")
    role_names = []

# --- Form input di sidebar ---
with st.sidebar.form("create_benchmark_form"):
    job_vacancy_id = st.text_input("Job Vacancy ID")
    role_name = st.selectbox("Role Name (dari dim_positions)", options=role_names)
    job_level = st.text_input("Job Level")
    role_purpose = st.text_area("Role Purpose")

    submit = st.form_submit_button("🚀 Generate Benchmark")

if submit:
    if not all([job_vacancy_id, role_name, job_level, role_purpose]):
        st.sidebar.warning("⚠️ Semua field wajib diisi.")
    else:
        try:
            # Ambil tahun maksimum
            year_query = supabase.table("performance_yearly").select("year").execute()
            all_years = [row["year"] for row in year_query.data]
            max_year = max(all_years)

            sql_query = f"""
            INSERT INTO talent_benchmarks (
                job_vacancy_id, 
                role_name, 
                job_level, 
                role_purpose, 
                selected_talent_ids,
                weights_config
            )
            SELECT 
                '{job_vacancy_id}' AS job_vacancy_id,
                '{role_name}' AS role_name,
                '{job_level}' AS job_level,
                '{role_purpose}' AS role_purpose,
                ARRAY_AGG(e.employee_id) AS selected_talent_ids,
                NULL AS weights_config
            FROM employees e
            INNER JOIN dim_positions dp ON e.position_id = dp.position_id
            INNER JOIN performance_yearly ep ON e.employee_id = ep.employee_id
            WHERE dp.name = '{role_name}'
              AND ep.year = {max_year}
              AND ep.rating = (
                  SELECT MAX(ep2.rating)
                  FROM employees e2
                  INNER JOIN dim_positions dp2 ON e2.position_id = dp2.position_id
                  INNER JOIN performance_yearly ep2 ON e2.employee_id = ep2.employee_id
                  WHERE dp2.name = '{role_name}'
                    AND ep2.year = {max_year}
              )
            GROUP BY dp.name
            ON CONFLICT (job_vacancy_id) DO UPDATE
            SET selected_talent_ids = EXCLUDED.selected_talent_ids,
                role_name = EXCLUDED.role_name;
            """

            supabase.rpc("exec_sql", {"query": sql_query}).execute()
            st.sidebar.success(f"✅ Benchmark untuk '{role_name}' berhasil dibuat/diupdate (tahun {max_year}).")

        except Exception as e:
            st.sidebar.error(f"Terjadi error: {e}")

# =============================
# 📊 MAIN PAGE – LIHAT / EDIT
# =============================
st.title("📋 Matching Talent")

try:
    benchmark_response = supabase.table("talent_benchmarks").select("job_vacancy_id, role_name").execute()
    job_list = [f"{b['job_vacancy_id']} - {b['role_name']}" for b in benchmark_response.data]
except Exception as e:
    st.error(f"Gagal mengambil data benchmark: {e}")
    job_list = []

selected_job = st.selectbox("Pilih Job Vacancy", options=["-- pilih --"] + job_list)

if selected_job != "-- pilih --":
    selected_id = selected_job.split(" - ")[0]
    detail_query = supabase.table("talent_benchmarks").select("*").eq("job_vacancy_id", selected_id).execute()

    if detail_query.data:
        benchmark = detail_query.data[0]
        st.markdown(f"**Role:** {benchmark['role_name']}")
        st.markdown(f"**Level:** {benchmark['job_level']}")
        st.markdown(f"**Purpose:** {benchmark['role_purpose']}")

        selected_ids = benchmark["selected_talent_ids"] or []

        if not selected_ids:
            st.info("Benchmark ini belum memiliki selected_talent_ids.")
        else:
            sql = f"""
                SELECT e.employee_id, e.fullname, da.name AS area_name, de.name AS education_name, e.years_of_service_months
                FROM employees e
                LEFT JOIN dim_areas da ON e.area_id = da.area_id
                LEFT JOIN dim_education de ON e.education_id = de.education_id
                WHERE e.employee_id = ANY('{{{",".join(selected_ids)}}}')
                ORDER BY e.employee_id
            """

            emp_detail = supabase.rpc("exec_sql_return", {"query": sql}).execute()

            if emp_detail.data:
                df = pd.DataFrame(emp_detail.data)
                df["Pilih"] = False

                for i, emp_id in enumerate(df["employee_id"]):
                    if emp_id in selected_ids[:3]:
                        df.at[i, "Pilih"] = True

                st.markdown("### Pilih Talent (maksimal 3):")

                edited_df = st.data_editor(
                    df,
                    hide_index=True,
                    disabled=["employee_id", "fullname", "area_name", "education_name", "years_of_service_months"],
                    column_config={
                        "employee_id": "Employee ID",
                        "fullname": "Full Name",
                        "area_name": "Area",
                        "education_name": "Education",
                        "years_of_service_months": "Service (Months)",
                        "Pilih": st.column_config.CheckboxColumn("Pilih"),
                    },
                    use_container_width=True,
                )

                selected_rows = edited_df[edited_df["Pilih"] == True]["employee_id"].tolist()

                if len(selected_rows) > 3:
                    st.error("⚠️ Maksimal hanya boleh memilih 3 talent.")
                elif st.button("Gunakan Talent Ini"):
                    st.session_state["selected_temp_ids"] = selected_rows
                    
            else:
                st.warning("Tidak ada data employee untuk ID tersebut.")

# ==============================
# 🔍 EKSEKUSI MATCHING CTE
# ==============================
if "selected_temp_ids" in st.session_state and st.session_state["selected_temp_ids"]:
    
    st.markdown("---")
    st.subheader("🔎 Jalankan Perhitungan Matching")

    if st.button("▶ Jalankan Matching Calculation"):
        # st.write("Selected IDs:", st.session_state["selected_temp_ids"])

        try:
            sql_cte = generate_cte_query(selected_id, st.session_state["selected_temp_ids"])
            result = supabase.rpc("exec_sql_return", {"query": sql_cte}).execute()

            if result.data:
                df_result = pd.DataFrame(result.data)
                st.success("✅ Perhitungan selesai!")
                desired_order = ["fullname", "education", "area", "match_rate", "strength", "top_tgv"]
                df_result = df_result[desired_order]
                st.dataframe(df_result, use_container_width=True)
            else:
                st.warning("Tidak ada hasil dari perhitungan.")
        except Exception as e:
            st.error(f"❌ Gagal menjalankan perhitungan: {e}")