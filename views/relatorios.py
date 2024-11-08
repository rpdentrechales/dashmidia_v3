import streamlit as st
import pandas as pd
import datetime
from auxiliar.auxiliar import *

st.set_page_config(page_title="Pró-Corpo - Relatórios", page_icon="💎",layout="wide")

st.markdown("# Pró-Corpo - Relatórios")

# Fetch all data
leads_data, appointments_data, bill_charges_data = run_fetch_all(start_date, end_date, extended_end_date, token)

# Convert to DataFrame and print results
if leads_data:
    df_leads = pd.DataFrame(leads_data)
    st.write("Leads")
    st.dataframe(df_leads)
if appointments_data:
    df_appointments = pd.DataFrame(appointments_data)
    st.write("Appointments")
    st.dataframe(df_appointments)
if bill_charges_data:
    df_bill_charges = pd.DataFrame(bill_charges_data)
    st.write("Bill Charges")
    st.dataframe(df_bill_charges)
