import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from auxiliar.auxiliar import *

st.set_page_config(page_title="Pró-Corpo - Relatórios", page_icon="💎",layout="wide")

st.markdown("# Pró-Corpo - Relatórios")

token = '145418|arQc09gsrcSNJipgDRaM4Ep6rl3aJGkLtDMnxa0u'

today = datetime.datetime.now()
three_days_ago = today - timedelta(days=3)

data_seletor = st.date_input(
    "Selecione a data",
    (three_days_ago, today),
    format="DD/MM/YYYY",
)

if len(data_seletor) == 2:
  start_date = data_seletor[0].strftime('%Y-%m-%d')
  end_date = data_seletor[1].strftime('%Y-%m-%d')
else:
  start_date = data_seletor[0].strftime('%Y-%m-%d')
  end_date = start_date

end_date_obj = datetime.datetime.strptime(end_date, '%Y-%m-%d')
extended_end_date = (end_date_obj + timedelta(days=15)).strftime('%Y-%m-%d')

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
