import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from auxiliar.auxiliar import *

st.set_page_config(page_title="Pró-Corpo - Relatórios", page_icon="💎",layout="wide")

st.title("Relatórios de Mídia")

yesterday = datetime.now() - timedelta(days=1)
start_of_week = yesterday - timedelta(days=yesterday.weekday())

data_seletor = st.date_input(
    "Selecione o Período do Relatório",
    (start_of_week, yesterday),
    format="DD/MM/YYYY",
)

if len(data_seletor) == 2:
  start_date = data_seletor[0].strftime('%Y-%m-%d')
  end_date = data_seletor[1].strftime('%Y-%m-%d')
else:
  start_date = data_seletor[0].strftime('%Y-%m-%d')
  end_date = start_date


if st.button("Gerar Relatórios"):

  funil_df = criar_funil(start_date,end_date)

  st.title("Funil por Unidade")

  groupby_unidade = funil_df.groupby(['Unidade']).agg({'Leads':'sum','Agendamentos':'sum','Atendimentos':'sum','Receita':'sum','Vendas':'sum'}).reset_index()

  st.dataframe(groupby_unidade)

  st.title("Funil por Data")

  opcoes_unidades = funil_df['Unidade'].unique()
  opcoes_unidades.insert(0,"Todas Unidades")

  filtro_unidade = st.selectbox("Selecione a Unidade", opcoes_unidades,"Todas Unidades")

  if filtro_unidade != "Todas Unidades":
    funil_df = funil_df.loc[funil_df['Unidade'] == filtro_unidade]

  groupby_data = funil_df.groupby(['Data']).agg({'Leads':'sum','Agendamentos':'sum','Atendimentos':'sum','Receita':'sum','Vendas':'sum'}).reset_index()

  st.dataframe(groupby_data)

  st.title("Visualizar Evolução por dia")

  metrics = ["Leads", "Agendamentos", "Atendimentos", "Receita", "Vendas"]
  selected_metrics = st.pills("Directions", metrics, selection_mode="multi",default=["Leads"])

  df_melted = groupby_data.melt(
      id_vars=["Data"], 
      value_vars=selected_metrics, 
      var_name="Metric", 
      value_name="Value"
  )

  fig = px.line(
    df_melted, 
    x="Data", 
    y="Value", 
    color="Metric"
  )

  st.plotly_chart(fig, use_container_width=True)
