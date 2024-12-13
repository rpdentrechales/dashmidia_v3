import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from auxiliar.auxiliar import *
import plotly.express as px

st.set_page_config(page_title="Pró-Corpo - Relatórios", page_icon="💎",layout="wide")
header_1, spacer,header_2 = st.columns([1,0.5,0.5])

with header_1:
  st.title("Relatórios de Mídia")

with header_2:

  yesterday = datetime.now() - timedelta(days=1)
  first_day_of_month = yesterday.replace(day=1)

  data_seletor = st.date_input(
      "Selecione o Período do Relatório",
      (first_day_of_month, yesterday),
      format="DD/MM/YYYY",
  )
  botao_gerar_relatorio = st.button("Atualizar Relatórios")

if len(data_seletor) == 2:

  start_date = data_seletor[0].strftime('%Y-%m-%d')
  end_date = data_seletor[1].strftime('%Y-%m-%d')

else:

  start_date = data_seletor[0].strftime('%Y-%m-%d')
  end_date = start_date

if 'funil_df' not in st.session_state:
  st.session_state['funil_df'] = criar_funil(start_date,end_date)


if botao_gerar_relatorio:
  st.session_state['funil_df'] = criar_funil(start_date,end_date)

funil_df = st.session_state['funil_df']

st.title("Funil por Unidade")

groupby_unidade = funil_df.groupby(['Unidade']).agg({'Leads':'sum','Agendamentos':'sum','Atendimentos':'sum','Receita':'sum','Vendas':'sum'}).reset_index()

st.dataframe(groupby_unidade,hide_index = True,use_container_width=True)

st.title("Funil por Data")

opcoes_unidades = funil_df['Unidade'].unique().tolist()
opcoes_unidades.insert(0,"Todas Unidades")

filtro_unidade = st.selectbox("Selecione a Unidade", opcoes_unidades)

if filtro_unidade != "Todas Unidades":
  funil_df = funil_df.loc[funil_df['Unidade'] == filtro_unidade]

groupby_data = funil_df.groupby(['Data']).agg({'Leads':'sum','Agendamentos':'sum','Atendimentos':'sum','Receita':'sum','Vendas':'sum'}).reset_index()

st.dataframe(groupby_data,hide_index = True,use_container_width=True)

st.title("Visualizar Evolução por dia")

metrics = ["Leads", "Agendamentos", "Atendimentos", "Receita", "Vendas"]
selected_metrics = st.pills("Directions", metrics, selection_mode="multi",default=["Leads"])

df_melted = groupby_data.melt(
    id_vars=["Data"],
    value_vars=selected_metrics,
    var_name="Metric",
    value_name="Value"
)

fig = px.bar(
  df_melted,
  x="Data",
  y="Value",
  color="Metric"
)

st.plotly_chart(fig, use_container_width=True)
