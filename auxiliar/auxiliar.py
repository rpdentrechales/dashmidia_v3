from pymongo import MongoClient, UpdateOne
import pandas as pd

def get_dataframe_from_mongodb(collection_name, database_name, query={}):

    client = MongoClient(f"mongodb+srv://rpdprocorpo:iyiawsSCfCsuAzOb@cluster0.lu6ce.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client[database_name]
    collection = db[collection_name]

    data = list(collection.find(query))

    if data:
        dataframe = pd.DataFrame(data)
        if '_id' in dataframe.columns:
            dataframe = dataframe.drop(columns=['_id'])
    else:
        dataframe = pd.DataFrame()

    return dataframe


@st.cache_data
def criar_funil(start_date,end_date):
  query = {"date": {"$gte": start_date, "$lte": end_date}}

  leads_df = get_dataframe_from_mongodb(collection_name="leads_db", database_name="dash_midia", query=query)
  appointments_df = get_dataframe_from_mongodb(collection_name="appointments_db", database_name="dash_midia", query=query)
  billcharges_df = get_dataframe_from_mongodb(collection_name="billcharges_db", database_name="dash_midia", query=query)

  # Trata os Leads

  leads_df['date'] = pd.to_datetime(leads_df['date'])
  leads_df['date'] = leads_df['date'].dt.strftime('%d/%m/%Y')
  leads_groupby = leads_df.groupby(['date','store']).agg({'id':'nunique'}).reset_index()
  leads_groupby.columns = ['Data','Unidade','Leads']

  # Trata os Agendamentos

  appointments_df['date'] = pd.to_datetime(appointments_df['date'])
  appointments_df['date'] = appointments_df['date'].dt.strftime('%d/%m/%Y')
  avalicacoes_df = appointments_df.loc[appointments_df['is_assessment'] == True]
  appointments_groupby = avalicacoes_df.groupby(['date','store_name']).agg({'id':'count','eh_atendimento':'sum'}).reset_index()
  appointments_groupby.columns = ['Data','Unidade','Agendamentos',"Atendimentos"]

  # Trata o billcharge

  billcharges_df['date'] = pd.to_datetime(billcharges_df['date'])
  billcharges_df['date'] = billcharges_df['date'].dt.strftime('%d/%m/%Y')
  finalizados_df = billcharges_df.loc[billcharges_df['status'] == 'completed']
  billcharges_groupby = finalizados_df.groupby(['date','store_name']).agg({'total_amount':'sum','quote_id':'nunique'}).reset_index()
  billcharges_groupby.columns = ['Data','Unidade','Receita','Vendas']

  merged_df = pd.merge(leads_groupby, appointments_groupby, on=['Data', 'Unidade'], how='left')
  merged_df = pd.merge(merged_df, billcharges_groupby, on=['Data', 'Unidade'], how='left')
  merged_df.fillna(0, inplace=True)

  return merged_df
