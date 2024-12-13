from pymongo import MongoClient, UpdateOne
import pandas as pd
import streamlit as st

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

  leads_columns = ['id', 'createdAt', 'customer_id', 'customer_name', 'date', 'email',
                  'message', 'name', 'source', 'status', 'store', 'telephone',
                  'utmCampaign', 'utmContent', 'utmMedium', 'utmSearch', 'utmTerm']

  appointments_columns = ['id', 'createdBy_name', 'customer_id', 'customer_name',
                          'customer_telephone', 'date', 'eh_atendimento', 'employee_name',
                          'is_assessment', 'procedure_name', 'startDate', 'status_label',
                          'store_name']

  billcharges_columns = ['id', 'customer_email', 'customer_id', 'customer_name',
                        'customer_taxvat', 'date', 'due_at', 'installments', 'is_paid',
                        'paid_at', 'payment_method', 'quote_id', 'quote_items', 'status',
                        'store_name', 'total_amount']  

  leads_df  = ensure_dataframe(leads_df, leads_columns)
  appointments_df  = ensure_dataframe(appointments_df, appointments_columns) 
  billcharges_df  = ensure_dataframe(billcharges_df, billcharges_columns)  

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

def ensure_dataframe(df, columns):
    if df is None or df.empty:  # Check if DataFrame is empty or None
        return pd.DataFrame(columns=columns)
    return df
