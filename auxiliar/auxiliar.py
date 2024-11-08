import aiohttp
import asyncio
import json
import pandas as pd
import nest_asyncio
import time
import streamlit as st
import datetime

nest_asyncio.apply()

# Async function to fetch GraphQL data with error handling and retries
async def fetch_graphql(session, url, query, variables, token):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
    }
    payload = {
        'query': query,
        'variables': variables,
    }

    attempt = 0
    while True:  # Infinite retry loop
        try:
            async with session.post(url, headers=headers, data=json.dumps(payload)) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'errors' in data:
                        print(f"GraphQL errors: {data['errors']}")
                        return None
                    return data
                else:
                    print(f"Request failed with status {response.status}")
        except aiohttp.ClientError as e:
            print(f"Request exception: {e}")

        # Exponential backoff and retry
        attempt += 1
        wait_time = min(5 * 2 ** attempt, 30)  # Exponential backoff with max wait time of 30 seconds
        print(f"Retrying in {wait_time} seconds (attempt {attempt})...")
        await asyncio.sleep(wait_time)

# Function to fetch all leads with pagination and retry logic
async def fetch_all_leads(session, start_date, end_date, token):
    current_page = 1
    all_leads = []

    while True:
        query = '''query ($filters: LeadFiltersInput, $pagination: PaginationInput) {
                    fetchLeads(filters: $filters, pagination: $pagination) {
                        data {
                            createdAt
                            id
                            source {
                                title
                            }
                            store {
                                name
                            }
                            status {
                                label
                            }
                            customer {
                                id
                                name
                            }
                            name
                            telephone
                            email
                            message
                            utmMedium
                            utmContent
                            utmCampaign
                            utmSearch
                            utmTerm
                        }
                        meta {
                            currentPage
                            lastPage
                        }
                    }
                }'''

        variables = {
            'filters': {
                'createdAtRange': {
                    'start': start_date,
                    'end': end_date,
                },
            },
            'pagination': {
                'currentPage': current_page,
                'perPage': 200,
            },
        }

        data = await fetch_graphql(session, 'https://open-api.eprocorpo.com.br/graphql', query, variables, token)

        if data is None:
            print(f"Failed to fetch leads on page {current_page}. Retrying...")
            continue

        leads_data = data['data']['fetchLeads']['data']
        all_leads.extend(leads_data)

        meta = data['data']['fetchLeads']['meta']
        last_page = meta['lastPage']

        print(f"Querying Leads - Page: {current_page}/{last_page} - startDate: {start_date} - endDate: {end_date}")

        if current_page >= last_page:
            break

        current_page += 1
        await asyncio.sleep(5)  # Small delay to avoid hitting API complaints

    return all_leads

# Fetch appointments with pagination + retry logic
async def fetch_appointments(session, start_date, end_date, token):
    current_page = 1
    all_appointments = []

    while True:
        query = '''query ($filters: AppointmentFiltersInput, $pagination: PaginationInput) {
                    fetchAppointments(filters: $filters, pagination: $pagination) {
                        meta {
                            currentPage
                            lastPage
                        }
                        data {
                            id
                            status {
                                label
                            }
                            createdBy {
                                name
                                createdAt
                            }
                            store {
                                name
                            }
                            customer {
                                id
                                name
                                telephones {
                                    number
                                }
                            }
                            procedure {
                                name
                                groupLabel
                            }
                            employee {
                                name
                            }
                            startDate
                        }
                    }
                }'''
        variables = {
            'filters': {
                'startDateRange': {
                    'start': start_date,
                    'end': extended_end_date,
                },
            },
            'pagination': {
                'currentPage': current_page,
                'perPage': 200,  # Large page size to reduce the number of requests
            },
        }

        data = await fetch_graphql(session, 'https://open-api.eprocorpo.com.br/graphql', query, variables, token)

        if data is None:
            print(f"Failed to fetch appointments on page {current_page}. Retrying...")
            continue  # Retry the loop on failure

        # appointments_data = data['data']['fetchAppointments']['data']
        # Check if 'customer' object exists in data and contains necessary fields
        appointments_data = [
            {
                "id": row['id'],
                "status_label": row.get('status', {}).get('label', 'N/A') if isinstance(row.get('status'), dict) else 'N/A',
                "store_name": row.get('store', {}).get('name', 'N/A') if isinstance(row.get('store'), dict) else 'N/A',
                "customer_id": row.get('customer', {}).get('id', 'N/A') if isinstance(row.get('customer'), dict) else 'N/A',
                "customer_name": row.get('customer', {}).get('name', 'N/A') if isinstance(row.get('customer'), dict) else 'N/A',
                "customer_telephone": (
                  row.get('customer', {}).get('telephones', [{}])[0].get('number', 'N/A')
                  if isinstance(row.get('customer'), dict) and row.get('customer', {}).get('telephones')
                  else 'N/A'
              ),
                "procedure_name": row.get('procedure', {}).get('name', 'N/A') if isinstance(row.get('procedure'), dict) else 'N/A',
                "procedure_group": row.get('procedure', {}).get('groupLabel', 'N/A') if isinstance(row.get('procedure'), dict) else 'N/A',
                "employee_name": row.get('employee', {}).get('name', 'N/A') if isinstance(row.get('employee'), dict) else 'N/A',
                "createdBy_name": row.get('createdBy', {}).get('name', 'N/A') if isinstance(row.get('createdBy'), dict) else 'N/A',
                "createdBy_createdAt": row.get('createdBy', {}).get('createdAt', 'N/A') if isinstance(row.get('createdBy'), dict) else 'N/A',
                "startDate": row.get('startDate', 'N/A'),
            }
            for row in data['data']['fetchAppointments']['data']
        ]
        all_appointments.extend(appointments_data)

        meta = data['data']['fetchAppointments']['meta']
        last_page = meta['lastPage']

        print(f"Querying Appointments - Page: {current_page}/{last_page} - startDate: {start_date} - endDate: {end_date}")

        if current_page >= last_page:
            break

        current_page += 1
        await asyncio.sleep(5)  # Small delay to avoid spamming the server

    return all_appointments

# Fetch bill charges with pagination and retry logic
async def fetch_bill_charges(session, start_date, end_date, token):
    current_page = 1
    all_bill_charges = []

    while True:
        query = '''query ($filters: BillChargeFiltersInput, $pagination: PaginationInput) {
                fetchBillCharges(filters: $filters, pagination: $pagination) {
                    data {
                        quote {
                            id
                            customer {
                                id
                                name
                                taxvat
                                email
                            }
                            status
                            bill {
                                total
                                installmentsQuantity
                                items {
                                    amount
                                    description
                                    quantity
                                }
                            }
                        }
                        store {
                            name
                        }
                        amount
                        paidAt
                        dueAt
                        isPaid
                        paymentMethod {
                            name
                        }
                    }
                    meta {
                        currentPage
                        lastPage
                    }
                }
            }'''
        variables = {
            'filters': {
                'paidAtRange': {
                    'start': start_date,
                    'end': end_date,
                }
            },
            'pagination': {
                'currentPage': current_page,
                'perPage': 200,
            }
        }

        data = await fetch_graphql(session, 'https://open-api.eprocorpo.com.br/graphql', query, variables, token)

        if data is None:
            print(f"Failed to fetch bill charges on page {current_page}. Retrying...")
            continue  # Retry the loop on failure

        bill_charges_data = data['data']['fetchBillCharges']['data']
        all_bill_charges.extend(bill_charges_data)

        meta = data['data']['fetchBillCharges']['meta']
        print(f"Querying Bill Charges - Page: {current_page}/{meta['lastPage']} - startDate: {start_date} - endDate: {end_date}")

        if current_page >= meta['lastPage']:
            break

        current_page += 1
        await asyncio.sleep(5)  # Small delay

    return all_bill_charges

# Main async function to fetch all data concurrently
async def fetch_all_data(start_date, end_date, extended_end_date, token):
    async with aiohttp.ClientSession() as session:
        leads_task = fetch_all_leads(session, start_date, end_date, token)  # Use original end_date for leads
        appointments_task = fetch_appointments(session, start_date, extended_end_date, token)  # Use extended_end_date for appointments
        bill_charges_task = fetch_bill_charges(session, start_date, end_date, token)  # Use original end_date for bill charges

        leads_data, appointments_data, bill_charges_data = await asyncio.gather(
            leads_task, appointments_task, bill_charges_task
        )

        return leads_data, appointments_data, bill_charges_data

# Wrapper function to run async functions
def run_fetch_all(start_date, end_date, extended_end_date, token):
    st.write(f"run_fetch: {extended_end_date}")
    return asyncio.run(fetch_all_data(start_date, end_date, extended_end_date, token))

def treat_leads(df_leads):
  leads_results_list = []
  for index, lead in df_leads.iterrows():
    formatted_row = {
        'createdAt': lead['createdAt'],
        'id': lead['id'],
        'source': lead['source']['title'],
        'store': lead['store']['name'] if lead['store'] else None,
        'status': lead['status']['label'],
        'name': lead['name'],
        'telephone': lead['telephone'],
        'email': lead['email'],
        'utmMedium': lead['utmMedium'],
        'utmCampaign': lead['utmCampaign'],
        'utmContent': lead['utmContent'],
        'utmSearch': lead['utmSearch'],
        'utmTerm': lead['utmTerm'],
        'message': lead['message'],

        # Dados do cliente
        'customer_id': lead['customer']['id'] if lead['customer'] else None, # Check if lead['customer'] exists
        'customer_name': lead['customer']['name'] if lead['customer'] else None, # Check if lead['customer'] exists
    }
    leads_results_list.append(formatted_row)

  df_leads = pd.DataFrame(leads_results_list)

  # Special treatment
  df_leads.loc[df_leads['customer_id'].isna(), 'customer_id'] = "Not found"
  df_leads = df_leads.loc[df_leads['store'] != "CENTRAL"]
  df_leads = df_leads.loc[df_leads['store'] != "HOMA"]
  df_leads = df_leads.loc[df_leads['store'] != "PLÁSTICA"]
  df_leads = df_leads.loc[df_leads['store'] != "PRAIA GRANDE"]
  df_leads['is_customer'] = df_leads['customer_id'].apply(lambda x: "True" if x != "Not found" else "False")
  df_leads['is_appointment'] = False
  df_leads['is_served'] = False
  df_leads['is_purchase'] = False
  df_leads['customer_id'] = pd.to_numeric(df_leads['customer_id'], errors='coerce').astype('Int64')

  # Prep for merging with Appointments
  df_leads_is_appointment = df_leads.loc[df_leads['is_customer'] == 'True']
  df_leads_is_appointment.shape

  return df_leads, df_leads_is_appointment
