import googleapiclient.errors as errors
import logging
from datetime import datetime
from os import getenv
import re
EMAIL_FILE_ID= str(getenv('EMAIL_FILE_ID'))
LOG_FILE_ID= str(getenv('LOG_FILE_ID'))
SECONDS_THRESHOLD_UPDATE = int(getenv('SECONDS_THRESHOLD_UPDATE'))

logging.basicConfig(level=logging.INFO)

def make_file_public(drive_service,file_id,role):
    try:
        permission = {
            'type': 'anyone',
            'role': role,
        }
        drive_service.permissions().create(fileId=file_id, body=permission).execute()
    except errors.HttpError as error:
        print(f'An error occurred: {error}')
        return

def get_clientes(mi_serie):
    count=0
    for i, sub_df in mi_serie.groupby(level='STATUS', sort=False):
        if len(sub_df) > 0:
            yield (f'\n-{i}-\n')
            for j, rest in sub_df.groupby(level='PUNTO DE VENTA'):
                if len(rest) > 0:
                    yield (f'\n{j}\n')
                    a=mi_serie.loc[(i,j)]
                    if len(a) > 0:
                        for client in a:
                            if client:
                                if i=='VIAJAN ✅':
                                    yield (f'{count+1}. {client}\n')
                                    count+=1
                                else:
                                    yield (f'{client}\n')
                    else:
                        continue
    return
            
def list_to_str_commas(lst):
    # Exclude None elements and convert rest to string
    str_list = [str(elem).strip() for elem in lst if elem]
    # Join all elements in the list with a space
    result = ','.join(str_list)
    return result

def seconds_since(timestamp,utc_on):
    if utc_on:
        # Get the current UTC time
        current_time = datetime.utcnow()
    else:
        current_time = datetime.now()
    # Convert to RFC 3339 format
    current_time_str = current_time.isoformat("T").rsplit('.')[0]+'Z'
    current_time_rfc3339=datetime.strptime(current_time_str, "%Y-%m-%dT%H:%M:%SZ")
    timestamp_str = timestamp.rsplit('.')[0]+'Z'
    try: 
        timestamp_rfc3339 = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        print('Error converting timestamp:',str(e))
        return 0 #needs to return a number for value comparison
    time_difference = current_time_rfc3339 - timestamp_rfc3339
    # Extract the total number of minutes from the timedelta object
    seconds_difference = time_difference.total_seconds()
    return(seconds_difference)

def create_logs(sheets_service,file_name,tab_name, file_id, tab_id, calendar_id,photos_folder_id,file_link,calendar_link,photos_folder_link,logs_data):
    sheet_name = 'logs'  # replace with your sheet name

    # How the input data should be interpreted.
    value_input_option = 'USER_ENTERED'  # or 'RAW'

    value_range_body = {
        "majorDimension": "ROWS",  # "ROWS"|"COLUMNS"
        "values": [
            [file_name,file_id, calendar_id,photos_folder_id,file_link,calendar_link,photos_folder_link,tab_name,tab_id]+logs_data
        ]
    }

    sheets_service.spreadsheets().values().append(spreadsheetId=LOG_FILE_ID, range=sheet_name, valueInputOption=value_input_option, body=value_range_body).execute()
    print('New log entry created')

def update_columns_logs(sheets_service,file_id, tab_id, update_values,columns):
    sheet_name='logs'
    data_range = f'{sheet_name}!A:I'
    
    # Read the data from the sheet
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=LOG_FILE_ID,
        range=data_range).execute()

    # Get the values from the result
    values = result.get('values', [])
    del result #just to freeup memory

    # Go through each row to look for matches. Start at the end for quicker find
    for i in range(len(values) - 1, -1, -1):
        row=values[i]
        if row and row[1] == file_id and str(row[8]) == tab_id:
            for update_value, column in zip(update_values, columns):
                range_ = f"{sheet_name}!{column}{i+1}"
                value_range_body = {
                    "values": [[update_value]]
                }
                sheets_service.spreadsheets().values().update(spreadsheetId=LOG_FILE_ID, range=range_, valueInputOption="USER_ENTERED", body=value_range_body).execute()
                print(f"Updated log with file ID {file_id} in column {column} with value {update_value}")
            del values
            return row
    del values
    return


def update_logs(sheets_service,file_id, tab_id, file_name, tab_name, new_row_values):
    sheet_name='logs'
    data_range = f'{sheet_name}!A:I' #Este range es solo para poder leer el id_archivo y id_pestaña de los logs
    
    # Read the data from the sheet
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=LOG_FILE_ID,
        range=data_range).execute()

    # Get the values from the result
    values = result.get('values', [])
    del result #just to freeup memory

    # Go through each row to look for matches. Start at the end for quicker find
    for i in range(len(values) - 1, -1, -1):
        row=values[i]
        if row and row[1] == file_id and str(row[8]) == tab_id:
            #update values for file_name and tab_name in the logs file
            row[0]=file_name
            row[7]=tab_name
            new_row_values=row+new_row_values
            range_ = f"{sheet_name}!A{i+1}:AI{i+1}"  # Replace the full row (columns A to AI)
            body = {
                "values": [new_row_values]
            }
            sheets_service.spreadsheets().values().update(
                spreadsheetId=LOG_FILE_ID,
                range=range_,
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()
            print(f"Updated entire row {i+1} for file ID {file_id}")
            del values
            return new_row_values
    del values
    return

def inspect_logs(sheets_service,file_id,tab_id, file_name, tab_name, logs_data):
   
    row = update_logs(sheets_service,file_id, tab_id, file_name, tab_name, logs_data) #update file_name in column A in all occurences and return all rows that match the file_id
    
    if not row:
        return (None, None, None)
    if len(row)>6:
        calendar_id=row[2]
        folder_id=row[3]
        folder_link=row[6]
    elif len(row)>3:
        calendar_id=row[2]
        folder_id=row[3]
        folder_link=None
    elif len(row)>2:
        calendar_id=row[2]
        folder_id=None
        folder_link=None
    else:
        calendar_id=None
        folder_id=None
        folder_link=None
    return (calendar_id, folder_id, folder_link)
  
    
def delete_logs(sheets_service, file_id):
    sheet_name = 'logs'
    data_range = f'{sheet_name}!A:AI'

    # Get only the required data, skipping the spreadsheet metadata request
    result = sheets_service.spreadsheets().values().get(spreadsheetId=LOG_FILE_ID, range=data_range).execute()
    values = result.get('values', [])

    search_string = file_id
    rows_to_delete = []
    calendar_ids = []
    folder_ids = []

    # Collect indices of rows to delete
    for i, row in enumerate(values):
        if row and row[1] == search_string:
            calendar_ids.append(row[2])
            folder_ids.append(row[3])
            rows_to_delete.append(i)  # Collect indices to delete later

    if not rows_to_delete:
        print(f"No matching rows found for {search_string}")
        return (calendar_ids, folder_ids)

    # Prepare batch delete request (reverse order to prevent shifting issues)
    delete_requests = [
        {
            "deleteDimension": {
                "range": {
                    "sheetId": 0,  # Sheet ID is often 0 for the first sheet; confirm if needed
                    "dimension": "ROWS",
                    "startIndex": i,
                    "endIndex": i + 1
                }
            }
        }
        for i in sorted(rows_to_delete, reverse=True)
    ]

    # Execute batch delete request
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=LOG_FILE_ID,
        body={"requests": delete_requests}
    ).execute()

    print(f"Deleted {len(rows_to_delete)} rows in {sheet_name}")
    return (calendar_ids, folder_ids)

def odd_minute_random():
    return datetime.now().minute % 2 == 1

def get_tabs(sheets_service, file_id, keyword, random):
    # Fetch details of the spreadsheet using the Sheets API
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=file_id).execute()
    sheets = spreadsheet.get('sheets', [])
    
    tabs_names=[]
    tabs_ids=[]
    
    # If random is True, check if the current minute is odd
    if random and odd_minute_random():
        sheets=sheets[::-1]  # Reverse the order of sheets only if user chose random and there is an odd minute  

    # Iterate through each sheet and check if the keyword is present in the sheet name
    for sheet in sheets:
        sheet_title = sheet['properties']['title']
        if keyword in sheet_title:
            sheet_id = str(sheet['properties']['sheetId'])
            tabs_names.append(sheet_title)
            tabs_ids.append(sheet_id)
    return (tabs_names,tabs_ids)

def get_data_hl(sheets_service, file_id, tab_name,file_name, multiday): 
    from pandas import DataFrame, concat, Series, NA

    try:
        # Fetch only ITINERARIO to check the date
        first_sheet_range = f"{tab_name}{getenv('NEW_ITINERARIO_RANGE')}"
        response = sheets_service.spreadsheets().values().get(
            spreadsheetId=file_id, range=first_sheet_range
        ).execute()

        values_first_sheet = response.get('values', [])

        if not values_first_sheet:
            print(f'No data found in {tab_name}.')
            return
        # Ensure matrix is square
        values_first_sheet = make_square_matrix(values_first_sheet)
    except Exception as e:
        print("Error fetching data from sheets ITINERARIO:", str(e))
        return
    try:
        # Convert to DataFrame and clean
        first_sheet_df = DataFrame(values_first_sheet[1:], columns=values_first_sheet[0])
        first_sheet_df = first_sheet_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)


        # Validate and format start and end dates
        start_date=first_sheet_df.loc[0, 'Fecha de inicio']

        if not first_sheet_df.loc[0, 'Hora de inicio']:
            first_sheet_df.loc[0, 'Hora de inicio'] = '06:00'
        date_time_start = start_date + 'T' + first_sheet_df.loc[0, 'Hora de inicio'] + ':00'
        if not first_sheet_df.loc[0, 'Hora de fin']:
            first_sheet_df.loc[0, 'Hora de fin'] = '18:00'
        date_time_end = first_sheet_df.loc[0, 'Fecha de fin'] + 'T' + first_sheet_df.loc[0, 'Hora de fin'] + ':00'

    except Exception as e:
        print("There's a problem with the start or end date:", str(e))
        return

    # Step 2: Skip if the event is in the past
    if seconds_since(date_time_end, False) > SECONDS_THRESHOLD_UPDATE:
        print("Start date is set in the past. Skipping event.")
        return
         
    try:

        #si son los tabs adicionales de un multiday no se necesita datos de pagos
        if multiday== 'SI' and not is_date_in_filename(start_date, file_name):
            ranges = [f"VIAJEROS{getenv('VIAJEROS_RANGE')}"]
            batch_response = sheets_service.spreadsheets().values().batchGet(
            spreadsheetId=file_id, ranges=ranges
            ).execute()
            values_second_sheet = batch_response['valueRanges'][0].get('values', [])
            if not values_second_sheet:
                print("No data found in VIAJEROS.")
                return
        else:  
            # Step 3: Fetch the remaining sheets since the event is valid
            ranges = [
                f"VIAJEROS{getenv('VIAJEROS_RANGE')}",
                f"PAGOS{getenv('PAGOS_RANGE')}",
            ]
            batch_response = sheets_service.spreadsheets().values().batchGet(
                spreadsheetId=file_id, ranges=ranges
            ).execute()

            values_second_sheet = batch_response['valueRanges'][0].get('values', [])
            values_pagos_sheet = batch_response['valueRanges'][1].get('values', [])

            if not values_second_sheet:
                print("No data found in VIAJEROS.")
                return
            if not values_pagos_sheet:
                print("No data found in PAGOS.")
                return

        values_second_sheet = make_square_matrix(values_second_sheet)
        second_sheet_df = DataFrame(values_second_sheet[1:], columns=values_second_sheet[0])
        second_sheet_df = second_sheet_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    except Exception as e:
        print("Error fetching data from sheets VIAJEROS or PAGOS:", str(e))
        return
    
    all_data={'multiday':multiday,'start_date':start_date,'date_time_start':date_time_start, 'date_time_end':date_time_end}
    
    # Process the ITINERARIO sheet for tour information
    try:
        guia = first_sheet_df.loc[0, 'Guia principal']
        chofer = first_sheet_df.loc[0, 'Chofer']
        staff = set(concat([Series(guia), first_sheet_df['Guia apoyo'], Series(chofer)]).reset_index(drop=True))
        all_data['tour_name'] = first_sheet_df.loc[0, 'Tour']
        all_data['guia'] = guia
        all_data['apoyo'] = list_to_str_commas(first_sheet_df.loc[:, 'Guia apoyo'])
        all_data['chofer'] = chofer
        all_data['atendees'] = get_emails(sheets_service, staff)
        all_data['transporte'] = first_sheet_df.loc[0, 'Transporte']
        all_data['comentarios'] = first_sheet_df.loc[0, 'Comentarios']
        all_data['logistica'] = first_sheet_df.loc[0, 'Logistica']
        all_data['avisos'] = first_sheet_df.loc[0, 'Avisos']
        # Retain renta_bicis logic
        all_data['renta_bicis'] = first_sheet_df.loc[0, 'Renta de bicis'] if 'Renta de bicis' in first_sheet_df.columns else None
        del first_sheet_df
        
    except Exception as e:
        print(f"There's a problem with the dataframe from sheet {tab_name}:", str(e))
        return

    
    # Process the VIAJEROS sheet for client status information
    try:
        second_sheet_df['STATUS'] = second_sheet_df['STATUS'].replace(r'^\s*$', NA, regex=True).fillna('VIAJAN ✅')
        second_sheet_df['PUNTO DE VENTA'] = second_sheet_df['PUNTO DE VENTA'].replace(r'^\s*$', NA, regex=True).fillna('OTRO')
        # Map statuses
        second_sheet_df['STATUS'] = second_sheet_df['STATUS'].replace({
            'RESERVADO✅': 'VIAJAN ✅',
            'REBOOKED⚠️': 'NO VIAJAN 🚫',
            'CANCELADO🚫': 'NO VIAJAN 🚫'
        })
        second_sheet_df = second_sheet_df.sort_values(by='STATUS') #TODO: Desde aquí hasta obtener el generator de clientes es posible que se pueda optimizar 
        clientes_by_status = second_sheet_df.groupby(['STATUS', 'PUNTO DE VENTA'])['NOMBRE'].apply(list)
        clientes_by_status = clientes_by_status.sort_index(level='STATUS', ascending=False)
        clientes = get_clientes(clientes_by_status)
        all_data['clientes']=clientes
        all_data['num_clientes']=second_sheet_df.STATUS.eq('VIAJAN ✅').sum()
        del second_sheet_df
    except Exception as e:
        print("There's a problem with the dataframe from sheet VIAJEROS:", str(e))
        return
    
        #si son los tabs adicionales de un multiday no poner datos redundantes
    if multiday== 'SI' and not is_date_in_filename(start_date, file_name):
        all_data['num_clientes']=None
        all_data['venta']=None
        all_data['gastos']=None
        all_data['gasto_efectivo']=None
        all_data['combustible']=None
        all_data['pago_chofer'] = None
        all_data['pago_guia'] = None
        all_data['pago_apoyo'] = None
        all_data['pago_apoyo_2'] = None
        all_data['pago_apoyo_3'] = None
        all_data['multiday']=None
        all_data['cobro_efectivo']=None
        all_data['cobro_transfe'] = None
        all_data['cobro_izettle'] = None
        all_data['cobro_fareharbor'] = None
        all_data['cobro_airbnb'] = None
        all_data['cobro_tripadvisor'] = None
        all_data['cobro_get_your_guide'] = None
        all_data['cobro_otros'] = None

        return all_data

    # Process the PAGOS sheet
    try:
        all_data['venta']= parse_currency(safe_parse(values_pagos_sheet, 0))#only use the row index, in the function i already set the column to 0
        all_data['gastos']=parse_currency(safe_parse(values_pagos_sheet, 1))
        all_data['gasto_efectivo']=parse_currency(safe_parse(values_pagos_sheet, 9))
        all_data['combustible'] = parse_currency(safe_parse(values_pagos_sheet, 10))
        all_data['pago_chofer'] = parse_currency(safe_parse(values_pagos_sheet, 11))
        all_data['pago_guia'] = parse_currency(safe_parse(values_pagos_sheet, 12))
        all_data['pago_apoyo'] = parse_currency(safe_parse(values_pagos_sheet, 13))
        all_data['pago_apoyo_2'] = parse_currency(safe_parse(values_pagos_sheet, 14))
        all_data['pago_apoyo_3'] = parse_currency(safe_parse(values_pagos_sheet, 15))

        #Sección de cobros
        all_data['cobro_efectivo']=parse_currency(safe_parse(values_pagos_sheet, 5, 3)) #column 3 is the amount of cobros
        all_data['cobro_transfe']=parse_currency(safe_parse(values_pagos_sheet, 6, 3))
        all_data['cobro_izettle'] = parse_currency(safe_parse(values_pagos_sheet, 7, 3))
        all_data['cobro_fareharbor'] = parse_currency(safe_parse(values_pagos_sheet, 8, 3))
        all_data['cobro_airbnb'] = parse_currency(safe_parse(values_pagos_sheet, 9, 3))
        all_data['cobro_tripadvisor'] = parse_currency(safe_parse(values_pagos_sheet, 10, 3))
        all_data['cobro_get_your_guide'] = parse_currency(safe_parse(values_pagos_sheet, 11, 3))
        all_data['cobro_otros'] = parse_currency(safe_parse(values_pagos_sheet, 12, 3))

    except Exception as e:
        print("There's a problem with the data from sheet PAGOS:", str(e))
        return
    
    return all_data

def is_date_in_filename(date_str, text):
    try:
        # Parse input date string
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        
        # Create both formats
        format_1 = date_obj.strftime('%Y-%m-%d')  # e.g., 2025-01-30
        format_2 = date_obj.strftime('%d-%m-%Y')  # e.g., 30-01-2025

        return format_1 in text or format_2 in text
    except ValueError:
        return False


def safe_parse(sheet, row, col=0):
    try:
        return sheet[row][col]
    except (IndexError, TypeError):
        return 0  # fallback value

def parse_currency(value):
    try:
        if value is None:
            return 0

        value = str(value).strip()

        # Case 1: comma as decimal separator (European-style)
        if ',' in value and not '.' in value:
            cleaned = re.sub(r'[^\d,]', '', value).replace(',', '.')
        # Case 2: both dot and comma present (European-style with thousands separator)
        elif '.' in value and ',' in value:
            cleaned = re.sub(r'[^\d,\.]', '', value).replace('.', '').replace(',', '.')
        # Case 3: US-style (dot is decimal separator)
        else:
            cleaned = re.sub(r'[^\d.-]', '', value)

        return float(cleaned) if cleaned else 0
    except (ValueError, TypeError):
        return 0


def create_calendar(calendar_service, file_link, file_type, file_name, all_data):
    try:
        tour_name = all_data['tour_name']
        guia = all_data['guia']
        apoyo = all_data['apoyo']
        chofer = all_data['chofer']
        atendees = all_data['atendees']
        transporte = all_data['transporte']
        comentarios = all_data['comentarios']
        logistica = all_data['logistica']
        avisos = all_data['avisos']
        renta_bicis = all_data['renta_bicis']
        clientes=all_data['clientes']
        date_time_start=all_data['date_time_start']
        date_time_end=all_data['date_time_end']
    except Exception as e:
        print("Event was not created. There's a problem getting data from dictionary all_data:", str(e))
        return (None, None)
    
    # Build the description text using client data and event details
    description=''
    for cliente in clientes:
        description+=cliente
    
    # Determine event color and append extra comments
    if renta_bicis is None:
        if 'street art' in tour_name.lower() and guia != '❌' and guia != '' and logistica == '✅':
            color = '2'  # verde
        elif (guia != '❌' and guia != '') and (chofer != '❌' and chofer != '') \
                and (transporte != '❌' and transporte != '') and avisos == '✅' and logistica == '✅':
            color = '2'  # verde
        elif (guia == '❌' or guia == '') and (chofer == '❌' or chofer == '') \
                and (transporte == '❌' or transporte == '') and (avisos == '❌' or avisos == '') \
                and (logistica == '❌' or logistica == ''):
            color = '6'  # rojo
        else:
            color = '5'  # amarillo
        description += f'\nComentarios:\n{comentarios}\n\nGuía: {guia}\nGuía de apoyo: {apoyo}\nChofer: {chofer}\nTransporte: {transporte}\nLogística: {logistica}\nAvisos: {avisos}'
    else:
        if 'street art' in tour_name.lower() and guia != '❌' and guia != '' and logistica == '✅' and renta_bicis == '✅':
            color = '2'  # verde
        elif (guia != '❌' and guia != '') and (chofer != '❌' and chofer != '') \
                and (transporte != '❌' and transporte != '') and avisos == '✅' and logistica == '✅' and renta_bicis == '✅':
            color = '2'  # verde
        elif (guia == '❌' or guia == '') and (chofer == '❌' or chofer == '') \
                and (transporte == '❌' or transporte == '') and (avisos == '❌' or avisos == '') \
                and (logistica == '❌' or logistica == '') and (renta_bicis == '❌' or renta_bicis == ''):
            color = '6'  # rojo
        else:
            color = '5'  # amarillo
        description += f'\nComentarios:\n{comentarios}\n\nGuía: {guia}\nGuía de apoyo: {apoyo}\nChofer: {chofer}\nTransporte: {transporte}\nLogística: {logistica}\nAvisos: {avisos}\nRenta de bicis: {renta_bicis}'
  
    # Construct the calendar event object
    event = {
        'summary': tour_name,
        'location': 'C. Macedonio Alcalá 802, RUTA INDEPENDENCIA, Centro, 68000 Oaxaca de Juárez, Oax.',
        'description': description,
        'colorId': color,
        'start': {
            'dateTime': date_time_start,
            'timeZone': 'America/Mexico_City',
        },
        'end': {
            'dateTime': date_time_end,
            'timeZone': 'America/Mexico_City',
        },
        'attendees': [{'email': attendee} for attendee in atendees if attendee],
        'reminders': {'useDefault': True},
        'attachments': [
            {
                'fileUrl': file_link,
                'mimeType': file_type,
                'title': file_name
            }
        ]
    }
    
    try:
        created_event = calendar_service.events().insert(
            calendarId='primary', sendUpdates='all', body=event, supportsAttachments=True
        ).execute()
    except errors.HttpError as e:
        print('Event not created, an error occurred:', str(e))
        return (None, None)
    
    calendar_id = created_event['id']
    calendar_link = created_event['htmlLink']
    print(f'Event created successfully! ID: {calendar_id}')
    return (calendar_id, calendar_link)

def create_photos_folder(drive_service,file_name,tab_name):
    #CREATE THE FOLDER FOR THE PHOTOS OF THIS EXPERIENCE WITH THE SAME NAME AS THE SHEETS FILE
    try:     
        file_month=get_month_from_file_name(file_name)
        file_year=get_year_from_file_name(file_name)
        folders=['Workflow Coyote Armando Technologies', file_year,'Fotos Tours', file_month]
        all_folders_found, last_folder_id, last_folder_name=find_last_subfolder_id(drive_service, folders)
        if all_folders_found:
            month_folder_id=last_folder_id
        elif last_folder_id and last_folder_name:
            print("No se encontraron todos los subfolders, se intentará crearlos")
            split_element = last_folder_name
            if split_element in folders:
                split_index = folders.index(split_element)
                folders = folders[split_index :]    
                month_folder_id = create_folder_tree(drive_service, folders, last_folder_id)
        else:
            print(f'Folder for photos was not created, it was not possible to fetch folder: {last_folder_name}')
            month_folder_id=None  
        
    except errors.HttpError as e:
        print('Folder for photos was not created, an error has occured: ',str(e))
        month_folder_id=None
    
    if month_folder_id:   #Antes tenía and not find_folder(drive_service, file_name, month_folder_id) para que no creara otro folder si ya existe uno
        if tab_name == 'ITINERARIO':
            name=file_name
        else:
            name=file_name+' '+tab_name
        folder_metadata = {
            'name': name, 
            'parents': [month_folder_id],
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        created_folder = drive_service.files().create(body=folder_metadata, fields='id, webViewLink').execute()
        folder_id=created_folder['id']
        folder_link=created_folder['webViewLink']
        print(f'Folder for photos created succesfully! ID:{folder_id}')
    else:
        folder_id=None
        folder_link=None
        print('Folder for photos was not created. There is a problem with the month folder')
    
    return (folder_id,folder_link)

def attach_folder_to_calendar(calendar_service, calendar_id,folder_link):
    if folder_link:
        try:
            event = calendar_service.events().get(calendarId='primary', eventId=calendar_id).execute()
            event['attachments'].append(
                { 
                    'fileUrl': folder_link,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'title': 'Folder de fotos'
                }
            )
            updated_event = calendar_service.events().update(calendarId='primary', eventId=calendar_id, body=event).execute()
        
            if updated_event:
                return True
        except errors.HttpError as e:
            print('Photos folder not attached to calendar event ',str(e))
    print('Photos folder not attached to calendar event, folder link not provided')
    return   



        
def update_calendar_and_folder(drive_service,calendar_service, calendar_id, folder_id, file_name, tab_name, folder_link,file_link,file_type, all_data):    
    try:
        tour_name = all_data['tour_name']
        guia = all_data['guia']
        apoyo = all_data['apoyo']
        chofer = all_data['chofer']
        atendees = all_data['atendees']
        transporte = all_data['transporte']
        comentarios = all_data['comentarios']
        logistica = all_data['logistica']
        avisos = all_data['avisos']
        renta_bicis = all_data['renta_bicis']
        clientes=all_data['clientes']
        date_time_start=all_data['date_time_start']
        date_time_end=all_data['date_time_end']
    except Exception as e:
        print("Event was not updated. There's a problem getting data from dictionary all_data:", str(e))
        return
    
    # Build the description text using client data and event details
    description=''
    for cliente in clientes:
        description+=cliente
        
    if renta_bicis is None:
        if 'street art' in tour_name.lower() and guia!='❌' and guia!='' and logistica=='✅':
            color= '2' #verde
        
        elif (guia!='❌' and guia!='') and (chofer!='❌' and chofer!='')\
        and (transporte!='❌' and transporte!='') and avisos=='✅' and logistica=='✅' :
            color='2' #verde
        
        elif (guia=='❌' or guia=='') and (chofer=='❌' or chofer=='')\
        and (transporte=='❌' or transporte=='') and (avisos=='❌' or avisos=='') and (logistica=='❌' or logistica=='') :
            color='6' #rojo
        
        else:
            color='5' #amarillo
        description+=f'\nComentarios:\n{comentarios}\n\nGuía: {guia}\nGuía de apoyo: {apoyo}\nChofer: {chofer}\nTransporte: {transporte}\nLogística: {logistica}\nAvisos: {avisos}'
    
    else:
        if 'street art' in tour_name.lower() and guia!='❌' and guia!='' and logistica=='✅' and renta_bicis=='✅':
            color= '2' #verde
        
        elif (guia!='❌' and guia!='') and (chofer!='❌' and chofer!='')\
        and (transporte!='❌' and transporte!='') and avisos=='✅' and logistica=='✅' and renta_bicis=='✅':
            color='2' #verde
        
        elif (guia=='❌' or guia=='') and (chofer=='❌' or chofer=='')\
        and (transporte=='❌' or transporte=='') and (avisos=='❌' or avisos=='') and (logistica=='❌' or logistica=='') and (renta_bicis=='❌' or renta_bicis==''):
            color='6' #rojo
        
        else:
            color='5' #amarillo
        description+=f'\nComentarios:\n{comentarios}\n\nGuía: {guia}\nGuía de apoyo: {apoyo}\nChofer: {chofer}\nTransporte: {transporte}\nLogística: {logistica}\nAvisos: {avisos}\nRenta de bicis: {renta_bicis}'

    if bool(re.search(r'cancela(do)?|cance(lo)?', file_name, re.IGNORECASE)):
        color='3' #morado
        tour_name='CANCELADO '+tour_name
    
    #Update event 
    event = {
        'summary': tour_name,
        'location': 'C. Macedonio Alcalá 802, RUTA INDEPENDENCIA, Centro, 68000 Oaxaca de Juárez, Oax.',
        'description': description,
        'colorId': color,
        'start': {
            'dateTime': date_time_start,
            'timeZone': 'America/Mexico_City',  # Adjust this to your desired timezone
        },
        'end': {
            'dateTime': date_time_end,
            'timeZone': 'America/Mexico_City',  # Adjust this to your desired timezone
        },
        'attendees': [
            {'email': attendee} for attendee in atendees if attendee
        ],
        'reminders': {
            'useDefault': True,
        }
    }
    
    if folder_link:
        event['attachments']=[
            {
                'fileUrl': file_link,
                'mimeType': file_type,
                'title': file_name
            },
            { 
                'fileUrl': folder_link,
                'mimeType': 'application/vnd.google-apps.folder',
                'title': 'Folder de fotos'
                }
        ]
    else:
        event['attachments']=[
            {
                'fileUrl': file_link,
                'mimeType': file_type,
                'title': file_name
            }
        ]
    
    try:
        existing_event = calendar_service.events().get(calendarId='primary', eventId=calendar_id).execute()
        text=existing_event.get('description')
        relevant_existing_info=text.split('Comentarios')[1].split('Chofer')[0].strip()
        relevant_new_info=description.split('Comentarios')[1].split('Chofer')[0].strip()
        if relevant_existing_info==relevant_new_info:
            send_update='none'
        else:
            send_update='all'
    
        updated_event = calendar_service.events().update(calendarId='primary', eventId=calendar_id, sendUpdates=send_update, body=event, supportsAttachments=True).execute()
    except errors.HttpError as e:
        print('Event not updated, an error has occured:',str(e))
        return
    print(f'Event updated succesfully: {updated_event["htmlLink"]}')
    
    #UPDATE THE PHOTOS FOLDER:
    try:
        if tab_name == 'ITINERARIO':
            name=file_name
        else:
            name=file_name+' '+tab_name
        drive_service.files().update(fileId=folder_id, body={'name': name}).execute()
        print(f"Photos folder name updated succesfully: {name}")
    except errors.HttpError as e:
        print('Name not updated in photos folder, an error has occured:',str(e))
        
    return

def get_emails(sheets_service, staff):
    mails=[]
    sheet_name = 'mails'
    data_range = sheet_name+str(getenv('EMAILS_RANGE'))
     # Read the data from the sheet
    try:
        result = sheets_service.spreadsheets().values().get(spreadsheetId=EMAIL_FILE_ID,range=data_range).execute()
        # Get the values from the result
        values = result.get('values', [])
        del result
        values = make_square_matrix(values)
        for person in staff:
            if person:
                # Go through each row in column A
                for row in values:
                    found=False
                    if row[0] and row[0].strip() == person.strip():
                        found=True
                        if row[1]:
                            mails.append(row[1].strip())
                        break
                if found==False:
                    print(f"{person} not found in list of emails")
    except errors.HttpError as e:
        print('An error has occured getting the emails of the staff:',str(e))
    return(mails)

# Callback function to handle responses from batch requests
def batch_delete_callback(request_id, response, exception):
    if exception:
        logging.error(f"Error in request {request_id}: {exception}, Response: {response}")
    else:
        logging.info(f"Successfully processed request {request_id}, Response: {response}")

# Function to delete multiple calendar events in batch
def batch_delete_calendar_events(calendar_service, calendar_ids):
    batch = calendar_service.new_batch_http_request(callback=batch_delete_callback)
    
    for event_id in calendar_ids:
        batch.add(calendar_service.events().delete(calendarId="primary", eventId=event_id))
    
    batch.execute()

# Function to delete multiple folders from Google Drive in batch
def batch_delete_drive_folders(drive_service, folder_ids):
    batch = drive_service.new_batch_http_request(callback=batch_delete_callback)
    
    for folder_id in folder_ids:
        batch.add(drive_service.files().delete(fileId=folder_id))
    
    batch.execute()

# Main function to delete calendar events and folders in batch
def delete_calendar_and_folders_batch(drive_service, calendar_service, calendar_ids, folder_ids):
    # Delete calendar events in batch
    if calendar_ids:
        logging.info(f"Starting to delete {len(calendar_ids)} calendar events.")
        batch_delete_calendar_events(calendar_service, calendar_ids)
    else:
        logging.warning(f"No calendar events to delete")
        
    
    # Delete folders in batch
    if folder_ids:
        logging.info(f"Starting to delete {len(folder_ids)} folders.")
        batch_delete_drive_folders(drive_service, folder_ids)
    else:
        logging.warning(f"No photo folders to delete")


def make_square_matrix(matrix): #TODO: checar cómo se ve matrix y ver si se puede optimizar esta función
    if len(matrix)>0:
        max_length = max(len(row) for row in matrix)
        for row in matrix:
            while len(row) < max_length:
                row.append(None)
    return matrix
        
def store_state(current_time,change_time,page_token, firestore_db):
    # Store the 'last_page_token' in the 'state' document of the 'app' collection
    firestore_db.collection('app').document('state').set({'last_page_token': page_token,'last_processed_change_time':change_time,'retrieve_time': current_time}, merge=True)
        

def get_last_state(firestore_db):
    # Get the 'state' document from the 'app' collection
    state_doc = firestore_db.collection('app').document('state').get()
    
    if state_doc.exists:
        # If the document exists, return the 'last_page_token' field
        return state_doc.to_dict()
    else:
        # If the document does not exist, return None
        return {'last_page_token': None,'last_processed_change_time':None,'retrieve_time': None }

def find_folder(drive_service, month, parent_folder_id):
    query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    del results

    if len(files) > 0:
        for file in files:
            if bool(re.fullmatch(month,(file['name']),re.IGNORECASE)):
                return (file['id'])
        print(f"No folders with the name {month} found in the parent folder {parent_folder_id}")
        return
    else:
        print(f"The parent folder {parent_folder_id} does not have any folders inside")
        return

def get_year_from_file_name(file_name):
    try:
        pattern = r'\b(\d{2}-\d{2}-\d{4})\b'
        match = re.search(pattern, file_name)
        if match:
            date= match.group(1)
            year_digits=date.split('-')[2]
            return year_digits
        pattern = r'\b(\d{4}-\d{2}-\d{2})\b'
        match = re.search(pattern, file_name)
        if match:
            date= match.group(1)
            year_digits=date.split('-')[0]
            return year_digits
    except Exception as error:
        print('Error extracting the year from the date: '+str(error))
        return
    
def get_month_from_file_name(file_name):
    month_mapping = {
        "01": "enero",
        "02": "febrero",
        "03": "marzo",
        "04": "abril",
        "05": "mayo",
        "06": "junio",
        "07": "julio",
        "08": "agosto",
        "09": "septiembre",
        "10": "octubre",
        "11": "noviembre",
        "12": "diciembre"
    }

    try:
        pattern = r'\b(\d{2}-\d{2}-\d{4})\b'
        match = re.search(pattern, file_name)
        if match:
            date= match.group(1)
            month_digits=date.split('-')[1]
            return(month_mapping[month_digits])
        pattern = r'\b(\d{4}-\d{2}-\d{2})\b'
        match = re.search(pattern, file_name)
        if match:
            date= match.group(1)
            month_digits=date.split('-')[1]
            return(month_mapping[month_digits])
        
    except Exception as error:
        print('Error extracting the month from the date: '+str(error))
        return

def find_last_subfolder_id(drive_service, directory_tree):
    parent_folder_id = 'root'  # Start from the root folder ID
    for folder_name in directory_tree:
        try: 
            current_folder_id=find_folder(drive_service, folder_name, parent_folder_id)
            if not current_folder_id:
                return (False, parent_folder_id, folder_name)  # Folder not found, return last parent
            parent_folder_id=current_folder_id

        except Exception as e:
            print(f"An error occurred: {e}")
            return (False, None, folder_name)

    return (True, current_folder_id, folder_name)

def create_folder_tree(drive_service, folder_names, parent_folder_id):
    current_parent_id = parent_folder_id

    for folder_name in folder_names:
        try:
            # Create a folder with the current name inside the current parent folder
            folder_metadata = {
                'name': folder_name,
                'parents': [current_parent_id],
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
            current_parent_id = folder['id']

        except Exception as e:
            print(f"An error occurred while creating the folder tree: {e}")
            return None

    return current_parent_id  # Return the ID of the last created folder  