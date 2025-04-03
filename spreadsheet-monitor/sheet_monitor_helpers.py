import googleapiclient.errors as errors
from datetime import datetime
from os import getenv
import re
LOG_FILE_ID= str(getenv('LOG_FILE_ID'))
SECONDS_THRESHOLD_UPDATE = int(getenv('SECONDS_THRESHOLD_UPDATE'))

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
                       # yield (f'\n-{employers_uppercase}-\n')
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

def create_logs(sheets_service,file_name,tab_name, file_id, tab_id, calendar_id,photos_folder_id,file_link,calendar_link,photos_folder_link):
    sheet_name = 'logs'  # replace with your sheet name

    # How the input data should be interpreted.
    value_input_option = 'USER_ENTERED'  # or 'RAW'

    value_range_body = {
        "majorDimension": "ROWS",  # "ROWS"|"COLUMNS"
        "values": [
            [file_name,file_id, calendar_id,photos_folder_id,file_link,calendar_link,photos_folder_link,tab_name,tab_id]
        ]
    }

    sheets_service.spreadsheets().values().append(spreadsheetId=LOG_FILE_ID, range=sheet_name, valueInputOption=value_input_option, body=value_range_body).execute()
    print('IDs added to log file')

def update_logs(sheets_service,file_id, tab_id, update_values,columns):
    sheet_name='logs'
    data_range = f'{sheet_name}!A:I'
    
    # Read the data from the sheet
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=LOG_FILE_ID,
        range=data_range).execute()

    # Get the values from the result
    values = result.get('values', [])
    del result #just to freeup memory

    # Go through each row to look for matches
    for i,row in enumerate(values):
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

def inspect_logs(sheets_service,file_id,tab_id, file_name, tab_name):
   
    row = update_logs(sheets_service,file_id, tab_id, [file_name,tab_name], ['A','H']) #update file_name in column A in all occurences and return all rows that match the file_id
    
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
  
    
def delete_logs(sheets_service,file_id):
    sheet_name='logs'
    data_range = f'{sheet_name}!A:I'

    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=LOG_FILE_ID).execute()
    sheets = spreadsheet.get('sheets', '')
    sheetId = None
    for sheet in sheets:
        if sheet.get('properties', {}).get('title') == sheet_name:
            sheetId = sheet.get('properties', {}).get('sheetId')
            break    
    result = sheets_service.spreadsheets().values().get(spreadsheetId=LOG_FILE_ID, range=data_range).execute()
    values = result.get('values', [])
    del result
    
    search_string = file_id 

    # Iterate over the rows
    calendar_ids=[]
    folder_ids=[]
    #for i, row in enumerate(values):
    i=0
    j=0
    while i < len(values):
        row=values[i]
        # If the search string is found in column B (index 0)
        if row and row[1] == search_string:
            calendar_id=row[2]
            folder_id=row[3]
                    
            # Delete the row
            request = sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=LOG_FILE_ID,
                body={
                    "requests": [
                        {
                            "deleteDimension": {
                                "range": {
                                    "sheetId": sheetId,
                                    "dimension": "ROWS",
                                    "startIndex": j,
                                    "endIndex": j + 1
                                }
                            }
                        }
                    ]
                }
            )
            request.execute()
            #Aqui tal vez poner un if request: para asegurarme que sí borró esa madre
            print(f"Deleted row {i + 1} in {sheet_name}")
            calendar_ids.append(calendar_id)
            folder_ids.append(folder_id)
        else:
            j+=1
        i+=1
    del values
    return (calendar_ids, folder_ids)

def get_tabs(sheets_service, file_id, keyword):
    # Fetch details of the spreadsheet using the Sheets API
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=file_id).execute()
    sheets = spreadsheet.get('sheets', [])
    
    tabs_names=[]
    tabs_ids=[]
    # Iterate through each sheet and check if the keyword is present in the sheet name
    for sheet in sheets:
        sheet_title = sheet['properties']['title']
        if keyword in sheet_title:
            sheet_id = str(sheet['properties']['sheetId'])
            tabs_names.append(sheet_title)
            tabs_ids.append(sheet_id)
    return (tabs_names,tabs_ids)


def create_calendar(drive_service, sheets_service, calendar_service, file_id, file_name, tab_name):
    from pandas import DataFrame, concat, Series

    try:
        # Use batchGet to fetch ranges from the same spreadsheet at once:
        ranges = [
            f"{tab_name}{getenv('ITINERARIO_RANGE')}",
            f"{tab_name}{getenv('NEW_ITINERARIO_RANGE')}",
            f"VIAJEROS{getenv('VIAJEROS_RANGE')}"
        ]
        batch_response = sheets_service.spreadsheets().values().batchGet(
            spreadsheetId=file_id, ranges=ranges
        ).execute()

        # The first two ranges correspond to ITINERARIO (old and new version)
        values_first_sheet = batch_response['valueRanges'][0].get('values', [])
        values_new_sheet = batch_response['valueRanges'][1].get('values', [])
        # The third range corresponds to VIAJEROS
        values_second_sheet = batch_response['valueRanges'][2].get('values', [])
        
        # Decide which ITINERARIO range to use based on cell A1
        if not values_first_sheet or not values_first_sheet[0] or values_first_sheet[0][0] != 'Guia principal':
            values_first_sheet = values_new_sheet

        if not values_first_sheet:
            print(f'Event was not created. No data found in {tab_name}.')
            return (None, None)
        if not values_second_sheet:
            print("Event was not created. No data found in VIAJEROS.")
            return (None, None)
        
        # Ensure the matrices are square
        values_first_sheet = make_square_matrix(values_first_sheet)
        values_second_sheet = make_square_matrix(values_second_sheet)
        
    except Exception as e:
        print("Event was not created. File doesn't have sheets 'ITINERARIO' o 'VIAJEROS':", str(e))
        return (None, None)
    
    # Convert fetched data to pandas DataFrames and clean string values
    try:
        first_sheet_df = DataFrame(values_first_sheet[1:], columns=values_first_sheet[0])
        first_sheet_df = first_sheet_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        second_sheet_df = DataFrame(values_second_sheet[1:], columns=values_second_sheet[0])
        second_sheet_df = second_sheet_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    except Exception as e:
        print("Error processing sheet data:", str(e))
        return (None, None)
    
    # Validate and format start and end dates
    try:
        if not first_sheet_df.loc[0, 'Hora de inicio']:
            first_sheet_df.loc[0, 'Hora de inicio'] = '06:00'
        date_time_start = first_sheet_df.loc[0, 'Fecha de inicio'] + 'T' + first_sheet_df.loc[0, 'Hora de inicio'] + ':00'
        if not first_sheet_df.loc[0, 'Hora de fin']:
            first_sheet_df.loc[0, 'Hora de fin'] = '18:00'
        date_time_end = first_sheet_df.loc[0, 'Fecha de fin'] + 'T' + first_sheet_df.loc[0, 'Hora de fin'] + ':00'
    except Exception as e:
        print("Event was not created. There's a problem with the start or end date:", str(e))
        return (None, None)
    
    # Ensure the event is set in the future (seconds_since() must return 0 for a valid future event)
    if seconds_since(date_time_start, False) >= 0:
        print("Start date is set in the past")
        return (None, None)
    
    # Extract event details from the ITINERARIO sheet
    try:
        summary = first_sheet_df.loc[0, 'Tour']
        guia = first_sheet_df.loc[0, 'Guia principal']
        apoyo = list_to_str_commas(first_sheet_df.loc[:, 'Guia apoyo'])
        chofer = first_sheet_df.loc[0, 'Chofer']
        staff = set(concat([Series(guia), first_sheet_df['Guia apoyo'], Series(chofer)]).reset_index(drop=True))
        atendees = get_emails(sheets_service, staff)
        transporte = first_sheet_df.loc[0, 'Transporte']
        comentarios = first_sheet_df.loc[0, 'Comentarios']
        logistica = first_sheet_df.loc[0, 'Logistica']
        avisos = first_sheet_df.loc[0, 'Avisos']
        # Retain renta_bicis logic
        renta_bicis = first_sheet_df.loc[0, 'Renta de bicis'] if 'Renta de bicis' in first_sheet_df.columns else None
        del first_sheet_df
    except Exception as e:
        print("Event was not created. There's a problem with the sheet ITINERARIO:", str(e))
        return (None, None)
    
    # Process the VIAJEROS sheet for client status information
    try:
        second_sheet_df['STATUS'] = second_sheet_df['STATUS'].fillna('VIAJAN ✅')
        second_sheet_df['PUNTO DE VENTA'] = second_sheet_df['PUNTO DE VENTA'].fillna('OTRO')
        # Map statuses
        second_sheet_df['STATUS'] = second_sheet_df['STATUS'].replace({
            'RESERVADO✅': 'VIAJAN ✅',
            'REBOOKED⚠️': 'NO VIAJAN 🚫',
            'CANCELADO🚫': 'NO VIAJAN 🚫'
        })
        second_sheet_df = second_sheet_df.sort_values(by='STATUS')
        clientes_by_status = second_sheet_df.groupby(['STATUS', 'PUNTO DE VENTA'])['NOMBRE'].apply(list)
        clientes_by_status = clientes_by_status.sort_index(level='STATUS', ascending=False)
        del second_sheet_df
        clientes = get_clientes(clientes_by_status)
    except Exception as e:
        print("Event was not created. There's a problem with the sheet VIAJEROS:", str(e))
        return (None, None)
    
    # Build the description text using client data and event details
    description = ' '
    for cliente_text in clientes:
        description += cliente_text
    
    # Determine event color and append extra comments
    if renta_bicis is None:
        if 'street art' in file_name.lower() and guia != '❌' and guia != '' and logistica == '✅':
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
        if 'street art' in file_name.lower() and guia != '❌' and guia != '' and logistica == '✅' and renta_bicis == '✅':
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

    # Get file metadata in one call
    try:
        file_metadata = drive_service.files().get(
            fileId=file_id, fields='mimeType, webViewLink'
        ).execute()
    except Exception as e:
        print("Error fetching file metadata:", str(e))
        return (None, None)
    
    # Construct the calendar event object
    event = {
        'summary': summary,
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
                'fileUrl': file_metadata.get('webViewLink'),
                'mimeType': file_metadata.get('mimeType'),
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

def create_photos_folder(drive_service,file_name):
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
        folder_metadata = {
            'name': file_name, #Mi carpeta de fotos se llama igual que el archivo de Sheets
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

def attach_folder_to_calendar(calendar_service, calendar_id,folder_link,file_name):
    if folder_link:
        try:
            event = calendar_service.events().get(calendarId='primary', eventId=calendar_id).execute()
            event['attachments'].append(
                { 
                    'fileUrl': folder_link,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'title': f'Folder de fotos {file_name}'
                }
            )
            updated_event = calendar_service.events().update(calendarId='primary', eventId=calendar_id, body=event).execute()
        
            if updated_event:
                return True
        except errors.HttpError as e:
            print('Photos folder not attached to calendar event ',str(e))
    print('Photos folder not attached to calendar event, folder link not provided')
    return   



        
def update_calendar_and_folder(drive_service,sheets_service,calendar_service, file_id, calendar_id, folder_id, file_name, tab_name, folder_link):    
    from pandas import DataFrame, concat, Series

    try:
        # first worksheet extraction from file
        first_sheet_title = tab_name
        range_first_sheet = first_sheet_title + str(getenv('ITINERARIO_RANGE'))
        result_first_sheet = sheets_service.spreadsheets().values().get(spreadsheetId=file_id, range=range_first_sheet).execute()
        values_first_sheet = result_first_sheet.get('values', [])
        del result_first_sheet
        
        # Check if the value in cell A1 is equal to 'Guia principal' to distinguish if using old or new version of hojas logisticas
        if not values_first_sheet or not values_first_sheet[0] or values_first_sheet[0][0] != 'Guia principal':
            # first worksheet extraction from file
            first_sheet_title = tab_name
            range_first_sheet = first_sheet_title + str(getenv('NEW_ITINERARIO_RANGE'))
            result_first_sheet = sheets_service.spreadsheets().values().get(spreadsheetId=file_id, range=range_first_sheet).execute()
            values_first_sheet = result_first_sheet.get('values', [])
            del result_first_sheet
        
        values_first_sheet = make_square_matrix(values_first_sheet)
        
    except Exception as e:
        print("Event was not updated. File doesn't have sheets 'ITINERARIO'",str(e))
        return
    try:
        # second worksheet extraction from file
        second_sheet_title = 'VIAJEROS'
        range_second_sheet = second_sheet_title + str(getenv('VIAJEROS_RANGE'))
        result_second_sheet = sheets_service.spreadsheets().values().get(spreadsheetId=file_id, range=range_second_sheet).execute()
        values_second_sheet = result_second_sheet.get('values', [])
        del result_second_sheet
        values_second_sheet = make_square_matrix(values_second_sheet)
    except Exception as e:
        print("Event was not updated. File doesn't have sheets 'VIAJEROS'",str(e))
        return
    
    if not values_first_sheet:
        print(f'Event was not updated. No data found in {first_sheet_title}.')
        return
    if not values_second_sheet:
        print(f'Event was not updated. No data found in {second_sheet_title}.')
        return
    #Get info from Sheet document
    first_sheet_df=DataFrame(values_first_sheet[1:],columns=values_first_sheet[0])
    first_sheet_df=first_sheet_df.applymap(lambda x:x.strip() if type(x)==str else x)
    
    second_sheet_df=DataFrame(values_second_sheet[1:],columns=values_second_sheet[0])
    second_sheet_df=second_sheet_df.applymap(lambda x:x.strip() if type(x)==str else x)
    
    # check if start date is in the future
    try:
        if not first_sheet_df.loc[0,'Hora de inicio']:       
            first_sheet_df.loc[0,'Hora de inicio']='06:00' #Si no tiene hora de inicio es esta por default
        date_time_start= first_sheet_df.loc[0,'Fecha de inicio']+'T'+first_sheet_df.loc[0,'Hora de inicio']+':00'
        if not first_sheet_df.loc[0,'Hora de fin']:
            first_sheet_df.loc[0,'Hora de fin']='18:00' 
        date_time_end=first_sheet_df.loc[0,'Fecha de fin']+'T'+first_sheet_df.loc[0,'Hora de fin']+':00'
    except Exception as e:
        print("Event was not updated. There's a problem with the start or end date:",str(e))
        return
    if seconds_since(date_time_end,False)>=SECONDS_THRESHOLD_UPDATE: #Event set in the past
        print("Start date is set in the past")
        return    
 
    # if event in the future, set variables for updating the calendar event 
    try: 
        summary= first_sheet_df.loc[0,'Tour']
        guia = first_sheet_df.loc[0, 'Guia principal']
        apoyo = list_to_str_commas(first_sheet_df.loc[:, 'Guia apoyo'])
        chofer= first_sheet_df.loc[0,'Chofer']
    
        staff=set(concat([Series(guia),first_sheet_df['Guia apoyo'],Series(chofer)]).reset_index(drop=True))  #get non-repeatead elements from the concatenated Series
        
        atendees=get_emails(sheets_service, staff)
        transporte=first_sheet_df.loc[0,'Transporte']
        comentarios=first_sheet_df.loc[0,'Comentarios']
        logistica = first_sheet_df.loc[0, 'Logistica']
        avisos=first_sheet_df.loc[0,'Avisos']
        renta_bicis = first_sheet_df.loc[0, 'Renta de bicis'] if 'Renta de bicis' in first_sheet_df.columns else None #Es importante que aquí renta_bicis tenga el valor de None y sólo lo pueda adquirir aquí
        del first_sheet_df
        
    except Exception as e:
        print("Event was not updated. There's a problem with the sheet ITINERARIO:",str(e))
        return
    
    try:
        second_sheet_df['STATUS'] = second_sheet_df['STATUS'].fillna('VIAJAN ✅')
        second_sheet_df['PUNTO DE VENTA'] = second_sheet_df['PUNTO DE VENTA'].fillna('OTRO')


        # Map REBOOKED and CANCELADO to NO VIAJAN
        second_sheet_df['STATUS'] = second_sheet_df['STATUS'].replace({'RESERVADO✅':'VIAJAN ✅','REBOOKED⚠️': 'NO VIAJAN 🚫', 'CANCELADO🚫': 'NO VIAJAN 🚫'})
        second_sheet_df = second_sheet_df.sort_values(by='STATUS')
        clientes_by_status = second_sheet_df.groupby(['STATUS','PUNTO DE VENTA'])['NOMBRE'].apply(list)
        clientes_by_status = clientes_by_status.sort_index(level='STATUS',ascending=False)
        del second_sheet_df
        clientes = get_clientes(clientes_by_status)

    except Exception as e:
        print("Event was not updated. There's a problem with the sheet VIAJEROS:",str(e))
        return
    
    #Get the big text for the description of the event, first the clients
    description=''
    for i in clientes:
        description+=i
        
    if renta_bicis is None:
        if 'street art' in file_name.lower() and guia!='❌' and guia!='' and logistica=='✅':
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
        if 'street art' in file_name.lower() and guia!='❌' and guia!='' and logistica=='✅' and renta_bicis=='✅':
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
        summary='CANCELADO '+summary
    
    #Update event 
    file_metadata = drive_service.files().get(fileId=file_id, fields='mimeType, webViewLink').execute()
    event = {
        'summary': summary,
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
                'fileUrl': file_metadata.get('webViewLink'),
                'mimeType': file_metadata.get('mimeType'),
                'title': file_name
            },
            { 
                'fileUrl': folder_link,
                'mimeType': 'application/vnd.google-apps.folder',
                'title': f'Folder de fotos {file_name}'
                }
        ]
    else:
        event['attachments']=[
            {
                'fileUrl': file_metadata.get('webViewLink'),
                'mimeType': file_metadata.get('mimeType'),
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
        drive_service.files().update(fileId=folder_id, body={'name': file_name}).execute()
        print(f'Photos folder name updated succesfully: {file_name}')
    except errors.HttpError as e:
        print('Name not updated in photos folder, an error has occured:',str(e))
        
    return

def get_emails(sheets_service, staff):
    mails=[]
    sheet_name = 'mails'
    data_range = sheet_name+str(getenv('EMAILS_RANGE'))
     # Read the data from the sheet
    try:
        result = sheets_service.spreadsheets().values().get(spreadsheetId=LOG_FILE_ID,range=data_range).execute()
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
                        else:
                            print(f"{person} found in list of mails but email field is empty")
                        break
                if found==False:
                    print(f"{person} not found in list of emails")
    except errors.HttpError as e:
        print('An error has occured getting the emails of the staff:',str(e))
    return(mails)

def delete_calendar_and_folder(drive_service, calendar_service,calendar_id, folder_id):
    try:
        calendar_service.events().delete(calendarId='primary', eventId=calendar_id).execute()
        print(f"Deleted event with ID: {calendar_id}")
    except:
        print(f'error trying to delete calendar event {calendar_id}')
    try:
        drive_service.files().delete(fileId=folder_id).execute()
        print(f"Deleted photos folder with ID: {folder_id}")
    except:
        print(f'error trying to delete photos folder ID: {folder_id}')

def make_square_matrix(matrix):
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