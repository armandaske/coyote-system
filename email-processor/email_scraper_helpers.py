from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import re
from os import getenv
from googleapiclient import errors
from json import dumps
#TODO: cuando se hayan homologado todas las hojas logisticas quitar la opcion de pickup en write viajeros. 

OTRO_FILE_ID = str(getenv('OTRO_FILE_ID'))

unificar_nombres_tours_dict={'La mejor caminata hasta Hierve el Agua y mezcal':'Ultimate Hike Hierve el Agua + Mezcal',
                    'Caminata Ultimate Hierve el Agua + Mezcal':'Ultimate Hike Hierve el Agua + Mezcal',
                    'Ruta ciclista de arte urbano' : 'Street Art Bike Ride',
                    'Paseo en Bicicleta de Arte Urbano' : 'Street Art Bike Ride',
                    'Hike en Hierve y Rincones Secretos + Mezcal' : 'Ultimate Hike Hierve el Agua + Mezcal',
                    'Hike en Hierve y Rincones Secretos + Tapetes Tradicionales' : 'Ultimate Hike Hierve el Agua + Teotitlán',
                    'Caminata de Arte Urbano' : 'Street Art Walk',
                    'Ultimate Hierve el Agua Hike & Teotitlan Textiles': 'Ultimate Hike Hierve el Agua + Teotitlán',
                    'Ultimate Hike Hierve el Agua & Mezcal': 'Ultimate Hike Hierve el Agua + Mezcal'
                    
                   }

def make_square_matrix(matrix):
    if len(matrix)>0:
        max_length = max(len(row) for row in matrix)
        for row in matrix:
            while len(row) < max_length:
                row.append(None)
    return matrix

def get_current_time():
    # Define the timezone offset for Mexico City (UTC-6 or UTC-5 depending on daylight saving time)
    mexico_city_offset = timezone(timedelta(hours=-6))
    
    # Get the current time in UTC
    current_time_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
    
    # Convert the current time to Mexico City timezone
    current_time_mexico = current_time_utc.astimezone(mexico_city_offset)
    
    # Format the time as a string
    current_time_str = current_time_mexico.strftime("%Y-%m-%d -> %H:%M:%S")
    
    return current_time_str


def get_time_week_before():
    current_time = datetime.now()
    one_week_ago = current_time - timedelta(days=20)
    after_date = one_week_ago.strftime("%Y/%m/%d")
    return after_date


def convert_currency_to_float(currency_str):
    if re.search(r"\$([\d,]+\.?\d{0,2})", currency_str):
        return float(re.search(r"\$([\d,]+\.?\d{0,2})", currency_str).group(1).replace(',', ''))
    else:
        return ''
    
    
def reformat_date(date_str):
    # Define the regex pattern to match the date components
    pattern = r'(\d+)/(\d+)/(\d+)'

    # Use regex to find the date components
    match = re.search(pattern, date_str)
    if match:
        # Extract the month, day, and year
        month, day, year = match.groups()

        # Swap the month and day
        swapped_date = f"{year}/{month}/{day}"

        # Replace the original date with the swapped date in the input string
        swapped_date_str = re.sub(pattern, swapped_date, date_str)
        return swapped_date_str
    else:
        return date_str
    
    
def convert_date_format(input_text, target_offset_hours):
    try:    
        # Parse the input date string
        input_format = "%a, %d %b %Y %H:%M:%S %z"
        parsed_date = datetime.strptime(input_text, input_format)
        
        # Apply the time zone offset
        shifted_date = parsed_date - timedelta(hours=target_offset_hours)
        
        # Format the date in the desired output format
        output_format = "%Y/%m/%d @ %I:%M%p"  # Updated format for 12-hour clock with am/pm
        formatted_date = shifted_date.strftime(output_format)
        
        return formatted_date  
    
    except Exception as e:
        print(f"An error occurred converting the date from airbnb message: {e}")
        return ''

def get_soup(part, charset):
    part = part.get_payload(decode=True)
    part = part.decode(charset)
    soup = BeautifulSoup(part, 'html.parser')
    return(soup)

def update_numeration(sheets_service,file_id):
    range_to_check='VIAJEROS'+"!B2:B"
    result = sheets_service.spreadsheets().values().get(spreadsheetId=file_id,
                                range=range_to_check).execute()
    values = result.get('values', [])
    last_entry_row = len(values)

    # Create new numeration values
    numeration = [[i+1] for i in range(last_entry_row)]
    # Update column A with new numeration
    body = {
        'values': numeration
    }
    range_to_update='VIAJEROS'+"!A2:A"
    result = sheets_service.spreadsheets().values().update(spreadsheetId=file_id, range=range_to_update,
                                   valueInputOption="RAW", body=body).execute()
    return

def write_viajeros(sheets_service, list_of_viajero_dicts, file_id):
    sheet_name = 'VIAJEROS'  # replace with your sheet name
    for viajero in list_of_viajero_dicts:
        name=viajero['name']
        country=viajero['country']
        age=viajero['age']
        phone=viajero['phone']
        email=viajero['email']
        comments=viajero['comments']
        sales_channel=viajero['sales_channel']
        reservacion=viajero['confirmation_code']
        sold_by=viajero['sold_by']
        status=viajero['status']
        payment=viajero['payment']
        reservation_date=viajero['reservation_date']

        # Get the last index from column A
        range_to_read = sheet_name + '!A:A'
        result = sheets_service.spreadsheets().values().get(spreadsheetId=file_id, range=range_to_read).execute()
        values = result.get('values', [])
        last_index = int(values[-1][0]) if values and values[-1][0].isnumeric() else 0  # Get the last value and convert to integer

        # Increment the index for the new entry
        new_index = last_index + 1
        next_row=new_index + 1 #porque en sheets se indexa desde 1 y no 0
        # How the input data should be interpreted.
        value_input_option = 'USER_ENTERED'  # or 'RAW'
        
        #Get the names of columns in the viajeros sheet
        range_to_read_cols = sheet_name + '!1:1'
        result_cols = sheets_service.spreadsheets().values().get(spreadsheetId=file_id, range=range_to_read_cols).execute()
        name_cols = result_cols.get('values', [])
        
        values_to_append =[] if not name_cols else [new_index, name, country, age, phone, email, comments, sales_channel, sold_by,'',reservacion, status, payment, reservation_date] if 'PICK UP (ESPECIAL)' in name_cols[0] else [new_index, name, country, age, phone, email, comments, sales_channel, sold_by, reservacion, status, payment, reservation_date]  # Include the index in the values
        value_range_body = {
            "majorDimension": "ROWS",  # "ROWS"|"COLUMNS"
            "values": [
                values_to_append
            ]
        }     
        range_to_append = f"{sheet_name}!A{next_row}"
        request = sheets_service.spreadsheets().values().append(spreadsheetId=file_id, 
                                                                range=range_to_append, 
                                                                valueInputOption=value_input_option, 
                                                                body=value_range_body).execute()
        if request:
            print('Client added to VIAJEROS')
        else:
            print('Client was NOT added to the list of VIAJEROS')

def write_itinerario(sheets_service, itinerario_dict, file_id):
    sheet_name = 'ITINERARIO'  # replace with your sheet name
    tour = itinerario_dict['experience_name']
    start_date = itinerario_dict['start_date']
    start_hour = itinerario_dict['start_hour']
    end_date = itinerario_dict['end_date']
    end_hour = itinerario_dict['end_hour']

    # Extracting values into a list
    values_to_write = [tour, start_date, start_hour,end_date,end_hour]

    # How the input data should be interpreted.
    value_input_option = 'USER_ENTERED'  # or 'RAW'

    value_range_body = {
        "majorDimension": "ROWS",
        "values": [values_to_write]  # Including the list of values in a list to make it a row
    }

    # Specify the range where you want to write the values depending of the hoja logistica
    range_to_read = sheet_name + '!1:1'
    result = sheets_service.spreadsheets().values().get(spreadsheetId=file_id, range=range_to_read).execute()
    values = result.get('values', [])
    range_to_update = sheet_name+'!D5:H5' if not values or not values[0] or values[0][0]!= 'Guia principal' else sheet_name+'!D2:H2'

    request = sheets_service.spreadsheets().values().update(spreadsheetId=file_id,
                                                            range=range_to_update,
                                                            valueInputOption=value_input_option,
                                                            body=value_range_body)
    request.execute()

    print(f'Itinerario info for {tour} on {start_date} has been updated')

def find_last_subfolder_id(drive_service, directory_tree):
    parent_folder_id = 'root'  # Start from the root folder ID
    for folder_name in directory_tree:
        try: 
            current_folder_id=find_folder(drive_service,folder_name, parent_folder_id)
            if not current_folder_id:
                return (False, parent_folder_id, folder_name)  # Folder not found, return last parent
            parent_folder_id=current_folder_id

        except Exception as e:
            print(f"An error occurred: {e}")
            return (False, None, folder_name)

    return (True, current_folder_id, folder_name)
    
def find_folder(drive_service, folder_name, parent_folder_id):
    query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    pattern = re.compile(folder_name, re.IGNORECASE)

    if len(files) > 0:
        for file in files:
            if pattern.search(file['name']):
                return (file['id'])
        print(f"No folders with the name {folder_name} found in the parent folder {parent_folder_id}")
        return
    else:
        print(f"The parent folder {parent_folder_id} does not have any folders inside")
        return
    
def create_folder_tree(drive_service,folder_names, parent_folder_id):
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
    
def find_existing_file(drive_service, file_name, folder_id, second_search=False):  #CHECAR QUÉ PEDO CON ESA SEGUNDA BUSQUEDA Y COMO HACER QUE NO TOME EN CUENTA SI EMPIEZA COMO PRIVADO. CREO QUE YA LO RESOLVI ABAJO SUPONIENDO QUE NINGUN TOUR LLEVA LA PALABRA PRIVADO EN SU NOMBRE  
    if '@None' not in file_name:
        at_none_in_name = False
        if not second_search:
            query = f'name="{file_name}" and "{folder_id}" in parents and mimeType="application/vnd.google-apps.spreadsheet" and trashed = false'
        else:
            query = f'name contains "{file_name}" and "{folder_id}" in parents and mimeType="application/vnd.google-apps.spreadsheet" and trashed = false'
    else:
        at_none_in_name = True
        search_words=file_name.split(' @None ')
        query = f"name contains '{search_words[0]}' and name contains '{search_words[-1]}' and '{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed = false"
    # Execute the query
    results = drive_service.files().list(q=query, fields="files(id, name, createdTime)", orderBy="createdTime desc").execute()
    files = results.get('files', [])
    # Check if any files were found
    if len(files) == 0:
        print(f"No Google Sheets file found with name: {file_name}")
        if not second_search and not at_none_in_name:
            print("Trying to find files that contain the file name") #To search for files that might have the format "CANCELADO 09-03-05..." or "106 09-03-05..."
            return find_existing_file(drive_service, file_name, folder_id, True)
        return    
    
    if second_search:
        final_list = [elem.get('id', '') for elem in files if (file_name.lower() in elem.get('name', '').lower() and 'privado' not in elem.get('name', '').lower()) or file_name.lower()==elem.get('name', '').lower()]
    elif at_none_in_name:
        final_list = [elem.get('id', '') for elem in files if 'privado' not in elem.get('name', '').lower()]
    else:        
        final_list= [elem.get('id','') for elem in files if file_name in elem.get('name','')]
    if len(final_list)==0:
        print(f"No Google Sheets file found with name: {file_name}")
        return
    elif len(final_list) > 1:
        print(f"Warning: Multiple files found with the same name: {file_name}")
    # Return a list with the ids of all files found
    return final_list

def get_year_from_date(date):
    try:
        year_digits=date.split('-')[0]
        return year_digits
    except Exception as error:
        print('Error extracting the year from the date: '+str(error))
        return

def get_month_from_date(date):
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
        month_digits=date.split('-')[1]
        return(month_mapping[month_digits])
    except Exception as error:
        print('Error extracting the month from the date: '+str(error))
        return

def dicts_from_extracted_data(extracted_data):  
    list_of_viajero_dicts=[]
    for i in range(extracted_data.get('number_of_guests')):
        if len(extracted_data.get('names_of_guests'))>i:
            name=extracted_data.get('names_of_guests')[i]
        else:
            name=f"Acompañante de {extracted_data.get('names_of_guests')[0]}"
        ages=extracted_data.get('ages',[])
        age=ages[i] if len(ages)>i else ''  
        payments=extracted_data.get('payments',[])
        payment=payments[i] if len(payments)>i else ''
        comments=extracted_data.get('comments',[])
        comment=comments[i] if len(comments)>i else ''
        
        viajero_dict={
        "name": name,
        'age': age,
        "country": extracted_data.get('country',''),
        'phone': "'"+extracted_data.get('phone',''),
        'email':extracted_data.get('email',''),
        'comments': comment,
        "sales_channel": extracted_data.get('sales_channel',''),
        "sold_by": extracted_data.get('sold_by',''),
        "confirmation_code": extracted_data.get('confirmation_code',''),
        "status": "RESERVADO✅",
        "payment": payment,
        "reservation_date": extracted_data.get('reservation_date','')
        }
        list_of_viajero_dicts.append(viajero_dict)
    itinerario_dict={
        "experience_name": extracted_data['experience_name'], 
        "start_date": extracted_data["start_date"],
        "start_hour": extracted_data["start_hour"],
        "end_date": extracted_data["end_date"],
        "end_hour": extracted_data["end_hour"]
    }
    return (itinerario_dict,list_of_viajero_dicts)


def find_and_cancel(service, file_id, guest_name, number_of_guests, sales_channel, reservation_code, status_value):
    # Define a range to fetch a large set of data (you can modify to match your data's size)
    range_name = "VIAJEROS!A1:L50"
    result = service.spreadsheets().values().get(spreadsheetId=file_id, range=range_name).execute()
    values = result.get('values', [])
    del result
    values=make_square_matrix(values)
    # Get the headers
    headers = values[0] if values else []
    # Identify columns based on headers
    try:
        name_col_index = headers.index("NOMBRE")
        sales_channel_col_index = headers.index("PUNTO DE VENTA")
        reservation_col_index = headers.index("RESERVACION")
        status_col_index = headers.index("STATUS")
    except ValueError:
        print("Error: Required columns not found!")
        return

    # Find rows where the guest name matches
    if sales_channel == 'Airbnb':
        matching_rows = [i for i, row in enumerate(values) if row and guest_name in row[name_col_index] and row[sales_channel_col_index] == sales_channel]
        number_of_guests = len(matching_rows) #because for airbnb the email doesn't specify the number of guests
    else: #if it's not Airbnb, it's Fareharbor, so i look for the reservation code
        matching_rows = [i for i, row in enumerate(values) if row and row[reservation_col_index] == reservation_code and row[sales_channel_col_index] == sales_channel]
    # If there's only one match, initiate the cancellation process
    if len(matching_rows) == 1:
        row_to_cancel= "VIAJEROS!"+number_to_letter(status_col_index+1)+str(matching_rows[0]+1) #Sé que hago muchas mierdas aquí pero ya lo quiero acabar
        cancel_process(service, file_id, row_to_cancel,status_value)
        print(f"{status_value} successful!")
        return True

    # If there are multiple name matches.
    elif len(matching_rows) > 1:
        if len(matching_rows)>=number_of_guests: #its Fareharbor and I know there are no more guests to cancel than guests found in document
            for i in range(number_of_guests):
                row_to_cancel= "VIAJEROS!"+number_to_letter(status_col_index+1)+str(matching_rows[-(i+1)]+1)#Sé que hago muchas mierdas aquí pero ya lo quiero acabar.Borro de atrás para adelante 
                cancel_process(service, file_id, row_to_cancel,status_value)
            print(f"{i+1} {status_value} successfully made!")
            return True
        print(f"Error: Multiple matches found for {reservation_code} but there are more guests to cancel or rebook than guests found in the document")
        return
    
    else:
        return

def cancel_process(service, file_id, range_name, value):
    body = {
        'values': [[value]]
    }
    # Call the Sheets API to update the cell
    result = service.spreadsheets().values().update(
        spreadsheetId=file_id, range=range_name,
        valueInputOption="RAW", body=body).execute()
    return result

def number_to_letter(n):
    if 1 <= n <= 26:
        return chr(64 + n)
    else:
        raise ValueError("The input number should be between 1 and 26 inclusive to select the columns in the Sheets document.")

def copy_and_rename_sheet(drive_service,original_sheet_id, new_name, folder_id):    
    copied_file = drive_service.files().copy(fileId=original_sheet_id, body={"parents": [folder_id]}).execute()  
    copied_file = drive_service.files().update(fileId=copied_file['id'], body={"name": new_name}).execute() 
    return copied_file

def add_label_to_email(gmail_service, user_id, msg_id,subject,label_ids,label_name):
    try:
        message = gmail_service.users().messages().modify(userId=user_id, id=msg_id, body={'addLabelIds': label_ids}).execute()
        print('labeled as '+label_name+' the email '+subject+' id:'+msg_id)
        return message
    except errors.HttpError as error:
        print(f'An error occurred: {error}')
        return
        
def get_label_id(gmail_service,label_name):
    try:
        # Fetch all the labels
        labels = gmail_service.users().labels().list(userId='me').execute().get('labels', [])
        for label in labels:
            if label['name'] == label_name:
                return label['id']
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def clean_filename(filename):
    forbidden_characters = '/\\*?<>"|'
    for char in forbidden_characters:
        filename = filename.replace(char, '')
    return filename

def booking_logic(drive_service,sheets_service,msg,platform):
    msg_date=msg.get('Date')
    for part in msg.iter_parts():
        charset=part.get_content_charset()
        if (part.get_content_type() == 'text/html'):
            soup=get_soup(part,charset)
            # Extract guest name, experience name, booking date
            try:
                if platform=='Airbnb':
                    extracted_data=abnb_extract_booking_info(soup)
                    extracted_data['reservation_date']=convert_date_format(msg_date, 6) #UTC-6
                elif platform=='Fareharbor':
                    extracted_data=fh_extract_booking_info(soup)
                start_date = extracted_data.get('start_date')
                start_hour = extracted_data.get('start_hour')
                experience_name = extracted_data.get('experience_name') 
                itinerario_dict,list_of_viajero_dicts=dicts_from_extracted_data(extracted_data)    
            except Exception as error:
                print("The specified element was not found in the HTML for booking logic",str(error))
                return
            break
    try:
        raw_file_name = '-'.join(reversed(start_date.split('-')))+f' @{start_hour} '+experience_name
        file_name = clean_filename(raw_file_name)
        year= get_year_from_date(start_date)
        month= get_month_from_date(start_date)
        folders=['Workflow Coyote Armando Technologies', year,'Hojas Logísticas', month]
        all_folders_found, folder_id, folder_name=find_last_subfolder_id(drive_service, folders)
        if all_folders_found:
            pass
        elif folder_id and folder_name:
            print("No se encontraron todos los subfolders, se intentará crearlos")
            split_element = folder_name
            if split_element in folders:
                split_index = folders.index(split_element)
                folders = folders[split_index :]    
                folder_id = create_folder_tree(drive_service,folders, folder_id)
        else:
            print(f"It was not possible to fetch the folder {folder_name}")
            return
    except Exception as error:
        print('Error trying to find existing folder: ',str(error))
        return    
    
    try:
        file_id=find_existing_file(drive_service ,file_name, folder_id)      
    except Exception as error:
        print('Error trying to find existing file: ',str(error))
        return       

    #There's no file, this is the first guest. Create new file and fill both the itinerario and viajeros sheet
    if not file_id:
        
        try:
            raw_file_name_ = 'BASE'+' '+experience_name
            file_name_ = clean_filename(raw_file_name_)
            folders_=['Workflow Coyote Armando Technologies', year,'Bases HL']
            all_folders_found_, folder_id_, folder_name_=find_last_subfolder_id(drive_service, folders_)
            if all_folders_found_:
                pass
            elif folder_id_ and folder_name_:
                print("No se encontraron todos los subfolders, se intentará crearlos")
                split_element_ = folder_name_
                if split_element_ in folders_:
                    split_index_ = folders_.index(split_element_)
                    folders_ = folders_[split_index_ :]    
                    folder_id_ = create_folder_tree(drive_service,folders_, folder_id_)
            else:
                print(f"It was not possible to fetch the folder {folder_name}")
                return
        except Exception as error:
            print('Error trying to find existing folder: ',str(error))
            return    
        
        try:
            original_id=find_existing_file(drive_service ,file_name_, folder_id_, second_search=True)      
        except Exception as error:
            print('Error trying to find existing file: ',str(error))
            return
        
        if not original_id:    
            original_id=OTRO_FILE_ID 
        else:
            original_id=original_id[0] #find_existing_file() returns either a list or None, so I take the first (only) value.
            
        print('Attempting to create a new file')
        file_metadata = copy_and_rename_sheet(drive_service,original_id, file_name, folder_id)
        file_id = file_metadata['id']
        print(f'New file created: {file_name} id:{file_id}')
        write_itinerario(sheets_service, itinerario_dict, file_id)    
        write_viajeros(sheets_service, list_of_viajero_dicts, file_id)
        print('Itinerario and Viajeros information added to new file')

    #The file already exists, just fill the new bookings of the viajeros sheet
    else:    
        print(f'Adding new reservations to file {file_name}')
        update_numeration(sheets_service, file_id[0])  #If there's more than 1 file with the same name i use the most recently created one
        write_viajeros(sheets_service, list_of_viajero_dicts, file_id[0])

    return True

def rebooking_logic(drive_service, sheets_service, msg, platform): 
    for part in msg.iter_parts():
        charset=part.get_content_charset()
        if (part.get_content_type() == 'text/html'):
            soup=get_soup(part,charset)
            # Extract guest name, experience name, booking date
            try:
                if platform=='Airbnb':
                    extracted_data=abnb_extract_rebooking_info(soup)
                elif platform=='Fareharbor':
                    extracted_data=fh_extract_rebooking_info(soup)
                guest_name=extracted_data.get('guest_name')
                number_of_guests=extracted_data.get('number_of_guests',1)
                sales_channel=extracted_data.get('sales_channel')
                old_experience_name=extracted_data.get('experience_name')
                old_start_date=extracted_data.get('start_date')
                old_start_hour=extracted_data.get('start_hour')
                old_confirmation_code=extracted_data.get('confirmation_code')
            except Exception as error:
                print("The specified element was not found in the HTML for rebooking logic",str(error))
                return
            break

    try:
        raw_old_file_name = '-'.join(reversed(old_start_date.split('-')))+f' @{old_start_hour} '+old_experience_name
        old_file_name = clean_filename(raw_old_file_name)
        old_year= get_year_from_date(old_start_date)
        old_month= get_month_from_date(old_start_date)
        folders=["Workflow Coyote Armando Technologies", old_year,'Hojas Logísticas', old_month]
        all_folders_found, old_folder_id, old_folder_name=find_last_subfolder_id(drive_service, folders)
        if all_folders_found:
            old_file_id=find_existing_file( drive_service,old_file_name, old_folder_id)           
        else:
            print("Folder Hojas Logisticas was not found. Cannot make the rebooking")
            return
        
    except Exception as error:
        print('Error trying to find existing file: ',str(error))
        return       
    if not old_file_id:
        print(f'The file {old_file_name} was not found. Cannot make the rebooking')
        return

    else:    
        for id_ in old_file_id:
            rebooked=find_and_cancel(sheets_service, id_, guest_name, number_of_guests, sales_channel, old_confirmation_code,"REBOOKED⚠️")
            if rebooked:
                break

    if not rebooked:
        if sales_channel == 'Airbnb':
            print(f"Error: {guest_name} not found!")
        elif old_confirmation_code:
            print(f"Error: {old_confirmation_code} not found!")
        return
    
    if platform=='Airbnb':
        print(f"{guest_name} marked as Rebooked in {old_file_name}, will try to make reservation for new date")
    else:
        print(f"{old_confirmation_code} marked as Rebooked in {old_file_name}, will try to make reservation for new date")
    if booking_logic(drive_service,sheets_service,msg,platform):
        return True
    return

def cancellation_logic(drive_service,sheets_service,msg,platform):
    for part in msg.iter_parts():
        charset=part.get_content_charset()
        if (part.get_content_type() == 'text/html'):
            soup=get_soup(part,charset)
            # Extract guest name, experience name, booking 
            try:
                if platform=='Airbnb':
                    extracted_data=abnb_extract_cancellation_info(soup)
                elif platform=='Fareharbor':
                    extracted_data=fh_extract_cancellation_info(soup)
                guest_name=extracted_data.get('guest_name')
                number_of_guests=extracted_data.get('number_of_guests',1)
                experience_name=extracted_data.get('experience_name')
                start_date=extracted_data.get('start_date')
                start_hour=extracted_data.get('start_hour')
                confirmation_code=extracted_data.get('confirmation_code')
                sales_channel=extracted_data.get('sales_channel')
            except Exception as error:
                print("The specified element was not found in the HTML for cancellation logic",str(error))
                return
            break

    try:
        raw_file_name = '-'.join(reversed(start_date.split('-')))+f' @{start_hour} '+experience_name     
        file_name = clean_filename(raw_file_name)
        year= get_year_from_date(start_date)
        month= get_month_from_date(start_date)
        folders=['Workflow Coyote Armando Technologies', year,'Hojas Logísticas', month]
        all_folders_found, folder_id, folder_name=find_last_subfolder_id(drive_service, folders)
        if all_folders_found:
            file_id=find_existing_file(drive_service,file_name, folder_id)           
        else:
            print("No se encontró el folder de Hojas logísticas, no se pudo hacer la cancelación")
            return
    except Exception as error:
        print('Error trying to find existing file: ',str(error))
        return       
    if not file_id:
        print(f'The file {file_name} was not found. Cannot make the cancellation')
        return
                  
    else:  
        for id_ in file_id:
            cancelled=find_and_cancel(sheets_service, id_, guest_name, number_of_guests, sales_channel,confirmation_code,"CANCELADO🚫")
            if cancelled:
                break
    if not cancelled:
        if sales_channel == 'Airbnb':
            print(f"Error: {guest_name} not found!")
        elif confirmation_code:
            print(f"Error: {confirmation_code} not found!")
        return
        
    return True

def other_logic(drive_service, sheets_service, msg, platform):
    if platform=='Airbnb':
        print('ATENDER CORREO RECIBIDO DE AIRBNB')
    elif platform=='Fareharbor':
        print('ATENDER CORREO RECIBIDO DE FAREHARBOR')
    else:
        print('ATENDER CORREO RECIBIDO NO IDENTIFICADO')
    return True

def abnb_extract_booking_info(soup):
    # Find the string using a regex that captures both "reservaciones más" and "reservación"
    try:
        # Find the string using a regex that captures both "reservaciones más" and "reservación"
        experience_info = soup.find(string=re.compile(r".+ acaba de recibir \d+ (reservaciones|reservación)"))
        if experience_info:
            # Extract the experience_name before " acaba de recibir"
            experience_name = experience_info.split(" acaba de recibir")[0].strip()

            # Extract the number using a regex that matches one or more digits
            number_of_guests = int(re.search(r"(\d+) (reservaciones|reservación)", experience_info).group(1))
        else:
            experience_info = soup.find(string=re.compile(r".+ just got \d+ (more bookings|more booking|booking)"))
            # Extract the experience_name before " acaba de recibir"
            experience_name = experience_info.split(" just got")[0].strip()

            # Extract the number using a regex that matches one or more digits
            number_of_guests = int(re.search(r"(\d+) (more bookings|more booking|booking)", experience_info).group(1))
            
    except Exception as e:
        print("Error al extraer experience_info para reservación de airbnb:",str(e))
        return
    # Extract names_of_guests from the section titled "Huéspedes confirmados"
    guest_section_header = soup.find(string=re.compile(r"Huéspedes confirmados"))
    if guest_section_header is None:
        guest_section_header = soup.find(string=re.compile(r"Confirmed guests"))
        
    guest_section = guest_section_header.find_next("table")
    names_of_guests = [p.text.strip() for p in guest_section.find_all("p", string=re.compile(r"\w+ \w+"))]

    # Extract country using the initial name-country pattern (e.g., "Emily US")
    country=''
    country_section = soup.find("p", string=re.compile(r"Miembro de Airbnb"))
    if country_section is None:
        country_section= soup.find("p", string=re.compile(r"On Airbnb since"))
    if country_section:
        country_section=country_section.find_previous("p")
        country = country_section.text.strip()

    # Extract event_date

    event_date_text = soup.find(string=re.compile(r"(\d+ \w+.|\w+ \d+|\w+), (\d+:\d+\s?(?:AM|PM)?) – (\d+ \w+.|\w+ \d+|\w+), (\d+:\d+\s?(?:AM|PM)?)")).strip()

    start_date, start_hour, end_date, end_hour = abnb_extract_date_time(event_date_text)

    # Extract confirmation_code
    payments=''
    confirmation_code_text = soup.find(string=re.compile(r"Código de confirmación"))
    if confirmation_code_text:
        #Extract payments
        confirmation_code_text=confirmation_code_text.find_next("p").text.strip()
        pago_element = soup.find(text=re.compile(r'\bPago\b'))
    else:
        confirmation_code_text = soup.find(string=re.compile(r"Confirmation code"))
        if confirmation_code_text:
            confirmation_code_text=confirmation_code_text.find_next("p").text.strip()
        pago_element = soup.find(text=re.compile(r'\bPayment\b'))


    payments=[]
    if pago_element:
        # Extract the text following 'Pago'  
        text_after_pago = pago_element.find_next('p').text.strip()
        payment= convert_currency_to_float(text_after_pago)
        payments=[payment]*number_of_guests
    else:
        print("Error. Couldn't find 'Pago' in the airbnb email")
        

    results_dict={
        "sales_channel": 'Airbnb',
        "experience_name": experience_name,
        "number_of_guests": number_of_guests,
        "names_of_guests": names_of_guests,
        "country": country,
        "start_date": start_date,
        "start_hour": start_hour,
        "end_date": end_date,
        "end_hour": end_hour,
        "confirmation_code": confirmation_code_text,
        "payments": payments
    }
    results_dict = {key: ' '.join(value.split()) if isinstance(value, str) else value for key, value in results_dict.items()}
    results_dict["experience_name"] = unificar_nombres_tours_dict.get(results_dict["experience_name"],results_dict["experience_name"]) #I homologate the tour names here right when i extract it  
    return results_dict

def fh_extract_booking_info(soup):             
    # Find the element containing the string "Created by:" (without the trailing space)
    created_by_element = soup.find(string=re.compile("Created by:|Rebooked by:",re.IGNORECASE))
    created_at_element = soup.find(string=re.compile("Created at:|Rebooked at:",re.IGNORECASE))
    
    # Initialize created_by variable
    created_by = ''
    created_at= ''
    
    if created_by_element:
        # Using the parent tag to locate the next sibling containing the created_by text
        parent_tag = created_by_element.find_parent()
        
        # Extract the created_by from the text of the next sibling
        if parent_tag:
            next_sibling = parent_tag.find_next_sibling(string=True)
            if next_sibling:
                created_by = next_sibling.strip()
    if created_at_element:
        # Using the parent tag to locate the next sibling containing the created_at text
        parent_tag = created_at_element.find_parent()
        
        # Extract the created_at from the text of the next sibling
        if parent_tag:
            next_sibling = parent_tag.find_next_sibling(string=True)
            if next_sibling:
                created_at = next_sibling.strip()
                created_at=reformat_date(created_at)
    
    # Find the element containing the string "Booking #" (within an <h3> tag)
    confirmation_code_element = soup.find('h3', string=re.compile("Booking #"))
    # Initialize confirmation_code variable
    confirmation_code = ''
    
    if confirmation_code_element:
        # Extract the confirmation_code from the text within the found element
        confirmation_code_text = confirmation_code_element.string.strip()
        confirmation_code = re.search(r"#\d+", confirmation_code_text).group(0) if re.search(r"#\d+", confirmation_code_text) else ''
    # Find the table containing "Booking #"
    booking_table = soup.find('h3', string=re.compile("Booking #")).find_parent('table')
    date_string=''
    experience_string=''
    if booking_table:
        date_string_element = booking_table.find('td', style=re.compile(r"font-family\s*:\s*Roboto\s*,\s*Helvetica\s*,\s*Arial\s*,\s*sans-serif\s*;?"))
        date_string = date_string_element.text if date_string_element else ''
        #extract exprience_name
        experience_name_section = booking_table.find_all('div', style=re.compile(r'font-size\s*:\s*16px\s*;?'))
        if experience_name_section:
            experience_string = experience_name_section[0]
            padding_bottom_pattern = re.compile(r"padding-bottom\s*:\s*4px\s*;?")
            for element in experience_name_section:
                if padding_bottom_pattern.search(element['style']):
                    experience_string = element
                    break
        
    experience_name=experience_string.get_text(strip=True) if experience_string else 'Sin nombre'     
    date_raw = date_string.strip() if date_string else ''
    start_date, start_hour, end_date, end_hour = fh_extract_date_time(date_raw)
    
    # Extract phone and email
    phone_section = soup.find('b', string="Phone:")
    if phone_section:
        phone = phone_section.parent.get_text(strip=True).replace('Phone:', '').strip()
    else:
        phone=''
    email_section = soup.find('b', string="Email:")
    if email_section:
        email = email_section.parent.get_text(strip=True).replace('Email:', '').strip()
    else:
        email=''

    # Extract number_of_guests. Locate the <div> element containing the number of clients and the string "General Ticket" at the beginning of the mail
    number_of_guests_section = soup.find('div', string=re.compile("General Ticket|Private Group", re.IGNORECASE))
    # Extract the number from the text content
    if number_of_guests_section:
        number_text = number_of_guests_section.get_text().strip()
        number_of_guests = int(re.search(r"(\d{1,2})", number_text).group(1))
    else:
        number_of_guests = 1
        
        
    # Extract names_of_guests and ages from the correct table
    details_h2 = soup.find('h2', string="Details")
    correct_table = details_h2.find_next_sibling('table') if details_h2 else None
    names_of_guests = []
    ages = []
    payments=[]
    country = ''
    comments=[]
    if correct_table:
        #get amount paid
        payment_elements = correct_table.find_all('span', string=re.compile("General Ticket|Private Group", re.IGNORECASE)) 
        if not payment_elements:
            payment_elements = correct_table.find_all('b', string=re.compile(r"\bTotal\b", re.IGNORECASE)) 

        payments=[convert_currency_to_float(payment.find_next('td').get_text(strip=True)) for payment in payment_elements]
        #get names
        names_elements = correct_table.find_all('b', string="Full name:")
        names_of_guests = [elem.parent.get_text(strip=True).replace('Full name:', '').strip() for elem in names_elements]

        if number_of_guests<len(names_of_guests):
            number_of_guests=len(names_of_guests)
        #get ages
        ages_elements = correct_table.find_all('b', string="Age:")   
        ages = [elem.parent.get_text(strip=True).replace('Age:', '').strip() for elem in ages_elements]
        ages.extend([''] * (len(names_of_guests) - len(ages))) #in case there are  more 
        
        #get country
        country_element = correct_table.find('b',string=re.compile("Where do you visit us from?"))
        country = country_element.parent.get_text(strip=True).split(':')[1].strip() if country_element else '' 
        
        #Get all the other comments/observations and save them in a dictionary
        try:
            rows = correct_table.find_all('tr')
            
            # Temporary storage for current person's data
            current_person = {}
            
            # Compile the regex pattern to match 'General Ticket' or 'Private Group'
            pattern = re.compile("General Ticket|Private Group", re.IGNORECASE)
            # List of keys to drop
            keys_to_drop = ['Age', 'Where do you visit us from?', 'Ticket Price', 'Full name', 'Total', 'Total paid']
            
            # Loop through each row
            for row in rows:
                # Check if the row contains the 'General Ticket' or 'Private Group' entry
                if pattern.search(row.text):
                    # If there's already data for the current person, save it and reset
                    if current_person:
                        for key in keys_to_drop:
                            current_person.pop(key, None)
                        comments.append(current_person)
                        current_person = {}
                    # Get the ticket price (if needed)
                    tickets_raw=row.find_all('td')
                    ticket_price = tickets_raw[1].text.strip() if len(tickets_raw)>1 else ''
                    current_person['Ticket Price'] = ticket_price
                else:
                    # Extract the details
                    if row.find('b'):
                        key = row.find('b').text.replace(':', "").strip()
                        row_raw=row.find_all('div')
                        value = row_raw[0].text.replace(f"{key}:", "").strip() if row_raw else ''
                        
                        current_person[key] = value
            
            # Append the last person's data
            if current_person:
                for key in keys_to_drop:
                    current_person.pop(key, None)
                comments.append(current_person)
            comments.extend([''] * (len(names_of_guests) - len(comments))) #in case there are  more 
            
        except Exception as error:
            print('Could not get the comments/observations from the reservation '+str(error))
            return  
    if len(names_of_guests)==0:
        name_section = soup.find('b', string="Name:")
        if name_section:
             names_of_guests= [name_section.parent.get_text(strip=True).replace('Name:', '').strip()]
        else:
            names_of_guests = ['']
            
    comments_string = [dumps(comment) for comment in comments]
        
    results_dict={
        "sales_channel": 'Fareharbor',
        "experience_name": experience_name,
        "sold_by": created_by,
        "reservation_date": created_at,
        "country" : country,
        "confirmation_code": confirmation_code,
        "date_raw": date_raw,
        "start_date": start_date,
        "start_hour": start_hour,
        "end_date": end_date,
        "end_hour": end_hour,
        "number_of_guests": number_of_guests,
        "names_of_guests": names_of_guests, 
        "ages": ages,
        "payments": payments,
        "phone": phone,
        "email": email,
        "comments": comments_string
    }
    results_dict = {key: ' '.join(value.split()) if isinstance(value, str) else value for key, value in results_dict.items()}
    results_dict["experience_name"] = unificar_nombres_tours_dict.get(results_dict["experience_name"],results_dict["experience_name"]) #I homologate the tour names here right when i extract it 
    return results_dict

def fh_extract_rebooking_info(soup):
    booking_table = soup.find('th', string=re.compile("Old",re.IGNORECASE)).find_parent('table')
    if booking_table:
        rows = booking_table.find_all('tr')[1:]  # Exclude the header row
        data = {}
        for row in rows:
            cells = row.find_all('td')
            key = cells[0].get_text(strip=True)
            value = cells[1].get_text(strip=True)
            data[key] = value
        # Now, assign the variables
        confirmation_code = data.get('ID')
        customers = data.get('Customers')
        number_of_guests = int(re.search(r"(\d{1,2})", customers).group(1)) if customers else 1
        experience_name = data.get('Item')
        date = data.get('Date')
        time = data.get('Time')
        
        start_date_raw=date.split(' - ')[0]
        end_date_raw=date.split(' - ')[-1]
        start_hour_raw=time.split(' - ')[0]
        end_hour_raw=time.split(' - ')[-1]
        
        start_clause=start_date_raw+' @ '+start_hour_raw
        end_clause=end_date_raw+ ' @ '+end_hour_raw
        date_raw=start_clause+' - '+end_clause
        start_date, start_hour, end_date, end_hour = fh_extract_date_time(date_raw)
    else:
        print('Table with rebooking information not found')
        return

    results_dict={
        "sales_channel": 'Fareharbor',
        "experience_name": experience_name, 
        "confirmation_code": confirmation_code,
        "start_date": start_date,
        "start_hour": start_hour,
        "number_of_guests": number_of_guests,    
    }
    results_dict = {key: ' '.join(value.split()) if isinstance(value, str) else value for key, value in results_dict.items()}
    results_dict["experience_name"] = unificar_nombres_tours_dict.get(results_dict["experience_name"],results_dict["experience_name"]) #I homologate the tour names here right when i extract it  
    return results_dict

def abnb_extract_rebooking_info(soup):
    print("Rebooking para Airbnb, realizar modificaciones manualmente")
    return

def abnb_extract_date_time(s):
    current_time = datetime.now()

    # Updated pattern to handle both date formats
    pattern = r"(\d+ \w+.|\w+ \d+|\w+), (\d+:\d+\s?(?:AM|PM)?) – (\d+ \w+.|\w+ \d+|\w+), (\d+:\d+\s?(?:AM|PM)?)"
    match = re.search(pattern, s)
    # Extract individual components
    start_date, start_hour, end_date, end_hour = match.groups()
    # Convert month abbreviations to month numbers (similar to your existing logic)
    start_date = convert_to_standard_date(start_date, current_time)
    end_date = convert_to_standard_date(end_date, current_time)
    start_hour = convert_to_standard_hour(start_hour)
    end_hour = convert_to_standard_hour(end_hour)
    
    return start_date, start_hour, end_date, end_hour

def convert_to_standard_date(date_str, current_time):
    # Logic to convert date string to standard format
    # You may need to customize this based on the actual date formats you encounter
    # For example, you might need a mapping for month abbreviations to month numbers
    year=current_time.year
    month_mapping = {'jan': '01','ene': '01', 'feb': '02', 'mar': '03', 'apr': '04','abr': '04', 'may': '05', 'jun': '06', 'jul': '07', 'aug': '08','ago': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12','dic': '12'}
    
    # Sample logic assuming date_str is in the format "8 mar." or "Jan 31"
    day, month_abbr = re.match(r"(\d+|\w+) (\w+.|\d+)", date_str).groups()
    if not day.isdigit():
        day,month_abbr=month_abbr,day
        
    # Additional logic to convert month abbreviation to month number if needed
    month = month_mapping.get(month_abbr.lower().strip('.'))
    
    if current_time.month > int(month):
        year += 1
    return f"{year}-{month}-{day.zfill(2)}"

def convert_to_standard_hour(s):
    try:
        # Parse the start time string
        time_obj = datetime.strptime(s, "%I:%M %p")
        # Format the start and end times in 24-hour format
        hour_24hrs = time_obj.strftime("%H:%M")
        return(hour_24hrs)
    except Exception:
        return(s)

def fh_extract_date_time(date_string):
    clauses = date_string.split(' - ')
    start_date, start_hour = fh_extract_date_hour_from_clause(clauses[0])

    # If there's no end part, assume it's a date-only format and set end_hour to 19:00
    if len(clauses) == 1:
        end_date = start_date
        end_hour = '19:00'
    else:
        end_date, end_hour = fh_extract_date_hour_from_clause(clauses[1],default_date=start_date)
    return start_date, start_hour, end_date, end_hour

def fh_extract_date_hour_from_clause(clause, default_date=None):
    if not clause:
        return '', '', '', ''
    
    month_mapping = {
        "January": "01", "February": "02", "March": "03", "April": "04",
        "May": "05", "June": "06", "July": "07", "August": "08",
        "September": "09", "October": "10", "November": "11", "December": "12"
    }
    # Try matching the full pattern with date and hour
    date_time_pattern = r'(\w+), (\w+) (\d{1,2}),? (\d{4})(?: (?:at|@) (\d+:\d+)\s?([ap]m))?'
    match = re.search(date_time_pattern, clause)
    
    # If there is a match with the full pattern
    if match:
        day_name, month, day, year, hour, meridiem = match.groups()

        if hour is None and meridiem is None:
            if default_date:
                hour = '19:00'
            else:
                hour = '06:00'
        else:
            hour = datetime.strptime(hour + meridiem, '%I:%M%p').time().strftime('%H:%M')
        date = f"{year}-{month_mapping[month]}-{day.zfill(2)}"
    # If only the time is provided (end hour without date)
    elif default_date and re.search(r'^\d+:\d+\s?[ap]m$', clause.strip()):
        clause=clause.replace(' ','')
        hour = datetime.strptime(clause.strip(), '%I:%M%p').time().strftime('%H:%M')
        date=default_date
    else:
        return '', ''

    return date, hour 

def abnb_extract_cancellation_info(soup):    
    # Define a mapping from Spanish month abbreviations to month numbers
    month_mapping = {
        'ene': '01',
        'feb': '02',
        'mar': '03',
        'abr': '04',
        'may': '05',
        'jun': '06',
        'jul': '07',
        'ago': '08',
        'sep': '09',
        'oct': '10',
        'nov': '11',
        'dic': '12',
        'jan': '01',
        'feb': '02',
        'mar': '03',
        'apr': '04',
        'may': '05',
        'jun': '06',
        'jul': '07',
        'aug': '08',
        'sep': '09',
        'oct': '10',
        'nov': '11',
        'dec': '12'     
    }
    
    # Extracting guest_name: Text before 'no podrán asistir' or 'no podrá asistir'
    guest_name_text = soup.find(string=re.compile(r"no podrán asistir|no podrá asistir|can’t make it to|no puede asistir"))
    
    try:
        guest_name = guest_name_text.split()[0].strip()
    except Exception:
        print('no se encontró nombre en correo de cancelación de Airbnb')
        return

    # Extracting remaining text after 'experiencia'
    try:
        tour_info_text = soup.find(string=re.compile(r"experiencia"))
        if tour_info_text is None:
            tour_info_text = guest_name_text.split(" can’t make it to ")[1].strip()
            experience_name, tour_date_text = tour_info_text.rsplit(" on ", 1)
        else:
            tour_info_text = tour_info_text.split("experiencia ")[1].strip()
            experience_name, tour_date_text = tour_info_text.rsplit(" el ", 1)
    except Exception:
        print('no se encontró experiencia o fecha en correo de cancelación de Airbnb')
        return
    
    # Converting 'tour_date' to format YYYY-MM-DD
    try:
        day, month_abbr, year = tour_date_text.split(" de ")
    except ValueError:
        # Split the string using whitespace as the delimiter for something like 'Dec 30, 2023.'
        parts = tour_date_text.split()
        month_abbr = parts[0]
        # Extract the day and year
        day = parts[1].strip(",")  # Remove any leading comma
        year = parts[2]
    except Exception:
        print('no se pudo separar la fecha en correo de cancelación de Airbnb')
        return

    month = month_mapping[month_abbr.strip('.').lower()]
    year = year.strip().strip(".")
    start_date = f"{year}-{month}-{day.zfill(2)}"
    
    results_dict= {
        "sales_channel": 'Airbnb',
        "guest_name": guest_name,
        "experience_name": experience_name,
        "start_date": start_date
    }
    results_dict = {key: ' '.join(value.split()) if isinstance(value, str) else value for key, value in results_dict.items()}
    results_dict["experience_name"] = unificar_nombres_tours_dict.get(results_dict["experience_name"],results_dict["experience_name"]) #I homologate the tour names here right when i extract it  
    return results_dict

def fh_extract_cancellation_info(soup): 
    # Initialize confirmation_code variable
    confirmation_code = ''
    date_string=''
    experience_string=''
    number_of_guests_section = soup.find('div', string=re.compile("General Ticket|Private Group", re.IGNORECASE))
    # Extract the number from the text content
    if number_of_guests_section:
        number_text = number_of_guests_section.get_text().strip()
        number_of_guests = int(re.search(r"(\d{1,2})", number_text).group(1))
    else:
        number_of_guests = 1
    
    confirmation_code_element = soup.find('h3', string=re.compile("Booking #"))
    if confirmation_code_element:
        # Extract the confirmation_code from the text within the found element
        confirmation_code_text = confirmation_code_element.string.strip()
        confirmation_code = re.search(r"#\d+", confirmation_code_text).group(0) if re.search(r"#\d+", confirmation_code_text) else ''  
        # Find the table containing "Booking #"
        booking_table = confirmation_code_element.find_parent('table')
    
        if booking_table:
            date_string_element = booking_table.find('td', style=re.compile(r"font-family\s*:\s*Roboto\s*,\s*Helvetica\s*,\s*Arial\s*,\s*sans-serif\s*;?"))
            date_string = date_string_element.text if date_string_element else ''
            #extract exprience_name
            experience_name_section = booking_table.find_all('div', style=re.compile(r'font-size\s*:\s*16px\s*;?'))
            if experience_name_section:
                experience_string = experience_name_section[0]
                padding_bottom_pattern = re.compile(r"padding-bottom\s*:\s*4px\s*;?")
                for element in experience_name_section:
                    if padding_bottom_pattern.search(element['style']):
                        experience_string = element
                        break
        
    experience_name=experience_string.get_text(strip=True) if experience_string else 'Sin nombre'     
    date_raw = date_string.strip() if date_string else ''
    start_date, start_hour, end_date, end_hour = fh_extract_date_time(date_raw)
    
    # Extract names_of_guests and ages from the correct table
    details_h2 = soup.find('h2', string="Details")
    correct_table = details_h2.find_next_sibling('table') if details_h2 else ''
    names_of_guests = []
    if correct_table:
        names_elements = correct_table.find_all('b', string="Full name:")
        names_of_guests = [elem.parent.get_text(strip=True).replace('Full name:', '').strip() for elem in names_elements]
    if len(names_of_guests)==0:
        name_section = soup.find('b', string="Name:")
        if name_section:
            names_of_guests= [name_section.parent.get_text(strip=True).replace('Name:', '').strip()]
        else:
            names_of_guests = ['']          
                 
    results_dict={
        "sales_channel": 'Fareharbor',
        "experience_name": experience_name, 
        "confirmation_code": confirmation_code,
        "start_date": start_date,
        "start_hour": start_hour,
        "number_of_guests" : number_of_guests,
        "guest_name": names_of_guests[0],     
    }
    results_dict = {key: ' '.join(value.split()) if isinstance(value, str) else value for key, value in results_dict.items()}
    results_dict["experience_name"] = unificar_nombres_tours_dict.get(results_dict["experience_name"],results_dict["experience_name"]) #I homologate the tour names here right when i extract it  
    return results_dict