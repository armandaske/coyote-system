def create_calendar(drive_service, sheets_service, calendar_service, file_id, file_name, tab_name):
    try:
        # Batch get both sheets in one request
        ranges = [f'{tab_name}{getenv("ITINERARIO_RANGE")}', f'VIAJEROS{getenv("VIAJEROS_RANGE")}',
                  f'{tab_name}{getenv("NEW_ITINERARIO_RANGE")}' ]
        result = sheets_service.spreadsheets().values().batchGet(spreadsheetId=file_id, ranges=ranges).execute()
        values_first_sheet = result['valueRanges'][0].get('values', [])
        values_second_sheet = result['valueRanges'][1].get('values', [])
        new_values_first_sheet = result['valueRanges'][2].get('values', [])
        
        # Determine which first sheet format to use
        if not values_first_sheet or not values_first_sheet[0] or values_first_sheet[0][0] != 'Guia principal':
            values_first_sheet = new_values_first_sheet

        # Ensure matrices are properly shaped
        values_first_sheet = make_square_matrix(values_first_sheet)
        values_second_sheet = make_square_matrix(values_second_sheet)

        if not values_first_sheet or not values_second_sheet:
            raise ValueError(f'Missing required data in sheets: {tab_name} or VIAJEROS')

    except Exception as e:
        print(f"Error retrieving sheet data: {e}")
        return None, None
    
    try:
        first_df = pd.DataFrame(values_first_sheet[1:], columns=values_first_sheet[0]).applymap(lambda x: x.strip() if isinstance(x, str) else x)
        second_df = pd.DataFrame(values_second_sheet[1:], columns=values_second_sheet[0]).applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        # Extract necessary fields
        first_df.fillna({'Hora de inicio': '06:00', 'Hora de fin': '18:00'}, inplace=True)
        date_time_start = f"{first_df.loc[0, 'Fecha de inicio']}T{first_df.loc[0, 'Hora de inicio']}:00"
        date_time_end = f"{first_df.loc[0, 'Fecha de fin']}T{first_df.loc[0, 'Hora de fin']}:00"
        
        if seconds_since(date_time_start, False) >= 0:
            print("Start date is set in the past")
            return None, None
        
        # Extract staff and attendees
        staff = set(pd.concat([pd.Series([first_df.loc[0, 'Guia principal']]), first_df['Guia apoyo'], pd.Series([first_df.loc[0, 'Chofer']])]).dropna())
        attendees = get_emails(sheets_service, staff)
        
        # Process second sheet
        second_df.fillna({'STATUS': 'VIAJAN ✅', 'PUNTO DE VENTA': 'OTRO'}, inplace=True)
        second_df['STATUS'].replace({'RESERVADO✅': 'VIAJAN ✅', 'REBOOKED⚠️': 'NO VIAJAN 🚫', 'CANCELADO🚫': 'NO VIAJAN 🚫'}, inplace=True)
        second_df.sort_values('STATUS', inplace=True)
        clientes_by_status = second_df.groupby(['STATUS', 'PUNTO DE VENTA'])['NOMBRE'].apply(list)
        clientes = get_clientes(clientes_by_status)
        
    except Exception as e:
        print(f"Error processing sheet data: {e}")
        return None, None
    
    try:
        file_metadata = drive_service.files().get(fileId=file_id, fields='mimeType, webViewLink').execute()
        
        description = '\n'.join(clientes) + f"\nComentarios:\n{first_df.loc[0, 'Comentarios']}\n\nGuía: {first_df.loc[0, 'Guia principal']}\nGuía de apoyo: {list_to_str_commas(first_df['Guia apoyo'])}\nChofer: {first_df.loc[0, 'Chofer']}\nTransporte: {first_df.loc[0, 'Transporte']}\nLogística: {first_df.loc[0, 'Logistica']}\nAvisos: {first_df.loc[0, 'Avisos']}"
        
        event = {
            'summary': first_df.loc[0, 'Tour'],
            'location': 'C. Macedonio Alcalá 802, RUTA INDEPENDENCIA, Centro, 68000 Oaxaca de Juárez, Oax.',
            'description': description,
            'start': {'dateTime': date_time_start, 'timeZone': 'America/Mexico_City'},
            'end': {'dateTime': date_time_end, 'timeZone': 'America/Mexico_City'},
            'attendees': [{'email': attendee} for attendee in attendees if attendee],
            'reminders': {'useDefault': True},
            'attachments': [{'fileUrl': file_metadata['webViewLink'], 'mimeType': file_metadata['mimeType'], 'title': file_name}]
        }
        
        created_event = calendar_service.events().insert(calendarId='primary', sendUpdates='all', body=event, supportsAttachments=True).execute()
        print(f'Event created successfully! ID: {created_event["id"]}')
        return created_event['id'], created_event['htmlLink']
    
    except errors.HttpError as e:
        print(f"Error creating calendar event: {e}")
        return None, None
