def create_calendar(drive_service, sheets_service, calendar_service, file_id, file_name, tab_name):
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
