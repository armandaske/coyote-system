def main_function(drive_service, sheets_service, calendar_service, firestore_db):
    from sheet_monitor_helpers import get_last_state, get_month_from_file_name, get_year_from_file_name, find_last_subfolder_id, delete_logs, delete_calendar_and_folders_batch,\
        update_calendar_and_folder, update_columns_logs, inspect_logs, create_logs, create_calendar, create_photos_folder, attach_folder_to_calendar, make_file_public, store_state,\
        get_tabs, get_data_hl
    import googleapiclient.errors as errors
    from datetime import datetime
    from os import getenv
    import gc

    ROOT_ID= str(getenv('ROOT_ID'))
    
    
    state= get_last_state(firestore_db)
    page_token = state.get('last_page_token')
    last_processed_change_time= state.get('last_processed_change_time')
    if not page_token:
        print('page token not found, creating a new one.')
        # Get start page token for changes
        response = drive_service.changes().getStartPageToken().execute()
        page_token = response.get('startPageToken')
    if page_token:
        # Get changes since the last page token
        try:
            response = drive_service.changes().list(pageToken=page_token,
                                                    spaces='drive').execute()
        except errors.HttpError as error:
            print(f'An error occurred: {error}\nWill try to get new page token') 
            response = drive_service.changes().getStartPageToken().execute()
            page_token = response.get('startPageToken')
            response = drive_service.changes().list(pageToken=page_token,
                                                    spaces='drive').execute()
        current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        print(f"Looking for changes in drive @: {current_time}")
        for change in response.get('changes',[]):
            if last_processed_change_time:
                if datetime.fromisoformat(change['time'][:-1])<= datetime.fromisoformat(last_processed_change_time[:-1]):
                    print('Skip changes that have already been processed')
                    continue
            if (change['changeType'] == 'file' and 'file' in change and change['file'].get('mimeType') == 'application/vnd.google-apps.spreadsheet'):
                file_type= change['file'].get('mimeType')
                # the change is in a sheets file
                file = change.get('file')
                file_name=file.get('name')
                file_month=get_month_from_file_name(file_name)
                file_year=get_year_from_file_name(file_name)
                
                #check if the file is in the right folder
                file_id=file.get('id')
                file_metadata = drive_service.files().get(fileId=file_id, fields='webViewLink, trashed, parents').execute()
                file_parents=file_metadata.get('parents',[])
                
                if not file_month or not file_year:
                    continue
                is_child = False
                directory_tree=['Workflow Coyote Armando Technologies', file_year,'Hojas Logísticas', file_month]       
                all_folders_found, folder_id, folder_name= find_last_subfolder_id(drive_service,directory_tree)
               
                if all_folders_found:
                    if not file_parents:
                        is_child = True
                    else:
                        is_child = folder_id in file_parents or ROOT_ID in file_parents      
                else:
                    print("No se encontró el folder de Hojas logísticas")


                if is_child:
                    #It is a sheet file inside the correct folder (hoja logística)
                    print(f"Change detected for file: {file_name}, ID: {file_id}")          
                    file_link = file_metadata.get('webViewLink','')

                    if file_metadata.get('trashed','') or ROOT_ID in file_parents or not file_parents: #When other user deletes the file, it goes to the trash bin
                        print('The file was deleted. Deleting calendar event, photos folder and logs')
                        calendar_ids,photos_folder_ids= delete_logs(sheets_service,file_id)
                        delete_calendar_and_folders_batch(drive_service, calendar_service,calendar_ids, photos_folder_ids)
                    
                    else:
                        #The file was modified or recently created
                        tabs_names,tabs_ids=get_tabs(sheets_service, file_id, 'ITINERARIO')
                        multiday='NO'
                        if len(tabs_ids)>1:
                            multiday='SI'
                        for i in range(len(tabs_ids)):
                            is_first_itinerario=True #Necesito saber si es el primer tab y poner pagos, gastos, etc
                            if i>0:
                                is_first_itinerario=False
                            all_data=get_data_hl(sheets_service, file_id, tabs_names[i],is_first_itinerario, multiday)
                            if not all_data:
                                continue #Nothing to do to this tab, go to next one
                            keys_to_keep=['guia', 'apoyo','chofer','tour_name','start_date','transporte','num_clientes','multiday','venta','gastos','combustible']
                            logs_data = [all_data[k] for k in keys_to_keep]
                            logs_data = [x if isinstance(x, (int, float, str)) else ('' if x is None else str(x)) for x in logs_data] #sanitize values for JSON

                            calendar_id, photos_folder_id, photos_folder_link=inspect_logs(sheets_service,file_id,tabs_ids[i],file_name,tabs_names[i],logs_data)#this also updates the name of the file and tab in the logs
                            #if calendar_id:
                            #    #ya se había hecho un calendar event y está en los logs esta tab de la hoja logísitica
                            #    print('Updating the calendar event and photos folder')
                            #    if not photos_folder_id:
                            #        photos_folder_id, photos_folder_link = create_photos_folder(drive_service,file_name,tabs_names[i])
                            #        #update photos folder id and link in the logs
                            #        update_columns_logs(sheets_service,file_id,tabs_ids[i],[photos_folder_id,photos_folder_link],['D','G'])
                            #    update_calendar_and_folder(drive_service,calendar_service, calendar_id, photos_folder_id, file_name, tabs_names[i], photos_folder_link,file_link,file_type, all_data)
                            #    
                            #else:
                            #    #Es una nueva hoja logística o un nuevo tab de itinerario
                            #    print('Creating a calendar event and photos folder for this experience')
                            #    calendar_id,calendar_link = create_calendar(calendar_service,file_link,file_type, file_name, all_data)
                            #    if calendar_id:
                            #        photos_folder_id, photos_folder_link = create_photos_folder(drive_service,file_name,tabs_names[i])
                            #        create_logs(sheets_service,file_name,tabs_names[i], file_id, tabs_ids[i], calendar_id, photos_folder_id, file_link, calendar_link, photos_folder_link,logs_data)
                            #        make_file_public(drive_service,file_id,'reader')
                            #        if photos_folder_id:
                            #            make_file_public(drive_service,photos_folder_id,'writer')
                            #            attach_folder_to_calendar(calendar_service, calendar_id, photos_folder_link)
                                
            # Save the current page token and change ID after processing each change  
            last_processed_change_time=change['time']
            store_state('state between changes',change['time'], page_token, firestore_db)
            
        # After processing all changes in the page, save the newStartPageToken or nextStartPageToken
        
        if 'newStartPageToken' in response:
            # Last page, save this token for the next polling interval
            page_token = response.get('newStartPageToken')
            print(f'newStartPageToken {page_token}')
            store_state(current_time,last_processed_change_time,page_token,firestore_db)
        elif 'nextPageToken' in response:
            page_token = response.get('nextPageToken')
            print(f'nextPageToken {page_token}')
            store_state(current_time,last_processed_change_time,page_token,firestore_db)
            main_function(drive_service, sheets_service, calendar_service, firestore_db) #keep processing the changes in next page before returning
        else:
            response = drive_service.changes().getStartPageToken().execute()
            page_token = response.get('startPageToken')
            print(f'There was no newStartPageToken or nextPageToken, got new getStartPageToken {page_token} but this in theory should never happen')
            store_state(current_time,last_processed_change_time,page_token,firestore_db)
        
        # Get a reference to the current local namespace
        local_vars = locals()
        
        # Delete all variables in the local namespace
        for var in local_vars.copy():  # Use copy() to avoid modifying the dictionary during iteration
            del local_vars[var]
        gc.collect()
    return