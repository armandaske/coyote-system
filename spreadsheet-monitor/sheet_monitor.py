def main_function(drive_service, sheets_service, calendar_service, firestore_db):
    from sheet_monitor_helpers import (
        get_last_state, get_month_from_file_name, get_year_from_file_name,
        find_last_subfolder_id, delete_logs, delete_calendar_and_folders_batch,
        update_calendar_and_folder, update_columns_logs, inspect_logs, create_logs,
        create_calendar, create_photos_folder, attach_folder_to_calendar,
        make_file_public, store_state, get_tabs, get_data_hl
    )
    import googleapiclient.errors as errors
    import re
    from datetime import datetime, timedelta
    from os import getenv
    import gc
    import warnings

    ROOT_ID = str(getenv("ROOT_ID"))

    # --- Load last checkpoint ---
    state = get_last_state(firestore_db)
    page_token = state.get("last_page_token")
    if not page_token:
        print("No page token found, creating a new one.")
        page_token = drive_service.changes().getStartPageToken().execute()["startPageToken"]

    current_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    print(f"Looking for changes in drive @ {current_time}")

    try:
        # --- Process all pages ---
        while True:
            response = drive_service.changes().list(
                pageToken=page_token,
                spaces="drive",
                fields="nextPageToken,newStartPageToken,changes(fileId,time,changeType,file/mimeType,file/name)"
            ).execute()

            for change in response.get("changes", []):
                file_id = change.get("fileId")
                change_time = change.get("time")
                if not file_id or not change_time:
                    continue

                change_key = f"{file_id}:{change_time}"

                # --- Deduplication check ---
                if firestore_db.collection("processed_changes").document(change_key).get().exists:
                    print(f"Skipping already processed change: {change_key}")
                    continue

                # --- Process spreadsheet changes only ---
                if (
                    change["changeType"] == "file"
                    and "file" in change
                    and change["file"].get("mimeType") == "application/vnd.google-apps.spreadsheet"
                ):
                    file = change["file"]
                    file_name = file.get("name")
                    file_month = get_month_from_file_name(file_name)
                    file_year = get_year_from_file_name(file_name)

                    if not file_month or not file_year:
                        continue

                    file_metadata = drive_service.files().get(
                        fileId=file_id, fields="webViewLink, trashed, parents"
                    ).execute()
                    file_parents = file_metadata.get("parents", [])

                    directory_tree = [
                        "Workflow Coyote Armando Technologies",
                        file_year,
                        "Hojas Logísticas",
                        file_month,
                    ]
                    all_folders_found, folder_id, folder_name = find_last_subfolder_id(
                        drive_service, directory_tree
                    )

                    is_child = False
                    if all_folders_found:
                        if not file_parents:
                            is_child = True
                        else:
                            is_child = folder_id in file_parents or ROOT_ID in file_parents
                    else:
                        print("No se encontró el folder de Hojas logísticas")

                    #Sí es una hoja logistica válida
                    if is_child:
                        print("Processing:", file_name)
                        file_link = file_metadata.get("webViewLink", "")
                        if file_metadata.get("trashed", "") or ROOT_ID in file_parents or not file_parents:
                            print("File deleted. Removing calendar events, photos, and logs")
                            calendar_ids, photos_folder_ids = delete_logs(sheets_service, file_id)
                            delete_calendar_and_folders_batch(
                                drive_service, calendar_service, calendar_ids, photos_folder_ids
                            )
                        else:
                            # Active or cancelled tour
                            tour_status = "cancelado" if re.search(r"cancela(do)?|cance(lo)?", file_name, re.IGNORECASE) else "activo"

                            tabs_names, tabs_ids = get_tabs(sheets_service, file_id, "ITINERARIO", True)
                            multiday = "SI" if len(tabs_ids) > 1 else "NO"

                            for i in range(len(tabs_ids)):
                                all_data = get_data_hl(sheets_service, file_id, tabs_names[i], file_name, multiday)
                                if not all_data:
                                    continue

                                keys_to_keep = [
                                    "guia","apoyo","chofer","tour_name","start_date","transporte","num_clientes","multiday",
                                    "venta","gastos","tipo_tour","tipo_costos","cobro_efectivo","cobro_transfe","cobro_izettle",
                                    "cobro_fareharbor","cobro_airbnb","cobro_tripadvisor","cobro_get_your_guide","cobro_otros",
                                    "combustible","gasto_efectivo","pago_chofer","pago_guia","pago_apoyo","pago_apoyo_2","pago_apoyo_3"
                                ]
                                logs_data = [all_data[k] for k in keys_to_keep]
                                logs_data.append(tour_status)
                                logs_data = [x if isinstance(x,(int,float,str)) else ("" if x is None else str(x)) for x in logs_data]

                                calendar_id, photos_folder_id, photos_folder_link = inspect_logs(
                                    sheets_service, file_id, tabs_ids[i], file_name, tabs_names[i], logs_data
                                )

                                if calendar_id:
                                    if not photos_folder_id:
                                        photos_folder_id, photos_folder_link = create_photos_folder(drive_service, file_name, tabs_names[i])
                                        update_columns_logs(sheets_service, file_id, tabs_ids[i], [photos_folder_id, photos_folder_link], ["D","G"])
                                    update_calendar_and_folder(drive_service, calendar_service, calendar_id, photos_folder_id,
                                                               file_name, tabs_names[i], photos_folder_link, file_link,
                                                               file.get("mimeType"), all_data)
                                else:
                                    calendar_id, calendar_link = create_calendar(calendar_service, file_link, file.get("mimeType"), file_name, all_data)
                                    if calendar_id:
                                        photos_folder_id, photos_folder_link = create_photos_folder(drive_service, file_name, tabs_names[i])
                                        create_logs(sheets_service, file_name, tabs_names[i], file_id, tabs_ids[i],
                                                    calendar_id, photos_folder_id, file_link, calendar_link, photos_folder_link, logs_data)
                                        make_file_public(drive_service, file_id, "reader")
                                        if photos_folder_id:
                                            make_file_public(drive_service, photos_folder_id, "writer")
                                            attach_folder_to_calendar(calendar_service, calendar_id, photos_folder_link)

                # --- mark change as processed ---
                firestore_db.collection("processed_changes").document(change_key).set({
                    "processed_at": datetime.utcnow().isoformat()
                })

            # --- Pagination handling ---
            if "nextPageToken" in response:
                page_token = response["nextPageToken"]
                print("Found nextPageToken:", page_token)
                continue 
            else:
                if "newStartPageToken" in response:
                    page_token = response["newStartPageToken"]
                    print("Finished processing, newStartPageToken:", page_token)
                    store_state(current_time, page_token, firestore_db)
                break

        # --- Cleanup old processed_changes (>1 days) ---
        cutoff = datetime.utcnow() - timedelta(days=1)
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            old_changes = firestore_db.collection("processed_changes").where("processed_at", "<", cutoff.isoformat()).stream()

            for doc in old_changes:
                doc.reference.delete()

    except errors.HttpError as e:
        print(f"Drive API error: {e}, resetting page token")
        page_token = drive_service.changes().getStartPageToken().execute()["startPageToken"]
        store_state(current_time, page_token, firestore_db)

    # --- Garbage collection ---
    gc.collect()
    return