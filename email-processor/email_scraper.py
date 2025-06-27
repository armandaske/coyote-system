from email import policy
from email.parser import BytesParser
from base64 import urlsafe_b64decode
from re import findall, sub
from email_scraper_helpers import booking_logic, cancellation_logic, rebooking_logic, other_logic, add_label_to_email, get_label_id, get_current_time, get_time_week_before
import logging

platforms_by_address_dict={
           'express@airbnb.com' : 'Airbnb',
           'automated@airbnb.com' : 'Airbnb',
           'messages@fareharbor.com' : 'Fareharbor',
           'info@coyoteaventuras.com' : 'Fareharbor'
          } 

procesado_label_object = {
    'name': 'Procesado',
    'labelListVisibility': 'labelShow',
    'messageListVisibility': 'show'
}

revisar_label_object = {
    'name': 'Revisar',
    'labelListVisibility': 'labelShow',
    'messageListVisibility': 'show'
}

actions = {'reservacion': booking_logic,
           'cancelacion': cancellation_logic,
           'rebook' : rebooking_logic,
           'otro' : other_logic,
          }

def get_action_for_email(subject,platform):
    subject = subject.lower()  # Convert subject to lowercase

    if platform.lower() == 'airbnb':
        if 'reservación confirmada para' in subject:
            return actions['reservacion']
        elif 'tu experiencia para' in subject:
            return actions['reservacion']
        elif 'booking confirmed' in subject:
            return actions['reservacion']
        elif 'tuvo que cancelar su' in subject:
            return actions['cancelacion']
        elif 'had to cancel' in subject:
            return actions['cancelacion']
        elif 'rebooked' in subject:
            return actions['rebook']
        
    elif platform.lower() == 'fareharbor':
        if 'new booking' in subject or 'new online booking' in subject:
            return actions['reservacion']
        elif 'cancelled' in subject: 
            return actions['cancelacion']
        elif 'rebooked' in subject:
            return actions['rebook']
    
    return actions['otro']


def email_scraper_main(drive_service, sheets_service, gmail_service):
    procesado_label_id=get_label_id(gmail_service, procesado_label_object.get('name'))
    revisar_label_id=get_label_id(gmail_service, revisar_label_object.get('name'))
    
    if not procesado_label_id:
        created_label = gmail_service.users().labels().create(userId='me', body=procesado_label_object).execute()
        procesado_label_id=created_label['id']    
        
    if not revisar_label_id:
        created_label = gmail_service.users().labels().create(userId='me', body=revisar_label_object).execute()
        revisar_label_id=created_label['id']
        
    current_time_str = get_current_time()
    logging.info(f"Looking for new emails at: {current_time_str}")
    week_before=get_time_week_before()
    queries = {
        f'from:(express@airbnb.com) NOT label:Procesado NOT label:Revisar after:{week_before}',
        f'from:(automated@airbnb.com) NOT label:Procesado NOT label:Revisar after:{week_before}',
        f'from:(messages@fareharbor.com) NOT label:Procesado NOT label:Revisar after:{week_before}',
        f'from:(info@coyoteaventuras.com) NOT label:Procesado NOT label:Revisar after:{week_before}'
    }
    for query in queries:
        try:
            results = gmail_service.users().messages().list(userId='me', q=query, maxResults=3).execute()
            messages = results.get('messages')
            del results
            if messages:
                for msg in messages:
                    msg_id=str(msg['id'])
                    logging.info('analizando mensaje',msg_id)
                    txt = gmail_service.users().messages().get(userId='me', id=msg['id'], format='raw').execute()
                    # Get message payload
                    data = txt['raw']
                    del txt
                    # Decode the base64 message body
                    data = data.replace("-","+").replace("_","/")
                    decoded_data = urlsafe_b64decode(data)
                    del data
                    msg = BytesParser(policy=policy.default).parsebytes(decoded_data)
                    del decoded_data
                    if msg.is_multipart():
                        sender = msg.get('From')
                        email_matches = findall(r'\S+@\S+', sender)
                        if email_matches:
                            email_address = email_matches[0]
                            email_address=sub(r'^[^a-zA-Z]+|[^a-zA-Z]+$', '', email_address) #strip non alpha values at beggining and end
                        else:
                            logging.info("No email address found in the email.")
                            email_address = None
                            
                        subject = msg.get('Subject')
                        platform= platforms_by_address_dict.get(email_address)
                        action= get_action_for_email(subject,platform)
                        perform_action=action(drive_service,sheets_service,msg,platform)
                        # After processing the email
                        if perform_action:
                            if action==other_logic:
                                add_label_to_email(gmail_service,'me', msg_id, subject, [revisar_label_id],'Revisar')
                            else:
                                add_label_to_email(gmail_service,'me', msg_id, subject, [procesado_label_id],'Procesado') #label_ids needs to be passed as a list, even if only one element.
                        else:
                            logging.info(f'Something ocurred when {action} was performed, email with ID:{msg_id}')
                            add_label_to_email(gmail_service,'me', msg_id, subject, [revisar_label_id], 'Revisar')
                    else:
                        logging.info(f"Email with ID: {msg_id} is not multipart, it might not contain HTML.")
                        add_label_to_email(gmail_service,'me', msg_id, subject, [revisar_label_id], 'Revisar') 
                        
                del messages
                
            else:
                logging.info("No emails found for that query.")
        except Exception as error:
            logging.info(f'An error occurred in the main process: {error}')
            add_label_to_email(gmail_service,'me', msg_id, subject, [revisar_label_id], 'Revisar')
    return    