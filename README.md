# 🐺 Coyote Aventuras Automation System

## 📋 Overview
This project is a complete automation and data management system built for **Coyote Aventuras**, one of the largest domestic tour agency in Mexico.  
It integrates multiple services from the **Google Cloud ecosystem** to automate administrative, financial, and logistics workflows — replacing all manual data entry and streamlining operations end-to-end.

The system processes booking emails, extracts key information, updates Google Sheets in real time, manages tour schedules, and syncs financial and calendar data automatically.

---

## 🚀 Key Features

- **Email Parsing & ETL Pipeline:** Extracts booking details from confirmation emails and structures the data for storage.  
- **Automated Spreadsheet Updates:** Writes tour, financial, and logistics data directly to Google Sheets via API.  
- **Calendar Integration:** Creates and updates Google Calendar events for each tour automatically.  
- **Financial Dashboard:** Tracks revenue, expenses, and profits with real-time synchronization.  
- **Scalable Cloud Architecture:** Deployed on Google App Engine for continuous and reliable execution.  
- **AppSheet Frontend:** Provides a simple user interface for staff to view, filter, and manage tour information.  

---

## ⚙️ System Flow
Each module runs in the cloud and communicates asynchronously through Pub/Sub, ensuring real-time updates and reliability.

Incoming Email → Gmail API → Data Extraction
              → Google Sheets → Google Calendar
              → AppSheet Dashboard → Business Insights


---

## 🛠️ Technologies Used

**Programming:** Python  
**Google Cloud Services:** App Engine, Pub/Sub, Firestore, Gmail API, Google Sheets API, Google Calendar API  
**Frontend:** AppSheet (for dashboards and reporting)  
**Data Handling:** Pandas, JSON, REST APIs  
**Version Control:** Git / GitHub  

---

## 📦 Deployment
The system is deployed on **Google App Engine** with scheduled triggers and Pub/Sub events for asynchronous processing.  
Configuration files (`app.yaml`, `requirements.txt`) are used to manage environments and dependencies.

---

## 📈 Impact
- Eliminated 100% of manual data entry in administrative workflows.  
- Improved operational efficiency, allowing the company to reallocate two full-time administrative roles.
- Improved operational reliability and consistency across departments.
- Reduced booking-to-calendar update time from hours to seconds.
- Enabled real-time visibility into financial and operational data through integrated dashboards.