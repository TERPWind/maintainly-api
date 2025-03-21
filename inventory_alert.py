import requests
import pandas as pd 
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import os 
import logging
import yaml

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

with open("config.yaml") as f:
    config = yaml.safe_load(f)


def generate_inventory_alerts(df):
    """
    Generates inventory alerts based on quantity, par level, and critical level.

    Parameters:
    df (pd.DataFrame): DataFrame containing 'quantity', 'par_level', and 'critical_level' columns.

    Returns:
    pd.DataFrame: DataFrame with an added 'alert' column.
    """
    def alert_logic(row):
        # Ensure default values in case of NaN
        quantity = row['quantity'] if pd.notna(row['quantity']) else 0
        par_level = row['par_level'] if pd.notna(row['par_level']) else 0
        critical_level = row['critical_level'] if pd.notna(row['critical_level']) else 0

        if quantity <= critical_level:
            return "URGENT: Critically Low Stock!"
        elif quantity < par_level:
            return "Warning: Stock is Low"
        return "Stock OK"

    # Ensure required columns exist
    required_cols = {'quantity', 'par_level', 'critical_level'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

    # Convert columns to numeric to prevent TypeErrors
   
    df.fillna({'quantity': 0, 'par_level': 0, 'critical_level': 0}, inplace=True)

    df['alert'] = df.apply(alert_logic, axis=1)
    return df

def send_email_notification(filtered_df):
    """
    Sends email notifications for inventory alerts.

    Parameters:
    filtered_df (pd.DataFrame): DataFrame containing filtered inventory alerts.
    """
    filtered_df = filtered_df[filtered_df["store.title"] == "Sheffield Parts Co. - TERP"]
    # rename columns
    filtered_df = filtered_df.rename(columns={"store.title": "Site", "title": "Part Name", "type": "Type", 
                                              "model": "Model",  
                                               "internal_reference": "Internal Reference",
                                                "price": "Price", "quantity": "Quantity", "par_level": "Par Level", 
                                                "critical_level": "Critical Level", 
                                                "alert": "Alert"})
    # Reorder columns
    filtered_df = filtered_df[["Site", "Part Name", "Type", "Model", "Internal Reference", "Price", "Quantity", "Par Level", "Critical Level", "Alert"]]
    # Order by alert type
    filtered_df = filtered_df.sort_values(by=["Quantity", "Alert"], ascending=[True, False])

    # Ensure DataFrame is not empty

    if filtered_df.empty:
        logging.info("No alerts to send.")
        return
    else:
        
        logging.info("Sending email notifications...")
        SERVER = "SMTP.hydro.local"
        sender = "beka.martinez@brookfieldrenewable.com"
        receivers = ["beka.martinez@brookfieldrenewable.com"
                    #  ,"James.Slicer@terraformpower.com"
                     ]
        html_body = f"""<html>
        <head></head>
        <body>
            <p><strong>Inventory Alerts - Sheffield Parts Co. - TERP </strong></p>
            <p>Please review the inventory levels below and take the appropriate action based on the alert type.</p>

            <ul>
                <li><strong>URGENT: Critically Low Stock!</strong> Quantity is at or below the critical level <br>
                    <i>(Condition: quantity ≤ critical level)</i>. Immediate action required!</li>
                <li><strong>Warning: Stock is Low</strong> – Quantity is below the par level but above the critical level <br>
                    <i>(Condition: critical level < quantity < par level)</i>. Consider reordering soon.</li>
            </ul>

            {filtered_df.to_html(index=False)}
        </body>
        </html>"""

        # Construct the email message
        msg = MIMEMultipart('alternative')
        msg['From'] = sender
        msg['To'] = ", ".join(receivers)
        msg['Subject'] = "Sheffield Weekly Inventory Update [TEST]"
        msg.attach(MIMEText(html_body, 'html'))
        today = datetime.now().date().isoformat()
        csv_attachment_path = f"data/historical_emails/inventory_alerts_{today}.csv"
        new_files_df = pd.DataFrame(filtered_df)
        new_files_df.to_csv(csv_attachment_path, index=False)
        # Attach the CSV file as a part of the email
        part = MIMEBase('application', 'octet-stream')
        with open(csv_attachment_path, 'rb') as file_attachment:
            part.set_payload(file_attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(csv_attachment_path)}"')
        msg.attach(part)
        # Send the email via SMTP
        try:
            with smtplib.SMTP(SERVER) as smtpObj:
                smtpObj.sendmail(sender, receivers, msg.as_string())
            logging.info("Email sent successfully!")
        except Exception as e:
            logging.info(f"Failed to send email: {e}")


def get_inventory_data():
    PAT= config['PAT']
    ORGANIZATION = config['ORGANIZATION']

    # Base URL
    url = f"https://app.maintainly.com/v1/{ORGANIZATION}/inventories"

    # Headers
    headers = {
        "Authorization": f"Bearer {PAT}"
    }

    # Initialize variables
    all_data = []
    page = 1
    per_page = 25  # Adjust this based on API limits

    # Loop through paginated API responses
    while True:
        try:
            # Add pagination parameters
            params = {
                "page": page,
                "per_page": per_page
            }

            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raise an error for 4xx, 5xx responses

            data = response.json()

            # Check if 'data' key exists and contains records
            if 'data' not in data or not data['data']:
                logging.info(f"No more data to fetch. Stopped at page {page}.")
                break

            # Append data to all_data list
            all_data.extend(data['data'])
            logging.info(f"🔹 Page {page} retrieved successfully. Total records so far: {len(all_data)}")

            # If fewer records than per_page are returned, it's the last page
            if len(data['data']) < per_page:
                break

            page += 1  # Move to next page

        except requests.exceptions.HTTPError as http_err:
            logging.info(f"HTTP error occurred on page {page}: {http_err}")
            break
        except requests.exceptions.RequestException as req_err:
            logging.info(f"Request error on page {page}: {req_err}")
            break
        except json.JSONDecodeError:
            logging.info(f"Failed to parse JSON on page {page}")
            break
        except Exception as e:
            logging.info(f"Unexpected error: {e}")
            break

    # Check if we got any data before proceeding
    if not all_data:
        logging.info("No data retrieved. Exiting script.")
        exit()

    # Save all data to JSON for verification
    file_path = 'full_inventory_2025_test.json'
    try:
        with open(file_path, 'w') as file:
            json.dump(all_data, file)
        logging.info(f"All inventory data saved to {file_path}")
    except Exception as e:
        logging.info(f"Failed to save JSON: {e}")
    # Convert to DataFrame
    try:
        inventory_df = pd.json_normalize(all_data, errors="ignore").reset_index(drop=True)
        inventory_df["unique_id"] = inventory_df.index  # Ensure unique ID is correct
    except Exception as e:
        logging.info(f"Error converting JSON to DataFrame: {e}")
        exit()
    return inventory_df

def flatten_process_inventory_data(inventory_df):
    # Ensure `inventories` column is always a list
    inventory_df["inventories"] = inventory_df["inventories"].apply(lambda x: x if isinstance(x, list) else [])

    # Expand inventories into a separate DataFrame
    flattened_inventories = []
    for idx, row in inventory_df.iterrows():
        for inv in row["inventories"]:
            inv_flat = pd.json_normalize(inv, errors="ignore")
            inv_flat["unique_id"] = idx  # Keep track of parent record
            flattened_inventories.append(inv_flat)

    # If inventories exist, concatenate them
    if flattened_inventories:
        inventories_df = pd.concat(flattened_inventories, ignore_index=True)
        logging.info(f"Flattened {len(inventories_df)} inventory records.")
    else:
        logging.info("No inventories found to flatten.")
        inventories_df = pd.DataFrame()

    # Merge flattened inventories with the main inventory data
    if not inventories_df.empty:
        merged_data = pd.merge(inventory_df.drop(columns=["inventories"]), inventories_df, on="unique_id", how="left")
    else:
        merged_data = inventory_df.drop(columns=["inventories"])

    # If merged data is not empty, process further
    if not merged_data.empty:
        columnstokeep = [
            "store.title", "store.cycle_count_system_value", "store.cycle_count_system_type",
            "title", "type", "model", "manufacturer", "asset_model", "unit_measurement",
            "internal_reference", "archive", "price", "unique_id", "quantity", "par_level", "critical_level"
        ]

        # Keep only existing columns safely
        merged_data = merged_data.loc[:, merged_data.columns.intersection(columnstokeep)]

        # Ensure numeric conversion only for existing numeric columns
        numeric_cols = ["quantity", "par_level", "critical_level"]
        for col in numeric_cols:
            if col in merged_data.columns:
                merged_data.loc[:, col] = pd.to_numeric(merged_data[col], errors="coerce").fillna(0)

        # Fix: Ensure numeric values before processing
        merged_data.fillna(0, inplace=True)
    return merged_data

def main():
    # Get inventory data
    inventory_data = get_inventory_data()
    merged_data = flatten_process_inventory_data(inventory_data)
    ## Bobs additions to filter out the data
    merged_data = merged_data[merged_data["type"] != "Procurement Pending"]
    merged_data = merged_data[merged_data["par_level"] != 0]
    merged_data = merged_data[merged_data["critical_level"] != 0]
    alerts_df = generate_inventory_alerts(merged_data)
    # Filter for urgent and warning alerts
    alert_filter = alerts_df["alert"].isin(["URGENT: Critically Low Stock!", "Warning: Stock is Low"])
    filtered_alerts = alerts_df[alert_filter]
    if not filtered_alerts.empty:
        logging.info("📩 Sending email notifications...")

        # Fix: Convert `.unique()` result to a Python list, dropping NaN values
        sites_with_alerts = filtered_alerts["store.title"].dropna().unique().tolist()
        print(sites_with_alerts)

        ## Add here for each other site to send specific email

        # Fix: Check if `send_email_notification()` expects a DataFrame or list
        send_email_notification(filtered_alerts)  # Pass filtered alerts DataFrame

        logging.info("Email notifications sent successfully!")
    else:
        logging.info("No urgent or warning alerts found.")

if __name__ == "__main__":
    main()




  



   


