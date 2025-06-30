"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import zipfile
import pandas as pd
from datetime import datetime

def clean_campaign_data():
    input_folder = "files/input"
    output_folder = "files/output"
    os.makedirs(output_folder, exist_ok=True)

    client_data = []
    campaign_data = []
    economics_data = []

    for file in os.listdir(input_folder):
        if file.endswith(".zip"):
            zip_path = os.path.join(input_folder, file)
            with zipfile.ZipFile(zip_path) as z:
                csv_name = z.namelist()[0]  # Asume un solo CSV por ZIP
                with z.open(csv_name) as f:
                    df = pd.read_csv(f)

                    # CLIENT
                    client_df = pd.DataFrame({
                        "client_id": df["client_id"],
                        "age": df["age"],
                        "job": df["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False),
                        "marital": df["marital"],
                        "education": df["education"]
                            .str.replace(".", "_", regex=False)
                            .replace("unknown", pd.NA),
                        "credit_default": df["credit_default"].apply(lambda x: 1 if x == "yes" else 0),
                        "mortgage": df["mortgage"].apply(lambda x: 1 if x == "yes" else 0),  # ← corregido aquí
                    })
                    client_data.append(client_df)

                    # CAMPAIGN
                    campaign_df = pd.DataFrame({
                        "client_id": df["client_id"],
                        "number_contacts": df["number_contacts"],
                        "contact_duration": df["contact_duration"],
                        "previous_campaign_contacts": df["previous_campaign_contacts"],
                        "previous_outcome": df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0),
                        "campaign_outcome": df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0),
                        "last_contact_date": df.apply(
                            lambda row: datetime.strptime(
                                f"2022-{row['month']}-{int(row['day']):02}",
                                "%Y-%b-%d"
                            ).strftime("%Y-%m-%d"),
                            axis=1
                        )
                    })
                    campaign_data.append(campaign_df)

                    # ECONOMICS
                    economics_df = pd.DataFrame({
                        "client_id": df["client_id"],
                        "cons_price_idx": df["cons_price_idx"],
                        "euribor_three_months": df["euribor_three_months"],
                    })
                    economics_data.append(economics_df)

    # Guardar resultados en CSV
    pd.concat(client_data).to_csv(os.path.join(output_folder, "client.csv"), index=False)
    pd.concat(campaign_data).to_csv(os.path.join(output_folder, "campaign.csv"), index=False)
    pd.concat(economics_data).to_csv(os.path.join(output_folder, "economics.csv"), index=False)


if __name__ == "__main__":
    clean_campaign_data()
