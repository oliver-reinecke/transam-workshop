# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd
# Import the custom function from your project library
from myfunctions import check_for_alert
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Get input and output datasets directly by their names
input_dataset = dataiku.Dataset("unified_weekly_metrics_windows")
output_dataset = dataiku.Dataset("python_leading_indicators_w_alerts")
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Read the input dataset into a Pandas DataFrame
df = input_dataset.get_dataframe()
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# --- Ensure correct data types and sort for rolling averages ---
# Convert 'week_start' to datetime if not already
df['week_start'] = pd.to_datetime(df['week_start'])
# Sort data to ensure rolling calculations are correct
df = df.sort_values(by=['region', 'week_start'])
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# --- Calculate % Change vs. Rolling Average for each metric ---
# Ensure rolling average columns are numeric, coerce errors to NaN
df['new_applications_rolling_avg'] = pd.to_numeric(df['new_applications_rolling_avg'], errors='coerce')
df['logins_rolling_avg'] = pd.to_numeric(df['logins_rolling_avg'], errors='coerce')
df['quotes_created_rolling_avg'] = pd.to_numeric(df['quotes_created_rolling_avg'], errors='coerce')
df['revenue_usd_rolling_avg'] = pd.to_numeric(df['revenue_usd_rolling_avg'], errors='coerce')
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Calculate percentage change, handling division by zero or NaN rolling averages
def calculate_change(current_value, rolling_avg):
    if pd.notna(rolling_avg) and rolling_avg != 0:
        return (current_value - rolling_avg) / rolling_avg
    return 0 # Or NaN, depending on desired behavior for no rolling average
df['new_applications_change_vs_avg'] = df.apply(
    lambda row: calculate_change(row['new_applications_sum'], row['new_applications_rolling_avg']),
    axis=1
)
df['logins_change_vs_avg'] = df.apply(
    lambda row: calculate_change(row['logins_sum'], row['logins_rolling_avg']),
    axis=1
)
df['quotes_created_change_vs_avg'] = df.apply(
    lambda row: calculate_change(row['quotes_created_sum'], row['quotes_created_rolling_avg']),
    axis=1
)
df['revenue_usd_change_vs_avg'] = df.apply(
    lambda row: calculate_change(row['revenue_usd_sum'], row['revenue_usd_rolling_avg']),
    axis=1
)
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# --- Calculate Alert Flag using the imported function and project variable ---
# Retrieve the custom variable for the alert threshold
alert_threshold = float(dataiku.get_custom_variables()["alert_threshold"])
# Apply the check_for_alert function for each metric
df['alert_flag_applications'] = df['new_applications_change_vs_avg'].apply(check_for_alert, alert_threshold=alert_threshold)
df['alert_flag_logins'] = df['logins_change_vs_avg'].apply(check_for_alert, alert_threshold=alert_threshold)
df['alert_flag_quotes_created'] = df['quotes_created_change_vs_avg'].apply(check_for_alert, alert_threshold=alert_threshold)
df['alert_flag_revenue'] = df['revenue_usd_change_vs_avg'].apply(check_for_alert, alert_threshold=alert_threshold)
# Combine all alert flags into a single 'overall_alert_flag'
df['overall_alert_flag'] = (df['alert_flag_applications'] | df['alert_flag_logins'] |
                            df['alert_flag_quotes_created'] | df['alert_flag_revenue']).astype(int)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write the result to the output dataset
output_dataset.write_with_schema(df)
