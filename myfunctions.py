def check_for_alert(change_vs_avg, alert_threshold):
    """
    Checks if a percentage change indicates a significant dip, triggering an alert.
    Returns 1 if an alert is triggered, 0 otherwise.
    """
    if change_vs_avg < alert_threshold:
        return 1
    else:
        return 0
