import pandas as pd

from .utils import *

dates = create_dates()

report_settings = {
    "cols_to_check": ['clearing_member', 'account', 'margin_type', 'margin'],
    "margin_classes": ["SPAN", "IMSM", "CESM", "AMPO", "AMEM", "AMCO", "AMCU", "AMWI", "DMEM"],
    "reports": [
        {
            "name": "cc050_eod_report",
            "table": "cc050",
            "date": create_dates()["last_day"],
            "valid_report": True,
        },
        {
            "name": "ci050_last_report",
            "table": "ci050",
            "date": create_dates()["last_day"],
            "time_of_day": create_dates()["max_time_of_day"],
            "valid_report": True,
        },
        {
            "name": "ci050_first_report",
            "table": "ci050",
            "date": create_dates()["current_day"],
            "time_of_day": create_dates()["min_time_of_day"],
            "valid_report": True,
        }
    ]
}

def main(report_config):
    
    try:
        # input validation and reporting
        is_valid, message = validate_input(report_config)
        send_report(is_valid, message)
        
        reports = fetch_reports(report_config)
        
        for key in reports.keys():
            print(key)
            
            items = reports[key]

            check_report(items['cc050_eod_report'], items['ci050_first_report'], report_config['cols_to_check'])
            check_report(items['cc050_eod_report'], items['ci050_last_report'], report_config['cols_to_check'])

    except Exception as e:
        print(f"Error at main: {str(e)}")
        return None

if __name__ == '__main__':
    main(report_settings)
