import pandas as pd
import json
import numpy as np
from utils import kibana_aloninja
import logging
from datetime import datetime, timedelta

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

log_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
log_num = log_date[-2:]
logging.basicConfig(filename=f"/home/vn_bi/source_code/aloninja-fakefail-tool/logs/load_event_alo1_{log_num}.log", 
                    filemode="a", 
                    level=logging.INFO, 
                    format='%(asctime)s %(message)s')

if __name__ == "__main__":
    try:
        final_call_log = pd.DataFrame()
        batch = 0
        for runtime in range(1,93):
            attempt_date = (datetime.today() - timedelta(days=runtime)).strftime("%Y-%m-%d")
            attempt_num = attempt_date[-2:]
            table_month = 'aloninja'
            print(f"Run data aloninja {attempt_date} - Start")
            logging.info(f"Run data aloninja {attempt_date} - Start")

            # import data from kibana
            kibana_data = kibana_aloninja.kibana(table_month, attempt_date)
            final_call_log = pd.concat([final_call_log,kibana_data])
            kibana_data.to_parquet(f'/home/vn_bi/source_code/aloninja-fakefail-tool/data/kibana_{attempt_num}.pq')
            print(f"Download from kibana {attempt_date}: Completed")
            logging.info("Download from kibana: Completed")
            batch += 1
            if batch % 10 == 0:
                final_call_log.to_parquet(f'/home/vn_bi/source_code/aloninja-fakefail-tool/data/final_call_log_{runtime}.pq')
                final_call_log =pd.DataFrame()
                print(f"done {batch}")
                logging.info(f"done {batch}")

    except Exception as e:
        print(f'Download from kibana: Fail!, Error: {e}')    
        logging.info(f'Download from kibana: Fail!, Error: {e}')    
