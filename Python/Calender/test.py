from datetime import datetime, date, timedelta
from time import strftime

business_date_start = '2022-03-01'
business_date_end = '2022-03-31'
from_date = business_date_start.replace('-','')
to_date = business_date_end.replace('-','')
processing_date = from_date+'_'+to_date
print(processing_date)