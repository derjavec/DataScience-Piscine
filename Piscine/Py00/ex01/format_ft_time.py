import time
from datetime import datetime

seconds = time.time()
date = datetime.now()

print(f"Seconds since January 1, 1970: {seconds:,.4f} \
or {seconds:.2e}in scientific notation")

print(date.strftime("%b %d %Y"))
