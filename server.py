from flask import Flask, render_template_string, send_from_directory
from flask_sock import Sock
import json
from datetime import datetime
from collections import deque
import pandas as pd
import threading
import os
import time
from pytz import timezone
from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger  # Import loguru's logger
from fyers_apiv3 import fyersModel

import credentials as cr

APP_ID       = cr.APP_ID
APP_TYPE     = cr.APP_TYPE
SECRET_KEY   = cr.SECRET_KEY
FY_ID        = cr.FY_ID
APP_ID_TYPE  = cr.APP_ID_TYPE
TOTP_KEY     = cr.TOTP_KEY
PIN          = cr.PIN
REDIRECT_URI = cr.REDIRECT_URI



# Declare vars for
## flask wserverappl  and sockserverappl 
app = Flask(__name__)
sock = Sock(app)


## Ticks imdb
TICK_DEQUE_MAXLEN = 50
tick_deque = deque(maxlen=TICK_DEQUE_MAXLEN)

## Candles imdb for rendering purpose 
CANDLES_DEQUE_MAXLEN = 9000 # 1day 5sec = 4500 now 2 days with today day data 
base_candle_deque = deque(maxlen=CANDLES_DEQUE_MAXLEN)


## var for sending udpate and thread  handling 
ws_client = None
ws_thread = None




###################################################### Loguru Configuration ###############################################
# Remove previous logger.basicConfig usage.
# Configure loguru to log messages to both console and file with rotation.
# logger.remove()  # Remove any default configuration



ist = timezone('Asia/Kolkata')
log_filename = datetime.now(ist).strftime('%b%d').lower() + ".log"
# Add logger with dynamic filename
logger.add(
    log_filename,
    rotation="5 MB",
    retention="10 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} [{level}] {message}"
)

logger.info(f"Initial tick_deque: {list(tick_deque)}")

######################################################

# Path for saving and reading csvs
import os 
data_dir = '/var/lib/data/'


if not os.path.exists(data_dir):
    print(f"Data dir {data_dir} dont exist")
else:
    print(f"Data dir {data_dir} does exist")

""" Take incoming tick replay or (ws_client ticks then also  save to tick_deque)   and update 
the 5sec candles deque base_candle_deque and also send 
the updated candles latest to  active sub ws 
"""

#  save ws (the current WebSocket connection) in a global/shared variable when the client first connects.
active_ws = None

@sock.route("/ws")
def ws_endpoint(ws):
    """
    Sets this connection as the active WebSocket and keeps it open.
    The active connection will receive candle updates directly from update_candle_with_tick.
    """
    global active_ws
    active_ws = ws
    try:
        # Keep the connection open
        while True:
            ws.receive()  # This call blocks until a message is received (or connection is closed)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        if active_ws == ws:
            active_ws = None

# Helper functions


## Util1

# def update_base_candle_deque(incoming_tick):
#     """
#     Processes an incoming tick to update the 5-second candle.
#     If the tick falls within the current 5-second bucket, the candle is updated;
#     otherwise, a new candle is created.
#     After updating, sends the updated candle to the active WebSocket if connected.
#     """
#     global base_candle_deque, active_ws
    
#     # Convert tick's exchange feed time to an integer timestamp
#     tick_dt = datetime.utcfromtimestamp(incoming_tick["exch_feed_time"]).replace(microsecond=0)
#     tick_timestamp = int(tick_dt.timestamp())
#     # Determine the 5-second bucket
#     bucket_time = tick_timestamp - (tick_timestamp % 5)
    
#     if base_candle_deque and base_candle_deque[-1]['time'] == bucket_time:
#         # Update the existing candle
#         last_candle = base_candle_deque[-1]
#         last_candle['high'] = max(last_candle['high'], last_candle["ltp"])
#         last_candle['low'] = min(last_candle['low'], last_candle["ltp"])
#         last_candle['close'] = incoming_tick["ltp"]
#         updated_candle = last_candle
#     else:
#         # Create a new candle for a new 5-second bucket
#         new_candle = {
#             'time': bucket_time,
#             'open': incoming_tick["ltp"],
#             'high': incoming_tick["ltp"],
#             'low': incoming_tick["ltp"],
#             'close': incoming_tick["ltp"]
#         }
#         base_candle_deque.append(new_candle)
#         updated_candle = new_candle

#     # Send the updated candle to the active WebSocket if connected
#     if active_ws:
#         try:
#             active_ws.send(json.dumps({'candle': updated_candle}, default=str))
#         except Exception as e:
#             logger.error(f"Error sending to WebSocket: {e}")
#             active_ws = None

def update_base_candle_deque(incoming_tick):
    global base_candle_deque, active_ws

    # incoming_tick["exch_feed_time"] is IST‑based epoch
    utc_ts = int(incoming_tick["exch_feed_time"]) - 19800
    # bucket it on 5s
    bucket_time = utc_ts - (utc_ts % 5)

    price = incoming_tick["ltp"]
    if base_candle_deque and base_candle_deque[-1]['time'] == bucket_time:
        last = base_candle_deque[-1]
        last['high']  = max(last['high'], price)
        last['low']   = min(last['low'],  price)
        last['close'] = price
        updated = last
    else:
        new_candle = {
            'time':  bucket_time,
            'open':  price,
            'high':  price,
            'low':   price,
            'close': price
        }
        base_candle_deque.append(new_candle)
        updated = new_candle

    if active_ws:
        try:
            active_ws.send(json.dumps({'candle': updated}, default=str))
        except Exception as e:
            logger.error(f"Error sending to WebSocket: {e}")
            active_ws = None


## Util2 
def gen_auth_token():
    # Need credentials.py in  same folder 
    import requests, time, base64, struct, hmac
    from fyers_apiv3 import fyersModel
    from urllib.parse import urlparse, parse_qs 
    import pyotp
    from urllib import parse
    import sys

    # Get access token using the new authentication logic

    # API endpoints
    BASE_URL = "https://api-t2.fyers.in/vagator/v2"
    BASE_URL_2 = "https://api-t1.fyers.in/api/v3"
    URL_SEND_LOGIN_OTP = BASE_URL + "/send_login_otp"
    URL_VERIFY_TOTP = BASE_URL + "/verify_otp"
    URL_VERIFY_PIN = BASE_URL + "/verify_pin"
    URL_TOKEN = BASE_URL_2 + "/token"
    URL_VALIDATE_AUTH_CODE = BASE_URL_2 + "/validate-authcode"

    SUCCESS = 1
    ERROR = -1
    # mongo_window
    def wait_for_next_totp_window():
        """Wait until the next 30-second TOTP window starts with buffer"""
        now = time.time()
        window_size = 30
        remaining = window_size - (now % window_size)
        wait_time = remaining + 2  # Add 2-second buffer
        logger.info(f"Waiting {wait_time:.1f}s for fresh TOTP window")
        time.sleep(wait_time)

    def send_login_otp(fy_id, app_id):
        """Send login OTP with retries"""
        for attempt in range(3):
            try:
                result = requests.post(URL_SEND_LOGIN_OTP, 
                                     json={"fy_id": fy_id, "app_id": app_id},
                                     timeout=5)
                if result.status_code == 200:
                    return [SUCCESS, result.json()["request_key"]]
                logger.warning(f"OTP send failed (attempt {attempt+1}): {result.text}")
            except Exception as e:
                logger.error(f"Network error (attempt {attempt+1}): {str(e)}")
            time.sleep(1)
        return [ERROR, "Failed after 3 attempts"]

    def verify_totp(request_key, totp):
        """Verify TOTP with enhanced logger"""
        logger.info(f"Verifying TOTP with request_key: {request_key[:15]}...")
        try:
            result = requests.post(URL_VERIFY_TOTP,
                                 json={"request_key": request_key, "otp": totp},
                                 timeout=5)
            if result.status_code == 200:
                return [SUCCESS, result.json()["request_key"]]
            return [ERROR, f"HTTP {result.status_code}: {result.text}"]
        except Exception as e:
            return [ERROR, str(e)]

    def generate_totp(secret):
        """Generate TOTP aligned with server time"""
        try:
            # Generate TOTP for current and next window
            totp = pyotp.TOTP(secret)
            current = totp.now()
            next_otp = totp.at(time.time() + 30)
            return [SUCCESS, current, next_otp]
        except Exception as e:
            return [ERROR, str(e)]

    def verify_PIN(request_key, pin):
        """Verify PIN with timeout"""
        try:
            result = requests.post(URL_VERIFY_PIN,
                                 json={
                                     "request_key": request_key,
                                     "identity_type": "pin",
                                     "identifier": pin
                                 },
                                 timeout=5)
            if result.status_code == 200:
                return [SUCCESS, result.json()["data"]["access_token"]]
            return [ERROR, f"HTTP {result.status_code}: {result.text}"]
        except Exception as e:
            return [ERROR, str(e)]

    def token(fy_id, app_id, redirect_uri, app_type, access_token):
        """Get auth token with improved error handling"""
        try:
            result = requests.post(
                URL_TOKEN,
                json={
                    "fyers_id": fy_id,
                    "app_id": app_id,
                    "redirect_uri": redirect_uri,
                    "appType": app_type,
                    "code_challenge": "",
                    "state": "sample_state",
                    "scope": "",
                    "nonce": "",
                    "response_type": "code",
                    "create_cookie": True
                },
                headers={'Authorization': f'Bearer {access_token}'},
                timeout=5
            )
            if result.status_code == 308:
                auth_code = parse.parse_qs(parse.urlparse(result.json()["Url"]).query)['auth_code'][0]                
                return [SUCCESS, auth_code]
            return [ERROR, f"HTTP {result.status_code}: {result.text}"]
        except Exception as e:
            return [ERROR, str(e)]

    # Main authentication flow with window alignment
    for attempt in range(3):  # Total authentication attempts
        try:
            # Wait for fresh TOTP window before starting
            wait_for_next_totp_window()

            # Generate auth code URL
            session = fyersModel.SessionModel(
                client_id=client_id,
                secret_key=SECRET_KEY,
                redirect_uri=REDIRECT_URI,
                response_type='code',
                grant_type='authorization_code'
            )
            urlToActivate = session.generate_authcode()
            logger.info(f"Auth URL: {urlToActivate}")

            # Step 1 - Send login OTP
            send_otp_result = send_login_otp(FY_ID, APP_ID_TYPE)
            if send_otp_result[0] != SUCCESS:
                raise Exception(f"OTP send failed: {send_otp_result[1]}")

            # Step 2 - Generate TOTP (current and next)
            generate_result = generate_totp(TOTP_KEY)
            if generate_result[0] != SUCCESS:
                raise Exception(f"TOTP generation failed: {generate_result[1]}")
            current_totp, next_totp = generate_result[1], generate_result[2]

            # Step 3 - Verify TOTP with retries
            for totp_attempt in range(2):
                verify_result = verify_totp(send_otp_result[1], current_totp)
                if verify_result[0] == SUCCESS:
                    break
                # Try next TOTP if current fails
                verify_result = verify_totp(send_otp_result[1], next_totp)
                if verify_result[0] == SUCCESS:
                    break
                time.sleep(1)
            else:
                raise Exception("TOTP verification failed after 2 attempts")

            # Step 4 - Verify PIN
            verify_pin_result = verify_PIN(verify_result[1], PIN)
            if verify_pin_result[0] != SUCCESS:
                raise Exception(f"PIN verification failed: {verify_pin_result[1]}")

            # Step 5 - Get auth code
            token_result = token(
                FY_ID, APP_ID, REDIRECT_URI, APP_TYPE, verify_pin_result[1]
            )
            if token_result[0] != SUCCESS:
                raise Exception(f"Token failed: {token_result[1]}")

            # Step 6 - Final access token
            session.set_token(token_result[1])
            response = session.generate_token()
            if response['s'] == 'ERROR':
                raise Exception(f"Final token error: {response.get('message', 'Unknown error')}")

            access_token = response["access_token"]
            logger.info("Authentication successful")
            break  # Exit retry loop on success

        except Exception as e:
            logger.error(f"Authentication attempt {attempt+1} failed: {str(e)}")
            if attempt == 2:
                logger.critical("All authentication attempts failed")
                sys.exit(1)
            time.sleep(5)
    else:
        logger.critical("All authentication attempts failed")
        sys.exit(1)


        access_token = response["access_token"]
        print(access_token)

    return access_token




def get_hist(clientId,accessToken):
    """
    Fetches historical candle data from Fyers API and populates the candles_data deque.
    """
    global base_candle_deque
    fyers = fyersModel.FyersModel(client_id=clientId, is_async=False, token=accessToken, log_path="./")
    

    #@mongo1
    static_date = datetime(2025, 4, 21)
    # Format static date
    current_date = static_date.strftime("%Y-%m-%d")
    range_from = current_date
    range_to = current_date
    data = {
        "symbol": "NSE:NIFTY50-INDEX",
        "resolution": "5S",
        "date_format": "1",
        "range_from": range_from,
        "range_to": range_to,
        "cont_flag": "1"
    }

    res = fyers.history(data=data)

    if "candles" in res:
        # Each candle is a list of 6 elements (time, open, high, low, close, volume)
        # We only want the first five, so we slice each candle accordingly.
        candles_sliced = [candle[:5] for candle in res['candles']]
        df = pd.DataFrame(candles_sliced, columns=['time', 'open', 'high', 'low', 'close'])

        # Convert time to integer timestamps if needed.
        df["time"] = df["time"].astype(int)

        # Convert historical timestamps from IST to UTC by subtracting 5.5 hours (19800 seconds)
        df["time"] = df["time"] - 19800

        # Append to candles_data deque
        for candle in df.to_dict(orient="records"):
            base_candle_deque.append(candle)

        logger.info(f"Historical candles appended. Total count: {len(base_candle_deque)}")
    else:
        logger.error("Failed to fetch historical data.")




def ws_client_connect(accessToken):

    # Rest of WebSocket setup remains unchanged

    # Now use the access token to connect to Fyers WebSocket
    from fyers_apiv3.FyersWebsocket import data_ws

    def onmessage(message):
        """
        Callback function for handling incoming messages from Fyers WebSocket.
        """
        logger.info(f"Raw message received: {message}")
        
        if isinstance(message, str):
            try:
                tick = json.loads(message)
                logger.info(f"Parsed JSON message: {tick}")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON message: {e}")
                return
        else:
            tick = message

        # Check if this is a market data message (has 'ltp' and 'exch_feed_time')
        if "ltp" in tick and "exch_feed_time" in tick:
            price = tick["ltp"]
            tick_time = datetime.utcfromtimestamp(tick["exch_feed_time"]).replace(microsecond=0)
            
            tick_deque.append({'timestamp': tick_time, 'price': price})
            update_base_candle_deque(tick)

            logger.info(f"Added tick data: price={price}, time={tick_time}")
            logger.info(f"Current deque size: {len(tick_deque)}")
            
            if len(tick_deque) == tick_deque.maxlen:
                logger.info("Deque reached maximum capacity. Flushing to CSV...")
                ist = timezone('Asia/Kolkata')
                current_date = datetime.now(ist).strftime('%b%d').lower()
                csv_filename = f'{current_date}.csv'
                df = pd.DataFrame(list(tick_deque))
                df.to_csv(os.path.join(data_dir, csv_filename), mode='a', header=False, index=False)
                tick_deque.clear()
                logger.info("CSV file updated and deque cleared")
        elif "type" in tick and tick["type"] in ["cn", "ful", "sub"]:
            # These are connection/subscription messages, not market data
            logger.info(f"Received system message: {tick['type']} - {tick.get('message', '')}")
        else:
            logger.warning(f"Unexpected message format: {tick}")
            
    def onerror(message):
        print("Error:", message)

    def onclose(message):
        print("Connection closed:", message)

    def onopen():
        # Subscribe to data using the FyersDataSocket instance.
        # Here, we use the same instance (ws_client) to subscribe.
        data_type = "SymbolUpdate"
        symbols = ['NSE:NIFTY50-INDEX']
        ws_client.subscribe(symbols=symbols, data_type=data_type)
        ws_client.keep_running()

    global ws_client
    ws_client = data_ws.FyersDataSocket(
        access_token=accessToken,  # Use the access token obtained from the new authentication logic
        log_path="",
        litemode=False,
        write_to_file=False,
        reconnect=True,
        on_connect=onopen,
        on_close=onclose,
        on_error=onerror,
        on_message=onmessage
    )

    # Establish the connection (this call blocks until the connection is closed)
    ws_client.connect()

##########################

client_id = f'{APP_ID}-{APP_TYPE}'

# bk_replay_loop
def realtime_feed_main():
    # 1. 
    access_token = gen_auth_token()

    # 2. get_hist()
    # Fetch historical data before starting real-time updates and append to base_candle_deaque
    get_hist(client_id, access_token)

    # 3. realtime_feed
    ws_client_connect(access_token)





###################################################### Flask Routes #######################################################


@app.route("/historic")
def get_historic_candles():
    """
    Returns all historical candles stored in memory.
    This endpoint is called when the chart is first loaded or refreshed.
    """
    return json.dumps(list(base_candle_deque), default=str)


@app.route("/download-csv")
def download_csv():
    """Provides a list of downloadable CSV files."""
    try:
        # List all CSV files in the data directory
        files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        html = "<h1>Download CSV Files</h1>"
        html += "<ul>"
        for file in files:
            html += f'<li><a href="/download-file/{file}">{file}</a></li>'
        html += "</ul>"
        return html
    except Exception as e:
        logger.error(f"Error listing CSV files: {e}")
        return "Error in listing files."


@app.route("/download-file/<filename>")
def download_file(filename):
    """Sends a file from the data directory to the client."""
    try:
        return send_from_directory(data_dir, filename, as_attachment=True)
    except Exception as e:
        logger.error(f"Error sending file {filename}: {e}")
        return "Error in sending file."



######################################################
###################################################### Flask WebSocket Server #######################################################





LW_CHART = """
<script>
   const chart = LightweightCharts.createChart(document.getElementById('chart'), {
    width: window.innerWidth,
    height: window.innerHeight,
    priceScale: { borderColor: '#cccccc' },
    timeScale: { 
        borderColor: '#cccccc', 
        timeVisible: true, 
        secondsVisible: true,
        tickMarkFormatter: (time) => {
            const utcDate = new Date(time * 1000); // Convert UNIX time to Date object (UTC)
            const istDate = new Date(utcDate.getTime() + (5.5 * 60 * 60 * 1000)); // Convert to IST
            return istDate.toLocaleTimeString('en-IN');
        }
    },
    localization: {
        timeFormatter: (time) => {
            const utcDate = new Date(time * 1000);
            const istDate = new Date(utcDate.getTime() + (5.5 * 60 * 60 * 1000)); 
            return istDate.toLocaleDateString('en-IN') + ' ' + istDate.toLocaleTimeString('en-IN');
        }
    }
});

   const candleSeries = chart.addCandlestickSeries();
   
</script>
"""

SET_DATA_X = """
<script>

   // First, fetch historical data
   fetch('/historic')
     .then(function(response => response.json())
     .then(candles => {
       // Set the initial data
       candleSeries.setData(candles);

</script>

"""


SET_DATA = """
<script>
  // First, fetch historical data
  fetch('/historic')
    .then(response => response.json())    // ← close this .then()
    .then(candles => {
      // Set the initial data
      candleSeries.setData(candles);
    })
    .catch(error => console.error('Error fetching historical data:', error));
</script>
"""


UPDATE_DATA_X  = """
<script>
       
       // Then connect to WebSocket for real-time updates
       const ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws");
       
       ws.onmessage = function(event) {
           const data = JSON.parse(event.data);
           if (data.candle) {
               // Receiving a candle directly from the server
               candleSeries.update(data.candle);
           }
       };
     })
     .catch(error => {
       console.error('Error fetching historical data:', error);
     });
</script>
"""


UPDATE_DATA = """
<script>
  // Then connect to WebSocket for real-time updates
  const ws = new WebSocket(
    (location.protocol === "https:" ? "wss://" : "ws://") +
    location.host + "/ws"
  );

  ws.onmessage = function(event) {
    const data = JSON.parse(event.data);
    if (data.candle) {
      // Receiving a candle directly from the server
      candleSeries.update(data.candle);
    }
  };

  ws.onerror = error => console.error('WebSocket error:', error);
</script>
"""



JS_BLOCK =  f"""
{LW_CHART}
{SET_DATA}
{UPDATE_DATA}
"""

@app.route("/")
def index():
    """Serves the frontend chart."""
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Live NIFTY Tick Chart (IST)</title>
        <script src="https://cdn.jsdelivr.net/gh/parth-royale/cdn@main/lightweight-charts.standalone.production.js"></script>
    </head>
    <body>
        <h1>Live NIFTY Tick Chart (IST)</h1>
        <div id="chart"></div>

        {JS_BLOCK}

    </body>
    </html>
    """
    return render_template_string(html)


###################################################### Main Flow #######################################################
def main():
    """Starts the WebSocket client thread."""
    # create_table_if_not_exists()
    # Start ws_client_connect in a separate thread.
    global ws_thread
    # logger.info(" ws_thread Thread Started .")
    print(" ws_thread Thread Started .")

    ws_thread = threading.Thread(target=realtime_feed_main, daemon=True)
    ws_thread.start()
    logger.info("Fyers WebSocket thread started.")

def stop_main():
    """Stops the WebSocket client connection gracefully."""
    global ws_client
    logger.info("Stopping Fyers WebSocket connection gracefully...")
    if ws_client:
        try:
            ws_client.close_connection()
            logger.info("WebSocket connection closed.")
        except Exception as e:
            logger.error("Error closing WebSocket connection: %s", e)


main()

###################################################### Scheduler Setup #######################################################
# from apscheduler.schedulers.background import BackgroundScheduler
# scheduler = BackgroundScheduler(daemon=True)

# # Schedule main() to start the WebSocket client (adjust the time as needed)
# scheduler.add_job(
#     main,
#     'cron',
#     day_of_week='mon-fri',
#     hour=9,
#     minute=14,
#     timezone='Asia/Kolkata'
# )

# # Schedule stop_main() to close the connection at a specified time
# scheduler.add_job(
#     stop_main,
#     'cron',
#     day_of_week='mon-fri',
#     hour=15,
#     minute=31,
#     timezone='Asia/Kolkata',
#     id='stop_main'
# )

# scheduler.start()

###################################################### Start Flask App #######################################################
port = int(os.getenv('PORT', 80))
print('Listening on port %s' % (port))
app.run(debug=False, host="0.0.0.0", port=port)
