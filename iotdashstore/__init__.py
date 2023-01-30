## Version 0.4 - convert json to influxdb line protocol
# there must be a better way...

import os
import logging
import azure.functions as func
import json
import requests
from influx_line_protocol import Metric
from decimal import Decimal

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Converting to Influx Line Protocol')

    data = json.loads(req.get_body())
    logging.info(F'Data:\n{data}')
    enrichments = data['enrichments']
    logging.info(F'Enrichments Received:\n{enrichments}')
    telemetry = data['telemetry']
    logging.info(F'Telemetry Received:\n{telemetry}')
    influx_host = os.environ['INFLUX_HOST']
    influx_orgid = os.environ['INFLUX_ORGID']
    logging.info(F'Influx Org: https://{influx_host}/orgs/{influx_orgid}')

    if 'mtagid' in enrichments:
        if enrichments['mtagid'] == "1":
            influx_bucket = os.environ['MTAG1_BUCKET']
            mtag_writer = os.environ['MTAG1_WRITER']
            meas_name = "mtag1"
        elif enrichments['mtagid'] == "2":
            influx_bucket = os.environ['MTAG2_BUCKET']
            mtag_writer = os.environ['MTAG2_WRITER']
            meas_name = "mtag2"
        elif enrichments['mtagid'] == "0":
            influx_bucket = os.environ['MTAG0_BUCKET']
            mtag_writer = os.environ['MTAG0_WRITER']
            meas_name = "mtag0"
        else:
            meas_name = "nomtag"
    else:
        meas_name = "nomtag"
    
    # Got the measurement name, now dreate the Metric in "meas" object
    meas = Metric(meas_name)
    # Add DEV_EUI and Gateway ID as Tags
    meas.add_tag('dev_eui', str(data['deviceId']))
    meas.add_tag('gateway_eui', str(telemetry['gateway']))

    # Calculate and store measurement timestamp for 'influxdb line protocol' - nano seconds.
    rx_time=telemetry['rx_time']
    meas.add_value('rx_time',str(rx_time))
    # store with correct time for digit count in provided rx_time string
    digits = len(str(telemetry['rx_time'])) - 11
    # adjust to nanoseconds rounded integer (pad zeros)
    meas_time=int(round(Decimal(rx_time),digits)*1000000000)
    # Save in Influx base on timestamp provided by Gatewa (rx_time)
    meas.with_timestamp(meas_time)

    # counter_up is key to find missed frames
    if 'counter_up' in telemetry: 
        meas.add_value('counter_up', int(telemetry['counter_up']))

    # Add Radio Data
    if 'rssi' in telemetry:
        meas.add_value('rssi', int(telemetry['rssi']))
    if 'snr' in telemetry: 
        meas.add_value('snr', round(float(telemetry['snr']),2))
    if 'size' in telemetry: 
        meas.add_value('frame_size', int(telemetry['size']))
    if 'datarate' in telemetry: 
        meas.add_value('datarate', int(telemetry['datarate']))
    if 'frequency' in telemetry: 
        meas.add_value('frequency', str(telemetry['frequency']))
    if 'bandwidth' in telemetry: 
        meas.add_value('bandwidth', int(telemetry['bandwidth']))
    if 'spreading_factor' in telemetry: 
        meas.add_value('spreading_factor', int(telemetry['spreading_factor']))
    if 'duplicate' in telemetry: 
        meas.add_value('duplicate', bool(telemetry['duplicate']))

    # Add Gateway Location if provided
    if 'gw_location' in telemetry: 
        meas.add_value('gw_latitude', round(telemetry['gw_location']['lat'],6))
        meas.add_value('gw_longitude', round(telemetry['gw_location']['lon'],6))
        meas.add_value('gw_altitude', int(telemetry['gw_location']['alt']))


    # Add Tags - CSV - commas ugh
    if 'tags' in telemetry: 
        meas.add_value('tags', str(telemetry['tags']))

    # Record Measurement in Logs for Debug
    logging.info(F"Saving to Measurement:\n{meas}")

    # send it to InfluxDB Cloud
 
    influxcloud_url = F'https://{influx_host}/api/v2/write?org={influx_orgid}&bucket={influx_bucket}&precision=ns'
    logging.info(F'InfluxCloud_URL: {influxcloud_url}')
    
    # check settings
    # logging.info(F'MTAG_TOKEN: {mtag_writer}')
    headers = {
        'Authorization': F'Token {mtag_writer}',
        'Content-Type': 'text/plain; charset=utf-8',
        'Accept': 'application/json'
    }
    influxcloud_response = requests.post(influxcloud_url, headers=headers, data=str(meas))
 
    logging.info(F'InfluxCloud Response:\n{influxcloud_response}')
    return func.HttpResponse(
        F'InfluxCloud Response: {influxcloud_response}\n',
        status_code=200
    )
