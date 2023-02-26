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
    logging.info(F'Influx Org URL: https://{influx_host}/orgs/{influx_orgid}')

    if 'mtagid' in enrichments:
        if enrichments['mtagid'] == "2":
            influx_bucket = os.environ['MTAG2_BUCKET']
            mtag_writer = os.environ['MTAG2_WRITER']
            meas_name = "ftd"
        elif enrichments['mtagid'] == "3":
            influx_bucket = os.environ['MTAG3_BUCKET']
            mtag_writer = os.environ['MTAG3_WRITER']
            meas_name = "ftd"
        else:
            meas_name = "nomtag"
    else:
        meas_name = "nomtag"
    
    # Got the measurement name, now dreate the Metric in "meas" object
    meas = Metric(meas_name)
    # Add DEV_EUI and Gateway ID as Tags
    meas.add_tag('dev_eui', str(data['deviceId']))
    meas.add_tag('gateway_eui', str(telemetry['gateway']))

    if 'decoder' in telemetry:
        meas.add_value('decoder', telemetry['decoder'])
    
    # Calculate and store measurement timestamp for 'influxdb line protocol' - nano seconds.
    if 'rx_time' in telemetry:
        rx_time=telemetry['rx_time']
    else:
        rx_time=telemetry['radio_time']
    meas.add_value('rx_time',rx_time)
    meas.add_value('rcv_time',rx_time)
    # store with correct time for digit count in provided rx_time string
    digits = len(str(telemetry['rx_time'])) - 11
    # adjust to nanoseconds rounded integer (pad zeros)
    meas_time=int(round(Decimal(rx_time),digits)*1000000000)
    # Save in Influx base on timestamp provided by Gatewa (rx_time)
    meas.with_timestamp(meas_time)

    # counter_up is key to find missed frames
    if 'counter_up' in telemetry: 
        meas.add_value('counter_up', int(telemetry['counter_up']))
        meas.add_value('f_count', int(telemetry['counter_up']))

    # Add Radio Info
    if 'rssi' in telemetry:
        meas.add_value('rssi', int(telemetry['rssi']))
    if 'snr' in telemetry: 
        meas.add_value('snr', round(float(telemetry['snr']),2))
    if 'size' in telemetry: 
        meas.add_value('frame_size', int(telemetry['size']))
    if 'port' in telemetry: 
        meas.add_value('port', int(telemetry['port']))
    if 'payload_base64' in telemetry: 
        meas.add_value('payload_base64', str(telemetry['payload_base64']))
    if 'datarate' in telemetry: 
        meas.add_value('datarate', int(telemetry['datarate']))
    if 'frequency' in telemetry: 
        meas.add_value('frequency', str(telemetry['frequency']))
    if 'bandwidth' in telemetry: 
        meas.add_value('bandwidth', int(telemetry['bandwidth']))
    if 'spreading_factor' in telemetry: 
        meas.add_value('spreading_factor', int(telemetry['spreading_factor']))
    # Packet Info
    if 'duplicate' in telemetry: 
        meas.add_value('duplicate', bool(telemetry['duplicate']))
    if 'packet_hash' in telemetry: 
        meas.add_value('packet_hash', str(telemetry['packet_hash']))
    if 'packet_id' in telemetry: 
        meas.add_value('packet_id', bool(telemetry['packet_id']))
    if 'packet_time' in telemetry: 
        meas.add_value('packet_time', telemetry['packet_time'])
    if 'delay' in telemetry: 
        meas.add_value('delay', telemetry['delay'])

    # Add Gateway Location if provided
    if 'gw_location' in telemetry: 
        meas.add_value('gw_latitude', round(telemetry['gw_location']['lat'],6))
        meas.add_value('gw_longitude', round(telemetry['gw_location']['lon'],6))
        if 'alt' in telemetry['gw_location']:
            meas.add_value('gw_altitude', int(telemetry['gw_location']['alt']))
        else:
            meas.add_value('gw_altitude', int(0))

    # Add Tags from csv string
    if 'tags' in telemetry:
        for i, val in enumerate(telemetry['tags'].split(",")):
            meas.add_value(f"tag{i+1}", str(val))

    # Decoded Payload
    if 'message_type' in telemetry:
        meas.add_value('message_type', str(telemetry['message_type']))
    if 'temperature' in telemetry: 
        meas.add_value('temperature', telemetry['temperature'])
    if 'battery_voltage' in telemetry: 
        meas.add_value('battery_voltage', telemetry['battery_voltage'])
    if 'ul_counter' in telemetry: 
        meas.add_value('ul_counter', int(telemetry['ul_counter']))
    if 'gps_quality' in telemetry: 
        meas.add_value('gps_quality', str(telemetry['gps_quality']))
    if 'gps_sats' in telemetry: 
        meas.add_value('gps_sats', int(telemetry['gps_sats']))
    if 'gps_hdop' in telemetry: 
        meas.add_value('gps_hdop', telemetry['gps_hdop'])
    if 'gps_valid' in telemetry: 
        meas.add_value('gps_valid', bool(telemetry['gps_valid']))
    if 'dl_counter' in telemetry: 
        meas.add_value('dl_counter', telemetry['dl_counter'])
    if 'dl_rssi' in telemetry: 
        meas.add_value('dl_rssi', bool(telemetry['dl_rssi']))
    if 'dl_snr' in telemetry: 
        meas.add_value('dl_snr', telemetry['dl_snr'])
    # Save Device GPS Location info
    if 'device_location' in telemetry:
        meas.add_value('latitude', telemetry['device_location']['lat'])
        meas.add_value('longitude', telemetry['device_location']['lon'])

    # Share (base64) payload
    if 'payload_base64' in telemetry:
        meas.add_value('payload_base64', telemetry['payload_base64'])

    # Record Measurement in Logs for Debug
    logging.info(F"Saving to Measurement:\n{meas}")

    # send it to InfluxDB Cloud
 
    influxcloud_url = F'https://{influx_host}/api/v2/write?org={influx_orgid}&bucket={influx_bucket}&precision=ns'

    # check settings
    logging.info(F'InfluxCloud_URL: {influxcloud_url}')
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
