## Version 0.5 - convert json to influxdb line protocol
# adding modern client library for influx cloud
# bonus, fixes ugly timestamp to scientific notation defect

import azure.functions as func
import os, logging, json
from decimal import Decimal
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Converting to Influx Line Protocol v2')
    data = json.loads(req.get_body())
    logging.info(F'Received Data:\n{data}')
    telemetry = data['telemetry']
    logging.info(F'Telemetry Received:\n{telemetry}')
    # Process Destination from app_settings on function
    enrichments = data['enrichments']
    logging.info(F'IoT-Central >> Data Export >> Enrichments Received: {enrichments}')
    if 'mtagid' in enrichments:
        if enrichments['mtagid'] == "2":
            bucket = os.environ['MTAG2_BUCKET']
            token = os.environ['MTAG2_WRITER']
            meas = "ftd"
        elif enrichments['mtagid'] == "3":
            bucket = os.environ['MTAG3_BUCKET']
            token = os.environ['MTAG3_WRITER']
            meas = "test3"
        else:
            meas = "nomtag"
    else:
        meas = "nomtag"

    url = F"https://{os.environ['INFLUX_HOST']}"
    org = os.environ['INFLUX_ORGID']
    write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
    logging.info(F'InfluxCloud_URL: {url}, Org: {org}')
    
    # calculated digits provided tro convert to Nanoseconds
    # pad zeros for nanosends

    digits = len(str(telemetry['rx_time'])) - 11
    point_time=int(round(Decimal(telemetry['rx_time']),digits)*1000000000)

    if 'snr' in telemetry:
        logging.info(F"SNR:{telemetry['snr']},SNR_TYPE:{type(telemetry['snr'])}")
    
    # build point from metadata
    point = (
        Point(meas)
        .time(point_time)
        .tag('dev_eui', data['deviceId'])
        .tag('gateway_eui', telemetry['gateway'])
        .field('decoder', telemetry['decoder'])
        .field('rx_time',telemetry['rx_time'])
        .field('rcv_time',telemetry['rx_time'])
        .field('rx_time',telemetry['rx_time'])
        .field('counter_up', telemetry['counter_up'])
        .field('f_count', telemetry['counter_up'])
        .field('rssi', telemetry['rssi'])
        .field('snr', float(telemetry['snr']))
        .field('frame_size', telemetry['size'])
        .field('port', telemetry['port'])
        .field('payload_base64', telemetry['payload_base64'])
        .field('frequency', str(telemetry['frequency']))
        .field('bandwidth', telemetry['bandwidth'])
        .field('datarate', telemetry['datarate'])
        .field('spreading_factor', telemetry['spreading_factor'])
        .field('packet_id', telemetry['packet_id'])
        .field('packet_hash', telemetry['packet_hash'])
        .field('duplicate', telemetry['duplicate'])
        .field('packet_time', telemetry['packet_time'])
        .field('delay', telemetry['delay'])
        .field('gw_latitude', telemetry['gw_location']['lat'])
        .field('gw_longitude', telemetry['gw_location']['lon'])
        .field('gw_altitude', telemetry['gw_location']['alt'])
    )

    # Add decoded device data if present
    if 'message_type' in telemetry:
        point.field('message_type', telemetry['message_type'])
    if 'temperature' in telemetry:
        point.field('temperature', telemetry['temperature']) 
    if 'battery_voltage' in telemetry:
        point.field('battery_voltage', telemetry['battery_voltage']) 
    if 'ul_counter' in telemetry:
        point.field('ul_counter', telemetry['ul_counter'])
    if 'gps_quality' in telemetry: 
        point.field('gps_quality', telemetry['gps_quality'])
    if 'gps_sats' in telemetry:
        point.field('gps_sats', telemetry['gps_sats']) 
    if 'gps_hdop' in telemetry: 
        point.field('gps_hdop', telemetry['gps_hdop'])
    if 'gps_valid' in telemetry: 
        point.field('gps_valid', bool(telemetry['gps_valid'])) 
    if 'dl_counter' in telemetry: 
        point.field('dl_counter', telemetry['dl_counter'])
    if 'dl_rssi' in telemetry: 
        point.field('dl_rssi', telemetry['dl_rssi'])
    if 'dl_snr' in telemetry: 
        point.field('dl_snr', telemetry['dl_snr'])
    if 'device_location' in telemetry: 
        point.field('latitude', telemetry['device_location']['lat'])
    if 'device_location' in telemetry: 
        point.field('longitude', telemetry['device_location']['lon'])
    
    # Add Tags from csv string
    if 'tags' in telemetry:
        for i, val in enumerate(telemetry['tags'].split(",")):
            point.field(f"tag{i+1}", str(val))

    # Write Point to Measurement
    logging.info(F"Writing Point:\n{point}")
    write_api = write_client.write_api(write_options=SYNCHRONOUS)
    influxcloud_response = write_api.write(bucket=bucket, org=org, record=point)
 
    logging.info(F'InfluxCloud Response:\n{influxcloud_response}')
    return func.HttpResponse(
        F'InfluxCloud Response: {influxcloud_response}\n',
        status_code=200
    )
