import os
import time
import constants
from io import StringIO, BytesIO
import traceback
from dv_utils.log_utils import log, LogLevel
from dv_utils.data_engine import create_client
from dv_data_engine_client.client import Client
from dv_data_engine_client.api.default import mount_collaborator, collaborator_status, query_collaborator, append_collaborator, export_collaborator
from dv_data_engine_client.models.mount_collaborator_body import MountCollaboratorBody
from dv_data_engine_client.models.query_collaborator_body import QueryCollaboratorBody
from dv_data_engine_client.models.append_collaborator_body import AppendCollaboratorBody
from dv_data_engine_client.models.start_quality_validation_response_201 import StartQualityValidationResponse201
from dv_data_engine_client.models.finished_report import FinishedReport
from dv_data_engine_client.types import File
from dv_data_engine_client.api.quality import start_quality_validation, get_quality_report
import pandas as pd
import config

def event_processor(evt: dict):
    log("event_processor started", LogLevel.INFO)
    try:
        event_handlers = {
            "EX_GET_FRAUDULENT_ACCOUNTS": get_fraudulent_accounts,
        }
        if evt["type"] in event_handlers:
            event_handlers[evt["type"]](evt)
    except Exception:
        log("error in event_processor", LogLevel.ERROR)
        log(traceback.format_exc(), LogLevel.ERROR)
    log("done processing event", LogLevel.INFO)
    
    
def get_fraudulent_accounts(evt):
    log("get fraudulent accounts started", LogLevel.INFO)

    providers_labels = ["BANKALIST", "BANKBLIST"]
    providers_id = []
    for provider_label in providers_labels:
      providers_id.append(os.environ[f"ID_{provider_label}"])
      
    # step 1: mount/initialize the providers
    for provider_id in providers_id:
      if not __mount_provider(provider_id):
        log("could not mount provider. Stopping execution", LogLevel.ERROR)
        return
    log("successfully initialized providers", LogLevel.INFO)

    for provider_id in providers_id:
      # step 2: validate provider
      if not __validate_collaborator(provider_id):
        log(f"validation of provider didn't succeed. Stopping execution.", LogLevel.ERROR)
        return
    
    # step 3: mount/initialize the consumer
    consumer_id = os.environ["ID_BANKBAGG"]
    if not __initialize_consumer(consumer_id):
      log("could not initialize consumer. Stopping execution.", LogLevel.ERROR)
      return
    log("successfully initialized consumer", LogLevel.INFO)

    # step 4: get the fraudulent accounts from providers
    results =  pd.DataFrame()
    for provider_id in providers_id:
      results = pd.concat([results, __query(provider_id)], axis=0, ignore_index=True)
    log("successfully loaded fraudulent accounts", LogLevel.INFO)

    # step 5: process the data 
    aggregation = (
        results
        .groupby('account_number')
        .agg(
            reporter_bic_list        = ('reporter_bic', list),
            suspected_account_list = ('suspected_account', list),
            date_added_list     = ('date_added', list),
            line_count          = ('account_number', 'size')
        )
        .reset_index()
    )
    log("successfully aggregated fraudulent accounts", LogLevel.INFO)

    # step 6: append results to data consumer
    if not __append_results(aggregation, consumer_id):
      log("could not append results. Stopping execution.", LogLevel.ERROR)
      return
    log("successfully appended results to consumer", LogLevel.INFO)
    
    # step 7: validate consumer
    if not __validate_collaborator(consumer_id):
      log("could not validate consumer. Stopping execution.", LogLevel.ERROR)
      return
    log("validated consumer") 
    
    # step 8: export data consumer to bucket
    if not __export_results(consumer_id):
      log("could not export results. Stopping execution", LogLevel.ERROR)
      return
    log("successfully exported results")

    
def __mount_provider(provider_id) -> bool:
  with create_client() as c:
    mount_response = mount_collaborator.sync_detailed(client=c, collaborator_id=provider_id, body=MountCollaboratorBody())
    mount_response_text = mount_response.content.decode("utf-8", errors="replace")
    if mount_response.status_code  != 204:
      log("error mounting provider: " + mount_response_text,LogLevel.ERROR)
    # wait for mounting to be completed
    return __wait_for_status(c, provider_id, "mounted")

def __wait_for_status(client: Client, collab_id: str, expected_status: str) -> bool:
  status = __get_collab_status(client, collab_id)
  tries = 0
  while expected_status != status and tries < config.MAX_TRIES:
    if status == "error":
      log(f"error for collaborator {collab_id}", LogLevel.ERROR)
      return False
    time.sleep(config.SLEEP_S)
    status = __get_collab_status(client, collab_id)
    tries += 1
  
  return status == expected_status

def __get_collab_status(client: Client, collab_id: str) -> str:
  resp = collaborator_status.sync_detailed(client=client, collaborator_id=collab_id)
  response_text = resp.content.decode("utf-8", errors="replace")
  if resp.status_code != 204 :
    log("collaborator status: " + response_text,LogLevel.ERROR)
  return resp.parsed.to_dict()["status"]


def __initialize_consumer(consumer_id) -> bool:
  body = MountCollaboratorBody.from_dict({"columns": constants.RESULT_COLUMNS})
  with create_client() as c:
    response = mount_collaborator.sync_detailed(client=c, collaborator_id=consumer_id, body=body)
    response_text = response.content.decode("utf-8", errors="replace")
    if response.status_code  != 204:
      log("error during consumer initialization: " + response_text,LogLevel.ERROR)
    return __wait_for_status(c, consumer_id, "initialized")
  
def __query(provider_id) -> pd.DataFrame:
  body = QueryCollaboratorBody.from_dict(constants.GET_FRAUDULENT_ACCOUNTS_QUERY)
  with create_client() as c:
    resp: str = query_collaborator.sync(client=c, collaborator_id=provider_id, body=body)
    return pd.read_csv(StringIO(resp), delimiter=',')
  
def __append_results(results: pd.DataFrame, consumer_id: str) -> bool:
    csv_str = results.to_csv(index=False, header=False)
    csv_bytes = csv_str.encode('utf-8')
    f = File(payload=csv_bytes, file_name="data.csv")
    body = AppendCollaboratorBody(data=f)
    with create_client() as c:
      response = append_collaborator.sync_detailed(client=c,collaborator_id=consumer_id,body=body)
      response_text = response.content.decode("utf-8", errors="replace")
      if response.status_code  != 204:
        log("error during data insertion: " + response_text,LogLevel.ERROR)
      return __wait_for_status(c, consumer_id, "mounted")
  
def __export_results(consumer_id) -> bool:
  with create_client() as c:
    export_collaborator.sync(client=c, collaborator_id=consumer_id)
    return __wait_for_status(c, consumer_id, "exported")

def __validate_collaborator(collaborator_id: str) -> bool:
  with create_client() as c:
    resp = start_quality_validation.sync(collaborator_id=collaborator_id, client=c)
    if not isinstance(resp, StartQualityValidationResponse201):
      log(f"could not start quality validation. Got {resp}", LogLevel.ERROR)
      return False
    report_id = resp.to_dict()["id"]
    return __check_quality_report(c, report_id)

def __check_quality_report(client: Client, report_id: str) -> bool:
  report = __get_finished_report(client, report_id)
  if report is None:
    log("could not get quality report", LogLevel.ERROR)
    return False
  log(report)
  fail = report["fail"]
  error = report["error"]
  
  return len(fail) == 0 and len(error) == 0

def __get_finished_report(client: Client, report_id: str) -> object:
  max_tries = 10
  tries = 0
  sleep_s = 1
  while tries < max_tries:
    time.sleep(sleep_s)
    tries += 1
    resp = get_quality_report.sync(report_id=report_id, client=client)
    if isinstance(resp, FinishedReport):
      return resp.to_dict()
  return None