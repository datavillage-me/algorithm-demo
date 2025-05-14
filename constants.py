import config

GET_FRAUDULENT_ACCOUNTS_QUERY = {
  "select": [
    {
      "property": {"property": "account_number"}
    },
    {
      "property": {"property": "reporter_bic"}
    },
    {
      "property": {"property": "critical_account"}
    },
    {
      "property": {"property": "date_added"}
    }
  ],
  "where": {
    "equalA": {
      "property": "bank_id"
    },
    "equalB": {
      "stringValue": config.BANK_ID
    }
  }
}

RESULT_COLUMNS = [
    {
      "name": "account_number",
      "type": "VARCHAR"
    },
    {
      "name": "reporter_bic_list",
      "type": "VARCHAR"
    },
    {
      "name": "date_added_list",
      "type": "VARCHAR"
    },
    {
      "name": "critical_account_list",
      "type": "VARCHAR"
    },
    {
      "name": "line_count",
      "type": "VARCHAR"
    }
  ]