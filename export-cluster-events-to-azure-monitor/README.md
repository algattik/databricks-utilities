# Export Databricks Cluster Events to Azure Monitor (Log Analytics)

This PowerShell script synchronizes Databricks [cluster
events](https://docs.azuredatabricks.net/user-guide/clusters/event-log.html)
into a Log Analytics workspace for easy retention and querying.

You can easily run this script on a timer using Azure Automation.

The script uses an Azure Storage table to store the date of the latest event it
has pulled on every execution, so that it can resume with subsequent events on
following runs.
