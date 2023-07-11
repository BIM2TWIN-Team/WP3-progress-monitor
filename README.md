# BIM2TWIN WP3 Progress Monitoring

This repo consist of scripts to create as-performed nodes like `action`, `operation` and `construction` in BIM2TWIN DTP
and perform progress monitering at activity level.

Set your `DEV_TOKEN`, `DTP_DOMAIN` and `LOG_DIR` in `DTP_API/DTP_config.xml`

## Create as-performed node

> **Warning**
> This script creates/modifies multiple nodes in the DTP.

The script will create as-performed nodes except as-performed elements. This script need to be run to create or update
nodes as new data scans are introduced to the DTP. The result of this script will directly influence progress
monitering.

```shell
python3 create_asperformed.py
```

The above script ignores nodes if the node already exist in the graph. But you have an option to force update node
with `--force_update`. If this flag is set, nodes will be updated even if the node already exist in the graph.

## Progress monitor

![B2T progress monitor](assets/progress.jpg)

This script run progress monitering at activity level with DTP.

```shell
python3 progress_monitoring.py
```

## Delete as-performed node

> **Warning**
> This script deletes multiple nodes in the DTP.

The below command will delete all `construction`, `operation` and `action` nodes

```shell
python3 delete_asperformed.py --target_level 'all'
```