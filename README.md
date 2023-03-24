# Solar

This CLI tool provides help with some routine Solr operations:
- Import / Export data
- Import / Export configs
- Re-index Collection (WIP)


# Usage

`pip install solar-cli`

# Export

## Export data

This command will save docs from `<collection>` to local folder `./data`:
```sh
solar -c "<collection>" "https://<username>:<password>@localhost:8333" export ./data
```

### Export nested documents
Solr can handle nested documents. To see nested structure of collection usually we add `fl="*, [child]"` to our query params. Solar can handle exporting nested documents by adding `--nested` flag:
```sh
solar -c "<collection>" "https://<username>:<password>@localhost:8333" export --nested ./data
```



## Export config

If we want to save collection config, we can user `export-config` command:

```sh
solar -c "<collection>" "https://<username>:<password>@localhost:8333" export-config ./configs
```
This will all config files to local folder `./configs`

# Import

## Import data

Later, we can import previously exported data with `import` command, and `./data/<collection>.json` as source file:
```sh
solar "https://<username>:<password>@localhost:8333" import ./data/<collection>.json
```

We do not have to specify collection name, source `.json` already have collection name. However, if we want to import data as collection with different name, we can set this with `-c` flag:
```sh
solar -c "<new collection name>" "https://<username>:<password>@localhost:8333" import ./data/<collection>.json
```

## Import config

Solar can help you import configsets to your Solr instance:
```sh
solar -c "https://<username>:<password>@localhost:8333" import-config <config folder path>
```

This command will read files from provide config path, zip them, and send to Solr. By default, created config name will be equal to config folder name. For example, if we import config from `./configs/products`, Solar will create config named `products`.

If we want to override default name, we can use `--name` flag:

```sh
solar -c "https://<username>:<password>@localhost:8333" import-config --name "product-v2" <config folder path>
```

This will create config `product-v2`.

Also, we can overwrite existing config with `--overwrite` flag
> This flag will add `cleanup=true` and `overwrite=true` params to request for creating config. However it is recommended to create config with the *new* name, because in some cases, Solr caches fields types, and some changes of new config will not be applied. Goog practice is version control your configs and name them with version identifier (commit hash, for example)
> Using this flag also requires that no collections is linked to this config

```sh
solar -c "https://<username>:<password>@localhost:8333" import-config --overwrite <config folder path>
```

# Other

## Remove config

Config `<config name>` can be removed from Solr with this command:

```sh
solar -c "https://<username>:<password>@localhost:8333" remove-config "<config name>"

```
