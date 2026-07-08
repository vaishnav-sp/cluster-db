# Configuration Package

## Architecture

The configuration package provides a small, reusable configuration layer for executables in ClusterDB. It keeps defaults in one place, loads environment-specific YAML files, applies environment overrides, and validates the resulting configuration.

## How Loading Works

The loader selects the configuration file based on the APP_ENV value:

- development -> configs/development.yaml
- production -> configs/production.yaml
- docker -> configs/docker.yaml
- any other value -> configs/development.yaml

The loader applies defaults first, reads the YAML file, then applies environment variable overrides using Viper's automatic environment support.

## Environment Overrides

Environment variables can override YAML values using the same names as the config fields, converted to uppercase with underscores. Examples:

- SERVER_PORT
- LOG_LEVEL
- NODE_ID

## How to Add New Configuration

1. Add a field to the relevant config struct in config.go.
2. Add a default value in defaults.go.
3. Add a YAML entry in the appropriate file under configs/.
4. Add validation in validator.go if needed.

## Example

```go
cfg, err := config.Load()
if err != nil {
    return err
}
_ = cfg
```
