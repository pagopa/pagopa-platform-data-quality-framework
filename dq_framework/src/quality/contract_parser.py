# TODO: ODCS data contract YAML parser and SodaCL translator
#
# Responsibilities:
#   1. Read and validate an ODCS YAML contract file
#   2. Extract metadata (title, version, dataset, models)
#   3. Produce the SodaCL string via the datacontract-cli Python API:
#          DataContract(data_contract_file=path).export(export_format="sodacl")
#   4. Normalise the SodaCL output so that table names match Spark temporary view names
