#!/bin/bash

# Function to display usage information
display_usage() {
    echo "Usage: $0 <action> <environment> <filepath>"
    echo "  <action>: 'enc' or 'dec'"
    echo "  <environment>: 'dev', 'uat', or 'prod'"
    echo "  <filepath>: path to the file"
    echo "Example: $0 enc dev /path/to/file"
}

# Function to validate action
validate_action() {
    if [ "$1" != "enc" ] && [ "$1" != "dec" ]; then
        echo "Error: Action must be 'enc' or 'dec'"
        exit 1
    fi
}

# Function to validate environment
validate_environment() {
    if [ "$1" != "dev" ] && [ "$1" != "uat" ] && [ "$1" != "prod" ]; then
        echo "Error: Environment must be 'dev', 'uat', or 'prod'"
        exit 1
    fi
}

# Function to validate filepath
validate_filepath() {
    if [ ! -f "$1" ]; then
        echo "Error: File '$1' not found"
        exit 1
    fi
}

# Main script starts here
main() {
    # Validate number of arguments
    if [ "$#" -ne 3 ]; then
        echo "Error: Incorrect number of arguments"
        display_usage
        exit 1
    fi

    # Assign arguments to variables
    action="$1"
    environment="$2"
    filepath="$3"

    # Validate action, environment, and filepath
    validate_action "$action"
    validate_environment "$environment"
    validate_filepath "$filepath"

    env_short=$(echo "$environment" | cut -c1)

    # TODO set your kv
    azure_kv_url=$(az keyvault key show --name pagopa-"$env_short"-TODO-sops-key --vault-name pagopa-"$env_short"-TODO-kv --query key.kid | sed 's/"//g')

    if [ "$action" == "enc" ]; then
      sops --encrypt --azure-kv "$azure_kv_url" --input-type dotenv --output-type  dotenv ./"$filepath" > ./"$filepath".encrypted
    fi;

    if [ "$action" == "dec" ]; then
      sops --decrypt --azure-kv "$azure_kv_url" --input-type dotenv --output-type dotenv ./"$filepath" > "$(echo "./$filepath" | sed 's/\.encrypted$//')"
    fi;

    echo 'done'
}

# Call the main function
main "$@"
