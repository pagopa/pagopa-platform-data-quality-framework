prefix    = "pagopa"
env       = "prod"
env_short = "p"

tags = {
  CreatedBy   = "Terraform"
  Environment = "Prod"
  Owner       = "pagoPA"
  Source      = "https://github.com/pagopa/your-repository" # TODO
  CostCenter  = "TS310 - PAGAMENTI & SERVIZI"
}

apim_dns_zone_prefix               = "platform"
external_domain                    = "pagopa.it"
hostname = "weuprod.<domain>.internal.platform.pagopa.it" # TODO
