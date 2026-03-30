prefix    = "pagopa"
env       = "uat"
env_short = "u"

tags = {
  CreatedBy   = "Terraform"
  Environment = "Uat"
  Owner       = "pagoPA"
  Source      = "https://github.com/pagopa/your-repository" # TODO
  CostCenter  = "TS310 - PAGAMENTI & SERVIZI"
}

apim_dns_zone_prefix               = "uat.platform"
external_domain                    = "pagopa.it"
hostname = "weuuat.<domain>.internal.uat.platform.pagopa.it" # TODO
