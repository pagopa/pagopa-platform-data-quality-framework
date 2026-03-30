locals {
  product = "${var.prefix}-${var.env_short}"

  apim = {
    name       = "${local.product}-apim"
    rg         = "${local.product}-api-rg"
    product_id = "TODO" # TODO product id to import from pagopa-infra
  }
}

