package requests

type ConversionProcessingItem struct {
	ConvertingOrderItem                    *string `json:"ConvertingOrderItem"`
	ConvertedOrderItem                     *int    `json:"ConvertedOrderItem"`
	ConvertingProductGroup                 *string `json:"ConvertingProductGroup"`
	ConvertedProductGroup                  *string `json:"ConvertedProductGroup"`
	ConvertingDeliverToParty               *string `json:"ConvertingDeliverToParty"`
	ConvertedDeliverToParty                *int    `json:"ConvertedDeliverToParty"`
	ConvertingDeliverFromParty             *string `json:"ConvertingDeliverFromParty"`
	ConvertedDeliverFromParty              *int    `json:"ConvertedDeliverFromParty"`
	ConvertingTransactionTaxClassification *string `json:"ConvertingTransactionTaxClassification"`
	ConvertedTransactionTaxClassification  *string `json:"ConvertedTransactionTaxClassification"`
	ConvertingProject                      *string `json:"ConvertingProject"`
	ConvertedProject                       *string `json:"ConvertedProject"`
}
