package requests

type ConversionProcessingItem struct {
	ConvertingOrderItem                        *string `json:"ConvertingOrderItem"`
	ConvertedOrderItem                         *int    `json:"ConvertedOrderItem"`
	ConvertingProduct                          *string `json:"ConvertingProduct"`
	ConvertedProduct                           *string `json:"ConvertedProduct"`
	ConvertingProductGroup                     *string `json:"ConvertingProductGroup"`
	ConvertedProductGroup                      *string `json:"ConvertedProductGroup"`
	ConvertingDeliverToParty                   *string `json:"ConvertingDeliverToParty"`
	ConvertedDeliverToParty                    *int    `json:"ConvertedDeliverToParty"`
	ConvertingDeliverFromParty                 *string `json:"ConvertingDeliverFromParty"`
	ConvertedDeliverFromParty                  *int    `json:"ConvertedDeliverFromParty"`
	ConvertingStockConfirmationBusinessPartner *string `json:"ConvertingStockConfirmationBusinessPartner"`
	ConvertedStockConfirmationBusinessPartner  *int    `json:"ConvertedStockConfirmationBusinessPartner"`
	ConvertingTransactionTaxClassification     *string `json:"ConvertingTransactionTaxClassification"`
	ConvertedTransactionTaxClassification      *string `json:"ConvertedTransactionTaxClassification"`
	ConvertingProject                          *string `json:"ConvertingProject"`
	ConvertedProject                           *string `json:"ConvertedProject"`
}
