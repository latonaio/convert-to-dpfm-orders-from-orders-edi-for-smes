package dpfm_api_processing_formatter

type ProcessingFormatterSDC struct {
	Header                     *Header                     `json:"Header"`
	ConversionProcessingHeader *ConversionProcessingHeader `json:"ConversionProcessingHeader"`
	Item                       []*Item                     `json:"Item"`
	ConversionProcessingItem   []*ConversionProcessingItem `json:"ConversionProcessingItem"`
	ItemPricingElement         []*ItemPricingElement       `json:"ItemPricingElement"`
	ItemScheduleLine           []*ItemScheduleLine         `json:"ItemScheduleLine"`
	Address                    []*Address                  `json:"Address"`
	Partner                    []*Partner                  `json:"Partner"`
}

type ConversionProcessingKey struct {
	SystemConvertTo       string   `json:"SystemConvertTo"`
	SystemConvertFrom     string   `json:"SystemConvertFrom"`
	LabelConvertTo        string   `json:"LabelConvertTo"`
	LabelConvertFrom      string   `json:"LabelConvertFrom"`
	CodeConvertFromInt    *int     `json:"CodeConvertFromInt"`
	CodeConvertFromFloat  *float32 `json:"CodeConvertFromFloat"`
	CodeConvertFromString *string  `json:"CodeConvertFromString"`
	BusinessPartner       int      `json:"BusinessPartner"`
}

type ConversionProcessingCommonQueryGets struct {
	CodeConversionID      int      `json:"CodeConversionID"`
	SystemConvertTo       string   `json:"SystemConvertTo"`
	SystemConvertFrom     string   `json:"SystemConvertFrom"`
	LabelConvertTo        string   `json:"LabelConvertTo"`
	LabelConvertFrom      string   `json:"LabelConvertFrom"`
	CodeConvertFromInt    *int     `json:"CodeConvertFromInt"`
	CodeConvertFromFloat  *float32 `json:"CodeConvertFromFloat"`
	CodeConvertFromString *string  `json:"CodeConvertFromString"`
	CodeConvertToInt      *int     `json:"CodeConvertToInt"`
	CodeConvertToFloat    *float32 `json:"CodeConvertToFloat"`
	CodeConvertToString   *string  `json:"CodeConvertToString"`
	BusinessPartner       int      `json:"BusinessPartner"`
}

type Header struct {
	ConvertingOrderID                          string   `json:"ConvertingOrderID"`
	OrderDate                                  *string  `json:"OrderDate"`
	ConvertingOrderType                        *string  `json:"ConvertingOrderType"`
	ConvertingBuyer                            *string  `json:"ConvertingBuyer"`
	ConvertingSeller                           *string  `json:"ConvertingSeller"`
	ConvertingBillToParty                      *string  `json:"ConvertingBillToParty"`
	ConvertingBillFromParty                    *string  `json:"ConvertingBillFromParty"`
	ConvertingPayer                            *string  `json:"ConvertingPayer"`
	ConvertingPayee                            *string  `json:"ConvertingPayee"`
	CreationDate                               *string  `json:"CreationDate"`
	LastChangeDate                             *string  `json:"LastChangeDate"`
	TotalNetAmount                             *float32 `json:"TotalNetAmount"`
	TotalTaxAmount                             *float32 `json:"TotalTaxAmount"`
	TotalGrossAmount                           *float32 `json:"TotalGrossAmount"`
	TransactionCurrency                        *string  `json:"TransactionCurrency"`
	RequestedDeliveryDate                      *string  `json:"RequestedDeliveryDate"`
	RequestedDeliveryTime                      *string  `json:"RequestedDeliveryTime"`
	ConvertingPaymentMethod                    *string  `json:"ConvertingPaymentMethod"`
	HeaderText                                 *string  `json:"HeaderText"`
	HeaderBlockStatus                          *bool    `json:"HeaderBlockStatus"`
	HeaderBillingBlockStatus                   *bool    `json:"HeaderBillingBlockStatus"`
	HeaderDeliveryBlockStatus                  *bool    `json:"HeaderDeliveryBlockStatus"`
	IsCancelled                                *bool    `json:"IsCancelled"`
	IsMarkedForDeletion                        *bool    `json:"IsMarkedForDeletion"`
	ConvertingStockConfirmationBusinessPartner *string  `json:"ConvertingStockConfirmationBusinessPartner"`
}

type ConversionProcessingHeader struct {
	ConvertingOrderID       *string `json:"ConvertingOrderID"`
	ConvertedOrderID        *int    `json:"ConvertedOrderID"`
	ConvertingOrderType     *string `json:"ConvertingOrderType"`
	ConvertedOrderType      *string `json:"ConvertedOrderType"`
	ConvertingBuyer         *string `json:"ConvertingBuyer"`
	ConvertedBuyer          *int    `json:"ConvertedBuyer"`
	ConvertingSeller        *string `json:"ConvertingSeller"`
	ConvertedSeller         *int    `json:"ConvertedSeller"`
	ConvertingBillToParty   *string `json:"ConvertingBillToParty"`
	ConvertedBillToParty    *int    `json:"ConvertedBillToParty"`
	ConvertingBillFromParty *string `json:"ConvertingBillFromParty"`
	ConvertedBillFromParty  *int    `json:"ConvertedBillFromParty"`
	ConvertingPayer         *string `json:"ConvertingPayer"`
	ConvertedPayer          *int    `json:"ConvertedPayer"`
	ConvertingPayee         *string `json:"ConvertingPayee"`
	ConvertedPayee          *int    `json:"ConvertedPayee"`
	ConvertingPaymentMethod *string `json:"ConvertingPaymentMethod"`
	ConvertedPaymentMethod  *string `json:"ConvertedPaymentMethod"`
}

type Item struct {
	ConvertingOrderID                          string   `json:"ConvertingOrderID"`
	ConvertingOrderItem                        string   `json:"ConvertingOrderItem"`
	OrderItemText                              *string  `json:"OrderItemText"`
	ConvertingProduct                          *string  `json:"ConvertingProduct"`
	ConvertingProductGroup                     *string  `json:"ConvertingProductGroup"`
	BaseUnit                                   *string  `json:"BaseUnit"`
	RequestedDeliveryDate                      *string  `json:"RequestedDeliveryDate"`
	RequestedDeliveryTime                      *string  `json:"RequestedDeliveryTime"`
	ConvertingDeliverToParty                   *string  `json:"ConvertingDeliverToParty"`
	ConvertingDeliverFromParty                 *string  `json:"ConvertingDeliverFromParty"`
	CreationDate                               *string  `json:"CreationDate"`
	LastChangeDate                             *string  `json:"LastChangeDate"`
	DeliverFromPlant                           *string  `json:"DeliverFromPlant"`
	DeliverFromPlantBatch                      *string  `json:"DeliverFromPlantBatch"`
	DeliveryUnit                               *string  `json:"DeliveryUnit"`
	ConvertingStockConfirmationBusinessPartner *string  `json:"ConvertingStockConfirmationBusinessPartner"`
	StockConfirmationPlant                     *string  `json:"StockConfirmationPlant"`
	StockConfirmationPlantBatch                *string  `json:"StockConfirmationPlantBatch"`
	OrderQuantityInBaseUnit                    *float32 `json:"OrderQuantityInBaseUnit"`
	OrderQuantityInDeliveryUnit                *float32 `json:"OrderQuantityInDeliveryUnit"`
	QuantityPerPackage                         *float32 `json:"QuantityPerPackage"`
	NetAmount                                  *float32 `json:"NetAmount"`
	GrossAmount                                *float32 `json:"GrossAmount"`
	ConvertingTransactionTaxClassification     *string  `json:"ConvertingTransactionTaxClassification"`
	ConvertingPaymentMethod                    *string  `json:"ConvertingPaymentMethod"`
	ConvertingProject                          *string  `json:"ConvertingProject"`
	ItemBlockStatus                            *bool    `json:"ItemBlockStatus"`
	ItemBillingBlockStatus                     *bool    `json:"ItemBillingBlockStatus"`
	ItemDeliveryBlockStatus                    *bool    `json:"ItemDeliveryBlockStatus"`
	IsCancelled                                *bool    `json:"IsCancelled"`
	IsMarkedForDeletion                        *bool    `json:"IsMarkedForDeletion"`
}

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

type ItemPricingElement struct {
	ConvertingOrderID          string   `json:"ConvertingOrderID"`
	ConvertingOrderItem        string   `json:"ConvertingOrderItem"`
	ConvertingBuyer            *string  `json:"ConvertingBuyer"`
	ConvertingSeller           *string  `json:"ConvertingSeller"`
	ConditionRateValue         *float32 `json:"ConditionRateValue"`
	ConditionCurrency          *string  `json:"ConditionCurrency"`
	ConditionQuantity          *float32 `json:"ConditionQuantity"`
	ConditionQuantityUnit      *string  `json:"ConditionQuantityUnit"`
	ConditionAmount            *float32 `json:"ConditionAmount"`
	TransactionCurrency        *string  `json:"TransactionCurrency"`
	ConditionIsManuallyChanged *bool    `json:"ConditionIsManuallyChanged"`
}

type ItemScheduleLine struct {
	ConvertingOrderID                           string   `json:"ConvertingOrderID"`
	ConvertingOrderItem                         string   `json:"ConvertingOrderItem"`
	Product                                     *string  `json:"Product"`
	ConvertingStockConfirmationBussinessPartner *string  `json:"ConvertingStockConfirmationBussinessPartner"`
	StockConfirmationPlantBatch                 *string  `json:"StockConfirmationPlantBatch"`
	RequestedDeliveryDate                       *string  `json:"RequestedDeliveryDate"`
	OriginalOrderQuantityInBaseUnit             *float32 `json:"OriginalOrderQuantityInBaseUnit"`
}

type Address struct {
	ConvertingOrderID string  `json:"ConvertingOrderID"`
	PostalCode        *string `json:"PostalCode"`
}

type Partner struct {
	ConvertingOrderID string  `json:"ConvertingOrderID"`
	Currency          *string `json:"Currency"`
}
