package requests

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
