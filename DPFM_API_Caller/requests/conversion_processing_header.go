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
	ConvertingPaymentMethod *string `json:"ConvertingPaymentMethod"`
	ConvertedPaymentMethod  *string `json:"ConvertedPaymentMethod"`
}
