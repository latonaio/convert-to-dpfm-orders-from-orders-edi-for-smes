package dpfm_api_output_formatter

import (
	dpfm_api_input_reader "convert-to-dpfm-orders-from-orders-edi-for-smes/DPFM_API_Input_Reader"
	dpfm_api_processing_formatter "convert-to-dpfm-orders-from-orders-edi-for-smes/DPFM_API_Processing_Formatter"
)

func OutputFormatter(
	sdc *dpfm_api_input_reader.SDC,
	psdc *dpfm_api_processing_formatter.ProcessingFormatterSDC,
	osdc *Output,
) error {
	header := ConvertToHeader(*sdc, *psdc)
	item := ConvertToItem(*sdc, *psdc)
	itemPricingElement := ConvertToItemPricingElement(*sdc, *psdc)
	itemScheduleLine := ConvertToItemScheduleLine(*sdc, *psdc)
	address := ConvertToAddress(*sdc, *psdc)
	partner := ConvertToPartner(*sdc, *psdc)

	osdc.DataConcatenation = DataConcatenation{
		Header:             header,
		Item:               item,
		ItemPricingElement: itemPricingElement,
		ItemScheduleLine:   itemScheduleLine,
		Address:            address,
		Partner:            partner,
	}

	osdc.ServiceLabel = "FUNCTION_ORDERS_DATA_CONCATENATION"
	osdc.APISchema = "DPFMDataConcatenation"
	osdc.APIProcessingResult = getBoolPtr(true)

	return nil
}

func ConvertToHeader(
	sdc dpfm_api_input_reader.SDC,
	psdc dpfm_api_processing_formatter.ProcessingFormatterSDC,
) *Header {
	dataProcessingHeader := psdc.Header
	dataConversionProcessingHeader := psdc.ConversionProcessingHeader

	header := &Header{
		OrderID:                   *dataConversionProcessingHeader.ConvertedOrderID,
		OrderDate:                 dataProcessingHeader.OrderDate,
		OrderType:                 dataConversionProcessingHeader.ConvertedOrderType,
		Buyer:                     dataConversionProcessingHeader.ConvertedBuyer,
		Seller:                    dataConversionProcessingHeader.ConvertedSeller,
		BillToParty:               dataConversionProcessingHeader.ConvertedBillToParty,
		BillFromParty:             dataConversionProcessingHeader.ConvertedBillFromParty,
		Payer:                     dataConversionProcessingHeader.ConvertedPayer,
		Payee:                     dataConversionProcessingHeader.ConvertedPayee,
		CreationDate:              dataProcessingHeader.CreationDate,
		LastChangeDate:            dataProcessingHeader.LastChangeDate,
		TotalNetAmount:            dataProcessingHeader.TotalNetAmount,
		TotalTaxAmount:            dataProcessingHeader.TotalTaxAmount,
		TotalGrossAmount:          dataProcessingHeader.TotalGrossAmount,
		TransactionCurrency:       dataProcessingHeader.TransactionCurrency,
		RequestedDeliveryDate:     dataProcessingHeader.RequestedDeliveryDate,
		RequestedDeliveryTime:     dataProcessingHeader.RequestedDeliveryTime,
		PaymentMethod:             dataConversionProcessingHeader.ConvertedPaymentMethod,
		HeaderText:                dataProcessingHeader.HeaderText,
		HeaderBlockStatus:         dataProcessingHeader.HeaderBlockStatus,
		HeaderDeliveryBlockStatus: dataProcessingHeader.HeaderDeliveryBlockStatus,
		HeaderBillingBlockStatus:  dataProcessingHeader.HeaderBillingBlockStatus,
		IsCancelled:               dataProcessingHeader.IsCancelled,
		IsMarkedForDeletion:       dataProcessingHeader.IsMarkedForDeletion,
	}

	return header
}

func ConvertToItem(
	sdc dpfm_api_input_reader.SDC,
	psdc dpfm_api_processing_formatter.ProcessingFormatterSDC,
) []*Item {
	dataProcessingItem := psdc.Item
	dataConversionProcessingHeader := psdc.ConversionProcessingHeader
	dataConversionProcessingItem := psdc.ConversionProcessingItem

	items := make([]*Item, 0)
	for i := range dataProcessingItem {
		items = append(items, &Item{
			OrderID:                          *dataConversionProcessingHeader.ConvertedOrderID,
			OrderItem:                        *dataConversionProcessingItem[i].ConvertedOrderItem,
			OrderItemText:                    dataProcessingItem[i].OrderItemText,
			Product:                          dataConversionProcessingItem[i].ConvertedProduct,
			ProductGroup:                     dataConversionProcessingItem[i].ConvertedProductGroup,
			BaseUnit:                         dataProcessingItem[i].BaseUnit,
			RequestedDeliveryDate:            dataProcessingItem[i].RequestedDeliveryDate,
			RequestedDeliveryTime:            dataProcessingItem[i].RequestedDeliveryTime,
			DeliverToParty:                   dataConversionProcessingItem[i].ConvertedDeliverToParty,
			DeliverFromParty:                 dataConversionProcessingItem[i].ConvertedDeliverFromParty,
			CreationDate:                     dataProcessingItem[i].CreationDate,
			LastChangeDate:                   dataProcessingItem[i].LastChangeDate,
			DeliverFromPlant:                 dataProcessingItem[i].DeliverFromPlant,
			DeliverFromPlantBatch:            dataProcessingItem[i].DeliverFromPlantBatch,
			DeliveryUnit:                     dataProcessingItem[i].DeliveryUnit,
			StockConfirmationBusinessPartner: dataConversionProcessingItem[i].ConvertedStockConfirmationBusinessPartner,
			StockConfirmationPlant:           dataProcessingItem[i].StockConfirmationPlant,
			StockConfirmationPlantBatch:      dataProcessingItem[i].StockConfirmationPlantBatch,
			OrderQuantityInBaseUnit:          dataProcessingItem[i].OrderQuantityInBaseUnit,
			OrderQuantityInDeliveryUnit:      dataProcessingItem[i].OrderQuantityInDeliveryUnit,
			QuantityPerPackage:               dataProcessingItem[i].QuantityPerPackage,
			NetAmount:                        dataProcessingItem[i].NetAmount,
			GrossAmount:                      dataProcessingItem[i].GrossAmount,
			TransactionTaxClassification:     dataConversionProcessingItem[i].ConvertedTransactionTaxClassification,
			PaymentMethod:                    dataConversionProcessingHeader.ConvertedPaymentMethod,
			Project:                          dataConversionProcessingItem[i].ConvertedProject,
			ItemBlockStatus:                  dataProcessingItem[i].ItemBlockStatus,
			ItemDeliveryBlockStatus:          dataProcessingItem[i].ItemDeliveryBlockStatus,
			ItemBillingBlockStatus:           dataProcessingItem[i].ItemBillingBlockStatus,
			IsCancelled:                      dataProcessingItem[i].IsCancelled,
			IsMarkedForDeletion:              dataProcessingItem[i].IsMarkedForDeletion,
		})
	}

	return items
}

func ConvertToItemPricingElement(
	sdc dpfm_api_input_reader.SDC,
	psdc dpfm_api_processing_formatter.ProcessingFormatterSDC,
) []*ItemPricingElement {
	dataProcessingItemPricingElement := psdc.ItemPricingElement
	dataConversionProcessingHeader := psdc.ConversionProcessingHeader
	dataConversionProcessingItem := psdc.ConversionProcessingItem

	dataConversionProcessingItemMap := make(map[string]*dpfm_api_processing_formatter.ConversionProcessingItem, len(dataConversionProcessingItem))
	for _, v := range dataConversionProcessingItem {
		dataConversionProcessingItemMap[*v.ConvertingOrderItem] = v
	}

	itemPricingElements := make([]*ItemPricingElement, 0)
	for i, v := range dataProcessingItemPricingElement {
		if _, ok := dataConversionProcessingItemMap[v.ConvertingOrderItem]; !ok {
			continue
		}

		itemPricingElements = append(itemPricingElements, &ItemPricingElement{
			OrderID:                    *dataConversionProcessingHeader.ConvertedOrderID,
			OrderItem:                  *dataConversionProcessingItemMap[v.ConvertingOrderItem].ConvertedOrderItem,
			Buyer:                      dataConversionProcessingHeader.ConvertedBuyer,
			Seller:                     dataConversionProcessingHeader.ConvertedSeller,
			ConditionRateValue:         dataProcessingItemPricingElement[i].ConditionRateValue,
			ConditionCurrency:          dataProcessingItemPricingElement[i].ConditionCurrency,
			ConditionQuantity:          dataProcessingItemPricingElement[i].ConditionQuantity,
			ConditionQuantityUnit:      dataProcessingItemPricingElement[i].ConditionQuantityUnit,
			ConditionAmount:            dataProcessingItemPricingElement[i].ConditionAmount,
			TransactionCurrency:        dataProcessingItemPricingElement[i].TransactionCurrency,
			ConditionIsManuallyChanged: dataProcessingItemPricingElement[i].ConditionIsManuallyChanged,
		})
	}

	return itemPricingElements
}

func ConvertToItemScheduleLine(
	sdc dpfm_api_input_reader.SDC,
	psdc dpfm_api_processing_formatter.ProcessingFormatterSDC,
) []*ItemScheduleLine {
	dataProcessingItemScheduleLine := psdc.ItemScheduleLine
	dataConversionProcessingHeader := psdc.ConversionProcessingHeader
	dataConversionProcessingItem := psdc.ConversionProcessingItem

	dataConversionProcessingItemMap := make(map[string]*dpfm_api_processing_formatter.ConversionProcessingItem, len(dataConversionProcessingItem))
	for _, v := range dataConversionProcessingItem {
		dataConversionProcessingItemMap[*v.ConvertingOrderItem] = v
	}

	itemScheduleLines := make([]*ItemScheduleLine, 0)
	for i, v := range dataProcessingItemScheduleLine {
		if _, ok := dataConversionProcessingItemMap[v.ConvertingOrderItem]; !ok {
			continue
		}

		itemScheduleLines = append(itemScheduleLines, &ItemScheduleLine{
			OrderID:                           *dataConversionProcessingHeader.ConvertedOrderID,
			OrderItem:                         *dataConversionProcessingItemMap[v.ConvertingOrderItem].ConvertedOrderItem,
			Product:                           dataConversionProcessingItemMap[v.ConvertingOrderItem].ConvertedProduct,
			StockConfirmationBussinessPartner: dataConversionProcessingItemMap[v.ConvertingOrderItem].ConvertedStockConfirmationBusinessPartner,
			StockConfirmationPlantBatch:       dataProcessingItemScheduleLine[i].StockConfirmationPlantBatch,
			RequestedDeliveryDate:             dataProcessingItemScheduleLine[i].RequestedDeliveryDate,
			ScheduleLineOrderQuantity:         dataProcessingItemScheduleLine[i].OriginalOrderQuantityInBaseUnit,
			OriginalOrderQuantityInBaseUnit:   dataProcessingItemScheduleLine[i].OriginalOrderQuantityInBaseUnit,
		})
	}

	return itemScheduleLines
}

func ConvertToAddress(
	sdc dpfm_api_input_reader.SDC,
	psdc dpfm_api_processing_formatter.ProcessingFormatterSDC,
) []*Address {
	dataConversionProcessingHeader := psdc.ConversionProcessingHeader
	dataProcessingAddress := psdc.Address

	addresses := make([]*Address, 0)
	for i := range dataProcessingAddress {
		addresses = append(addresses, &Address{
			OrderID:    *dataConversionProcessingHeader.ConvertedOrderID,
			PostalCode: dataProcessingAddress[i].PostalCode,
		})
	}

	return addresses
}

func ConvertToPartner(
	sdc dpfm_api_input_reader.SDC,
	psdc dpfm_api_processing_formatter.ProcessingFormatterSDC,
) []*Partner {
	dataProcessingPartner := psdc.Partner
	dataConversionProcessingHeader := psdc.ConversionProcessingHeader

	partners := make([]*Partner, 0)
	for i := range dataProcessingPartner {
		partners = append(partners, &Partner{
			OrderID:  *dataConversionProcessingHeader.ConvertedOrderID,
			Currency: dataProcessingPartner[i].Currency,
		})
	}

	return partners
}

func getBoolPtr(b bool) *bool {
	return &b
}
