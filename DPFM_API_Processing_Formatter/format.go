package dpfm_api_processing_formatter

import (
	"context"
	"convert-to-dpfm-orders-from-orders-edi-for-smes/DPFM_API_Caller/requests"
	dpfm_api_input_reader "convert-to-dpfm-orders-from-orders-edi-for-smes/DPFM_API_Input_Reader"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	"golang.org/x/xerrors"
)

type ProcessingFormatter struct {
	ctx context.Context
	db  *database.Mysql
	l   *logger.Logger
}

func NewProcessingFormatter(ctx context.Context, db *database.Mysql, l *logger.Logger) *ProcessingFormatter {
	return &ProcessingFormatter{
		ctx: ctx,
		db:  db,
		l:   l,
	}
}

func (p *ProcessingFormatter) ProcessingFormatter(
	sdc *dpfm_api_input_reader.SDC,
	psdc *ProcessingFormatterSDC,
) error {
	var err error
	var e error

	if bpIDIsNull(sdc) {
		return xerrors.New("business_partner is null")
	}

	wg := sync.WaitGroup{}

	psdc.Header = p.Header(sdc, psdc)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		// Ref: Header
		psdc.ConversionProcessingHeader, e = p.ConversionProcessingHeader(sdc, psdc)
		if e != nil {
			err = e
			return
		}
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		// Ref: Header
		psdc.Item = p.Item(sdc, psdc)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			// Ref: Item
			psdc.ConversionProcessingItem, e = p.ConversionProcessingItem(sdc, psdc)
			if e != nil {
				err = e
				return
			}
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			// Ref: Header, Item
			psdc.ItemPricingElement = p.ItemPricingElement(sdc, psdc)
		}(wg)

		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			// Ref: Header, Item
			psdc.ItemScheduleLine = p.ItemScheduleLine(sdc, psdc)
		}(wg)
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		// Ref: Header
		psdc.Address = p.Address(sdc, psdc)
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		// Ref: Header
		psdc.Partner = p.Partner(sdc, psdc)
	}(&wg)

	wg.Wait()
	if err != nil {
		return err
	}

	p.l.Info(psdc)

	return nil
}

func (p *ProcessingFormatter) Header(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) *Header {
	data := sdc.Header
	dataItem := sdc.Header.Item[0]

	systemDate := getSystemDatePtr()

	res := Header{
		ConvertingOrderID:         data.ExchangedOrdersDocumentIdentifier,
		OrderDate:                 data.ExchangedOrdersDocumentIssueDate,
		ConvertingOrderType:       data.ExchangedOrdersDocumentTypeCode,
		ConvertingBuyer:           data.TradeBuyerIdentifier,
		ConvertingSeller:          data.TradeSellerIdentifier,
		ConvertingBillToParty:     data.TradeBuyerIdentifier,
		ConvertingBillFromParty:   data.TradeSellerIdentifier,
		ConvertingPayer:           data.TradeBuyerIdentifier,
		ConvertingPayee:           data.TradeSellerIdentifier,
		CreationDate:              systemDate,
		LastChangeDate:            systemDate,
		TotalNetAmount:            data.TradeOrdersSettlementMonetarySummationNetTotalAmount,
		TotalTaxAmount:            data.TradeSettlementMonetarySummationTotalTaxAmount,
		TotalGrossAmount:          data.TradeOrdersMonetarySummationIncludingTaxesTotalAmount,
		TransactionCurrency:       data.SupplyChainTradeCurrencyCode,
		RequestedDeliveryDate:     dataItem.SupplyChainEventRequirementOccurrenceDate,
		RequestedDeliveryTime:     dataItem.SupplyChainEventRequirementOccurrenceTime,
		ConvertingPaymentMethod:   data.TradePaymentTermsTypeCode,
		HeaderText:                data.OrdersDocument,
		HeaderBlockStatus:         getBoolPtr(false),
		HeaderBillingBlockStatus:  getBoolPtr(false),
		HeaderDeliveryBlockStatus: getBoolPtr(false),
		IsCancelled:               getBoolPtr(false),
		IsMarkedForDeletion:       getBoolPtr(false),
		ConvertingStockConfirmationBusinessPartner: data.TradeShipFromPartyIdentifier,
	}

	return &res
}

func (p *ProcessingFormatter) ConversionProcessingHeader(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) (*ConversionProcessingHeader, error) {
	dataKey := make([]*ConversionProcessingKey, 0)

	dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "ExchangedOrdersDocumentIdentifier", "OrderID", psdc.Header.ConvertingOrderID))
	dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "ExchangedOrdersDocumentTypeCode", "OrderType", psdc.Header.ConvertingOrderType))
	dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeBuyerIdentifier", "Buyer", psdc.Header.ConvertingBuyer))
	dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeSellerIdentifier", "Seller", psdc.Header.ConvertingSeller))
	dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeBuyerIdentifier", "BillToParty", psdc.Header.ConvertingBillToParty))
	dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeSellerIdentifier", "BillFromParty", psdc.Header.ConvertingBillFromParty))
	dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeBuyerIdentifier", "Payer", psdc.Header.ConvertingPayer))
	dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeSellerIdentifier", "Payee", psdc.Header.ConvertingPayee))
	dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradePaymentTermsTypeCode", "PaymentMethod", psdc.Header.ConvertingPaymentMethod))

	dataQueryGets, err := p.ConversionProcessingCommonQueryGets(dataKey)
	if err != nil {
		return nil, xerrors.Errorf("ConversionProcessing Error: %w", err)
	}

	data, err := p.ConvertToConversionProcessingHeader(dataKey, dataQueryGets)
	if err != nil {
		return nil, xerrors.Errorf("ConvertToConversionProcessing Error: %w", err)
	}

	return data, nil
}

func (psdc *ProcessingFormatter) ConvertToConversionProcessingHeader(conversionProcessingKey []*ConversionProcessingKey, conversionProcessingCommonQueryGets []*ConversionProcessingCommonQueryGets) (*ConversionProcessingHeader, error) {
	data := make(map[string]*ConversionProcessingCommonQueryGets, len(conversionProcessingCommonQueryGets))
	for _, v := range conversionProcessingCommonQueryGets {
		data[v.LabelConvertTo] = v
	}

	for _, v := range conversionProcessingKey {
		if _, ok := data[v.LabelConvertTo]; !ok {
			return nil, xerrors.Errorf("%s is not in the database", v.LabelConvertTo)
		}
	}

	pm := &requests.ConversionProcessingHeader{}

	pm.ConvertingOrderID = data["OrderID"].CodeConvertFromString
	pm.ConvertedOrderID = data["OrderID"].CodeConvertToInt
	pm.ConvertingOrderType = data["OrderType"].CodeConvertFromString
	pm.ConvertedOrderType = data["OrderType"].CodeConvertToString
	pm.ConvertingBuyer = data["Buyer"].CodeConvertFromString
	pm.ConvertedBuyer = data["Buyer"].CodeConvertToInt
	pm.ConvertingSeller = data["Seller"].CodeConvertFromString
	pm.ConvertedSeller = data["Seller"].CodeConvertToInt
	pm.ConvertingBillToParty = data["BillToParty"].CodeConvertFromString
	pm.ConvertedBillToParty = data["BillToParty"].CodeConvertToInt
	pm.ConvertingBillFromParty = data["BillFromParty"].CodeConvertFromString
	pm.ConvertedBillFromParty = data["BillFromParty"].CodeConvertToInt
	pm.ConvertingPayer = data["Payer"].CodeConvertFromString
	pm.ConvertedPayer = data["Payer"].CodeConvertToInt
	pm.ConvertingPayee = data["Payee"].CodeConvertFromString
	pm.ConvertedPayee = data["Payee"].CodeConvertToInt
	pm.ConvertingPaymentMethod = data["PaymentMethod"].CodeConvertFromString
	pm.ConvertedPaymentMethod = data["PaymentMethod"].CodeConvertToString

	res := &ConversionProcessingHeader{
		ConvertingOrderID:       pm.ConvertingOrderID,
		ConvertedOrderID:        pm.ConvertedOrderID,
		ConvertingOrderType:     pm.ConvertingOrderType,
		ConvertedOrderType:      pm.ConvertedOrderType,
		ConvertingBuyer:         pm.ConvertingBuyer,
		ConvertedBuyer:          pm.ConvertedBuyer,
		ConvertingSeller:        pm.ConvertingSeller,
		ConvertedSeller:         pm.ConvertedSeller,
		ConvertingBillToParty:   pm.ConvertingBillToParty,
		ConvertedBillToParty:    pm.ConvertedBillToParty,
		ConvertingBillFromParty: pm.ConvertingBillFromParty,
		ConvertedBillFromParty:  pm.ConvertedBillFromParty,
		ConvertingPayer:         pm.ConvertingPayer,
		ConvertedPayer:          pm.ConvertedPayer,
		ConvertingPayee:         pm.ConvertingPayee,
		ConvertedPayee:          pm.ConvertedPayee,
		ConvertingPaymentMethod: pm.ConvertingPaymentMethod,
		ConvertedPaymentMethod:  pm.ConvertedPaymentMethod,
	}

	return res, nil
}

func (p *ProcessingFormatter) Item(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) []*Item {
	res := make([]*Item, 0)
	dataHeader := psdc.Header
	data := sdc.Header.Item

	systemDate := getSystemDatePtr()

	for _, data := range data {

		res = append(res, &Item{
			ConvertingOrderID:          dataHeader.ConvertingOrderID,
			ConvertingOrderItem:        data.OrdersDocumentItemlineIdentifier,
			OrderItemText:              data.NoteOrdersItemContentText,
			ConvertingProduct:          data.TradeProductIdentifier,
			ConvertingProductGroup:     data.ProductCharacteristicIdentifier,
			BaseUnit:                   data.QuantityUnitCode,
			RequestedDeliveryDate:      data.SupplyChainEventRequirementOccurrenceDate,
			RequestedDeliveryTime:      data.SupplyChainEventRequirementOccurrenceTime,
			ConvertingDeliverToParty:   sdc.Header.TradeShipToPartyIdentifier,
			ConvertingDeliverFromParty: sdc.Header.TradeShipFromPartyIdentifier,
			CreationDate:               systemDate,
			LastChangeDate:             systemDate,
			DeliverFromPlant:           data.LogisticsLocationIdentification,
			DeliverFromPlantBatch:      data.TradeProductInstanceBatchIdentifier,
			DeliveryUnit:               data.ReferencedLogisticsPackageQuantityUnitCode,
			ConvertingStockConfirmationBusinessPartner: dataHeader.ConvertingStockConfirmationBusinessPartner,
			StockConfirmationPlant:                     data.LogisticsLocationIdentification,
			StockConfirmationPlantBatch:                data.TradeProductInstanceBatchIdentifier,
			OrderQuantityInBaseUnit:                    data.SupplyChainTradeDeliveryRequestedQuantity,
			OrderQuantityInDeliveryUnit:                data.SupplyChainTradeDeliveryPerPackageUnitQuantity,
			NetAmount:                                  data.ItemTradeOrdersSettlementMonetarySummationNetTotalAmount,
			GrossAmount:                                data.ItemTradeOrdersSettlementMonetarySummationIncludingTaxesNetTotalAmount,
			ConvertingTransactionTaxClassification:     data.ItemTradeTaxCategoryCode,
			ConvertingPaymentMethod:                    dataHeader.ConvertingPaymentMethod,
			ConvertingProject:                          sdc.Header.ProjectIdentifier,
			ItemBlockStatus:                            getBoolPtr(false),
			ItemBillingBlockStatus:                     getBoolPtr(false),
			ItemDeliveryBlockStatus:                    getBoolPtr(false),
			IsCancelled:                                getBoolPtr(false),
			IsMarkedForDeletion:                        getBoolPtr(false),
		})
	}

	return res
}

func (p *ProcessingFormatter) ConversionProcessingItem(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) ([]*ConversionProcessingItem, error) {
	data := make([]*ConversionProcessingItem, 0)

	for _, item := range psdc.Item {
		dataKey := make([]*ConversionProcessingKey, 0)

		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "OrdersDocumentItemlineIdentifier", "OrderItem", item.ConvertingOrderItem))
		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeProductIdentifier", "Product", item.ConvertingProduct))
		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "ProductCharacteristicIdentifier", "ProductGroup", item.ConvertingProductGroup))
		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeShipToPartyIdentifier", "DeliverToParty", item.ConvertingDeliverToParty))
		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeShipFromPartyIdentifier", "DeliverFromParty", item.ConvertingDeliverFromParty))
		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeShipFromPartyIdentifier", "StockConfirmationBusinessPartner", item.ConvertingStockConfirmationBusinessPartner))
		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "ItemTradeTaxCategoryCode", "TransactionTaxClassification", item.ConvertingTransactionTaxClassification))
		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "ProjectIdentifier", "Project", item.ConvertingProject))

		dataQueryGets, err := p.ConversionProcessingCommonQueryGets(dataKey)
		if err != nil {
			return nil, xerrors.Errorf("ConversionProcessing Error: %w", err)
		}

		datum, err := p.ConvertToConversionProcessingItem(dataKey, dataQueryGets)
		if err != nil {
			return nil, xerrors.Errorf("ConvertToConversionProcessing Error: %w", err)
		}

		data = append(data, datum)
	}

	return data, nil
}

func (p *ProcessingFormatter) ConvertToConversionProcessingItem(conversionProcessingKey []*ConversionProcessingKey, conversionProcessingCommonQueryGets []*ConversionProcessingCommonQueryGets) (*ConversionProcessingItem, error) {
	data := make(map[string]*ConversionProcessingCommonQueryGets, len(conversionProcessingCommonQueryGets))
	for _, v := range conversionProcessingCommonQueryGets {
		data[v.LabelConvertTo] = v
	}

	for _, v := range conversionProcessingKey {
		if _, ok := data[v.LabelConvertTo]; !ok {
			return nil, xerrors.Errorf("%s is not in the database", v.LabelConvertTo)
		}
	}

	pm := &requests.ConversionProcessingItem{}

	pm.ConvertingOrderItem = data["OrderItem"].CodeConvertFromString
	pm.ConvertedOrderItem = data["OrderItem"].CodeConvertToInt
	pm.ConvertingProduct = data["Product"].CodeConvertFromString
	pm.ConvertedProduct = data["Product"].CodeConvertToString
	pm.ConvertingProductGroup = data["ProductGroup"].CodeConvertFromString
	pm.ConvertedProductGroup = data["ProductGroup"].CodeConvertToString
	pm.ConvertingDeliverToParty = data["DeliverToParty"].CodeConvertFromString
	pm.ConvertedDeliverToParty = data["DeliverToParty"].CodeConvertToInt
	pm.ConvertingDeliverFromParty = data["DeliverFromParty"].CodeConvertFromString
	pm.ConvertedDeliverFromParty = data["DeliverFromParty"].CodeConvertToInt
	pm.ConvertingStockConfirmationBusinessPartner = data["StockConfirmationBusinessPartner"].CodeConvertFromString
	pm.ConvertedStockConfirmationBusinessPartner = data["StockConfirmationBusinessPartner"].CodeConvertToInt
	pm.ConvertingTransactionTaxClassification = data["TransactionTaxClassification"].CodeConvertFromString
	pm.ConvertedTransactionTaxClassification = data["TransactionTaxClassification"].CodeConvertFromString
	pm.ConvertingProject = data["Project"].CodeConvertFromString
	pm.ConvertedProject = data["Project"].CodeConvertFromString

	res := &ConversionProcessingItem{
		ConvertingOrderItem:                        pm.ConvertingOrderItem,
		ConvertedOrderItem:                         pm.ConvertedOrderItem,
		ConvertingProduct:                          pm.ConvertingProduct,
		ConvertedProduct:                           pm.ConvertedProduct,
		ConvertingProductGroup:                     pm.ConvertingProductGroup,
		ConvertedProductGroup:                      pm.ConvertedProductGroup,
		ConvertingDeliverToParty:                   pm.ConvertingDeliverToParty,
		ConvertedDeliverToParty:                    pm.ConvertedDeliverToParty,
		ConvertingDeliverFromParty:                 pm.ConvertingDeliverFromParty,
		ConvertedDeliverFromParty:                  pm.ConvertedDeliverFromParty,
		ConvertingStockConfirmationBusinessPartner: pm.ConvertingStockConfirmationBusinessPartner,
		ConvertedStockConfirmationBusinessPartner:  pm.ConvertedStockConfirmationBusinessPartner,
		ConvertingTransactionTaxClassification:     pm.ConvertingTransactionTaxClassification,
		ConvertedTransactionTaxClassification:      pm.ConvertedTransactionTaxClassification,
		ConvertingProject:                          pm.ConvertingProject,
		ConvertedProject:                           pm.ConvertedProject,
	}

	return res, nil
}

func (p *ProcessingFormatter) ItemPricingElement(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) []*ItemPricingElement {
	res := make([]*ItemPricingElement, 0)
	dataHeader := psdc.Header
	dataItem := psdc.Item
	data := sdc.Header.Item

	for _, dataItem := range dataItem {
		for _, data := range data {

			res = append(res, &ItemPricingElement{
				ConvertingOrderID:          dataHeader.ConvertingOrderID,
				ConvertingOrderItem:        dataItem.ConvertingOrderItem,
				ConvertingBuyer:            dataHeader.ConvertingBuyer,
				ConvertingSeller:           dataHeader.ConvertingSeller,
				ConditionRateValue:         data.TradeOrdersPriceChargeAmount,
				ConditionCurrency:          sdc.Header.SupplyChainTradeCurrencyCode,
				ConditionQuantity:          data.TradePriceBasisQuantity,
				ConditionQuantityUnit:      data.TradePriceBasisUnitCode,
				ConditionAmount:            data.ItemTradeTaxGrandTotalAmount,
				TransactionCurrency:        sdc.Header.SupplyChainTradeCurrencyCode,
				ConditionIsManuallyChanged: getBoolPtr(true),
			})
		}
	}

	return res
}

func (p *ProcessingFormatter) ItemScheduleLine(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) []*ItemScheduleLine {
	res := make([]*ItemScheduleLine, 0)
	dataHeader := psdc.Header
	dataItem := psdc.Item
	data := sdc.Header.Item

	for _, dataItem := range dataItem {
		for _, data := range data {

			res = append(res, &ItemScheduleLine{
				ConvertingOrderID:   dataHeader.ConvertingOrderID,
				ConvertingOrderItem: dataItem.ConvertingOrderItem,
				Product:             data.TradeProductBuyerAssignedIdentifier,
				ConvertingStockConfirmationBussinessPartner: dataItem.ConvertingStockConfirmationBusinessPartner,
				StockConfirmationPlantBatch:                 data.TradeProductInstanceBatchIdentifier,
				RequestedDeliveryDate:                       data.SupplyChainEventRequirementOccurrenceDate,
				OriginalOrderQuantityInBaseUnit:             data.SupplyChainTradeDeliveryRequestedQuantity,
			})
		}
	}

	return res
}

func (p *ProcessingFormatter) Address(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) []*Address {
	res := make([]*Address, 0)

	buyerAddress := buyerAddress(sdc, psdc)
	if !postalCodeContains(buyerAddress.PostalCode, res) {
		res = append(res, buyerAddress)
	}

	sellerAddress := sellerAddress(sdc, psdc)
	if !postalCodeContains(sellerAddress.PostalCode, res) {
		res = append(res, sellerAddress)
	}

	deliverToPartyAddress := deliverToPartyAddress(sdc, psdc)
	if !postalCodeContains(deliverToPartyAddress.PostalCode, res) {
		res = append(res, deliverToPartyAddress)
	}

	// deliverFromPartyAddress := deliverFromPartyAddress(sdc, psdc)
	// if !postalCodeContains(deliverFromPartyAddress.PostalCode, res) {
	// 	res = append(res, deliverFromPartyAddress)
	// }

	return res
}

func buyerAddress(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) *Address {
	dataHeader := psdc.Header

	res := &Address{
		ConvertingOrderID: dataHeader.ConvertingOrderID,
		PostalCode:        sdc.Header.BuyerAddressPostalCode,
	}

	return res
}

func sellerAddress(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) *Address {
	dataHeader := psdc.Header

	res := &Address{
		ConvertingOrderID: dataHeader.ConvertingOrderID,
		PostalCode:        sdc.Header.SellerAddressPostalCode,
	}

	return res
}

func deliverToPartyAddress(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) *Address {
	dataHeader := psdc.Header

	res := &Address{
		ConvertingOrderID: dataHeader.ConvertingOrderID,
		PostalCode:        sdc.Header.ShipToPartyAddressPostalCode,
	}

	return res
}

// func deliverFromPartyAddress(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) *Address {
// 	dataHeader := psdc.Header

// 	res := &Address{
// 		ConvertingOrderID: dataHeader.ConvertingOrderID,
// 		PostalCode:        sdc.Header.ShipFromPartyAddressPostalCode,
// 	}

// 	return res
// }

func (p *ProcessingFormatter) Partner(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) []*Partner {
	res := make([]*Partner, 0)
	dataHeader := psdc.Header

	res = append(res, &Partner{
		ConvertingOrderID: dataHeader.ConvertingOrderID,
		Currency:          dataHeader.TransactionCurrency,
	})

	return res
}

func postalCodeContains(postalCode *string, addresses []*Address) bool {
	for _, address := range addresses {
		if address.PostalCode == nil || postalCode == nil {
			continue
		}
		if *address.PostalCode == *postalCode {
			return true
		}
	}

	return false
}

func bpIDIsNull(sdc *dpfm_api_input_reader.SDC) bool {
	return sdc.BusinessPartnerID == nil
}
