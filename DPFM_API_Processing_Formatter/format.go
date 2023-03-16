package dpfm_api_processing_formatter

import (
	"context"
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

	p.appendDataKey(&dataKey, sdc, "ExchangedOrdersDocumentIdentifier", "OrderID", psdc.Header.ConvertingOrderID)
	p.appendDataKey(&dataKey, sdc, "ExchangedOrdersDocumentTypeCode", "OrderType", psdc.Header.ConvertingOrderType)
	p.appendDataKey(&dataKey, sdc, "TradeBuyerIdentifier", "Buyer", psdc.Header.ConvertingBuyer)
	p.appendDataKey(&dataKey, sdc, "TradeSellerIdentifier", "Seller", psdc.Header.ConvertingSeller)
	p.appendDataKey(&dataKey, sdc, "TradeBuyerIdentifier", "BillToParty", psdc.Header.ConvertingBillToParty)
	p.appendDataKey(&dataKey, sdc, "TradeSellerIdentifier", "BillFromParty", psdc.Header.ConvertingBillFromParty)
	p.appendDataKey(&dataKey, sdc, "TradeBuyerIdentifier", "Payer", psdc.Header.ConvertingPayer)
	p.appendDataKey(&dataKey, sdc, "TradeSellerIdentifier", "Payee", psdc.Header.ConvertingPayee)
	p.appendDataKey(&dataKey, sdc, "TradePaymentTermsTypeCode", "PaymentMethod", psdc.Header.ConvertingPaymentMethod)

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
			return nil, xerrors.Errorf("Value of %s is not in the database", v.LabelConvertTo)
		}
	}

	res := &ConversionProcessingHeader{}

	if _, ok := data["OrderID"]; ok {
		res.ConvertingOrderID = data["OrderID"].CodeConvertFromString
		res.ConvertedOrderID = data["OrderID"].CodeConvertToInt
	}
	if _, ok := data["OrderType"]; ok {
		res.ConvertingOrderType = data["OrderType"].CodeConvertFromString
		res.ConvertedOrderType = data["OrderType"].CodeConvertToString
	}
	if _, ok := data["Buyer"]; ok {
		res.ConvertingBuyer = data["Buyer"].CodeConvertFromString
		res.ConvertedBuyer = data["Buyer"].CodeConvertToInt
	}
	if _, ok := data["Seller"]; ok {
		res.ConvertingSeller = data["Seller"].CodeConvertFromString
		res.ConvertedSeller = data["Seller"].CodeConvertToInt
	}
	if _, ok := data["BillToParty"]; ok {
		res.ConvertingBillToParty = data["BillToParty"].CodeConvertFromString
		res.ConvertedBillToParty = data["BillToParty"].CodeConvertToInt
	}
	if _, ok := data["BillFromParty"]; ok {
		res.ConvertingBillFromParty = data["BillFromParty"].CodeConvertFromString
		res.ConvertedBillFromParty = data["BillFromParty"].CodeConvertToInt
	}
	if _, ok := data["Payer"]; ok {
		res.ConvertingPayer = data["Payer"].CodeConvertFromString
		res.ConvertedPayer = data["Payer"].CodeConvertToInt
	}
	if _, ok := data["Payee"]; ok {
		res.ConvertingPayee = data["Payee"].CodeConvertFromString
		res.ConvertedPayee = data["Payee"].CodeConvertToInt
	}
	if _, ok := data["PaymentMethod"]; ok {
		res.ConvertingPaymentMethod = data["PaymentMethod"].CodeConvertFromString
		res.ConvertedPaymentMethod = data["PaymentMethod"].CodeConvertToString
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

		p.appendDataKey(&dataKey, sdc, "OrdersDocumentItemlineIdentifier", "OrderItem", item.ConvertingOrderItem)
		p.appendDataKey(&dataKey, sdc, "TradeProductIdentifier", "Product", item.ConvertingProduct)
		p.appendDataKey(&dataKey, sdc, "ProductCharacteristicIdentifier", "ProductGroup", item.ConvertingProductGroup)
		p.appendDataKey(&dataKey, sdc, "TradeShipToPartyIdentifier", "DeliverToParty", item.ConvertingDeliverToParty)
		p.appendDataKey(&dataKey, sdc, "TradeShipFromPartyIdentifier", "DeliverFromParty", item.ConvertingDeliverFromParty)
		p.appendDataKey(&dataKey, sdc, "TradeShipFromPartyIdentifier", "StockConfirmationBusinessPartner", item.ConvertingStockConfirmationBusinessPartner)
		p.appendDataKey(&dataKey, sdc, "ItemTradeTaxCategoryCode", "TransactionTaxClassification", item.ConvertingTransactionTaxClassification)
		p.appendDataKey(&dataKey, sdc, "ProjectIdentifier", "Project", item.ConvertingProject)

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
			return nil, xerrors.Errorf("Value of %s is not in the database", v.LabelConvertTo)
		}
	}

	res := &ConversionProcessingItem{}

	if _, ok := data["OrderItem"]; ok {
		res.ConvertingOrderItem = data["OrderItem"].CodeConvertFromString
		res.ConvertedOrderItem = data["OrderItem"].CodeConvertToInt
	}
	if _, ok := data["Product"]; ok {
		res.ConvertingProduct = data["Product"].CodeConvertFromString
		res.ConvertedProduct = data["Product"].CodeConvertToString
	}
	if _, ok := data["ProductGroup"]; ok {
		res.ConvertingProductGroup = data["ProductGroup"].CodeConvertFromString
		res.ConvertedProductGroup = data["ProductGroup"].CodeConvertToString
	}
	if _, ok := data["DeliverToParty"]; ok {
		res.ConvertingDeliverToParty = data["DeliverToParty"].CodeConvertFromString
		res.ConvertedDeliverToParty = data["DeliverToParty"].CodeConvertToInt
	}
	if _, ok := data["DeliverFromParty"]; ok {
		res.ConvertingDeliverFromParty = data["DeliverFromParty"].CodeConvertFromString
		res.ConvertedDeliverFromParty = data["DeliverFromParty"].CodeConvertToInt
	}
	if _, ok := data["StockConfirmationBusinessPartner"]; ok {
		res.ConvertingStockConfirmationBusinessPartner = data["StockConfirmationBusinessPartner"].CodeConvertFromString
		res.ConvertedStockConfirmationBusinessPartner = data["StockConfirmationBusinessPartner"].CodeConvertToInt
	}
	if _, ok := data["TransactionTaxClassification"]; ok {
		res.ConvertingTransactionTaxClassification = data["TransactionTaxClassification"].CodeConvertFromString
		res.ConvertedTransactionTaxClassification = data["TransactionTaxClassification"].CodeConvertFromString
	}
	if _, ok := data["Project"]; ok {
		res.ConvertingProject = data["Project"].CodeConvertFromString
		res.ConvertedProject = data["Project"].CodeConvertFromString
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

func (p *ProcessingFormatter) appendDataKey(dataKey *[]*ConversionProcessingKey, sdc *dpfm_api_input_reader.SDC, labelConvertFrom string, labelConvertTo string, codeConvertFrom any) {
	switch v := codeConvertFrom.(type) {
	case int, float32:
	case string:
		if v == "" {
			return
		}
	case *int, *float32:
		if v == nil {
			return
		}
	case *string:
		if v == nil || *v == "" {
			return
		}
	default:
		return
	}
	*dataKey = append(*dataKey, p.ConversionProcessingKey(sdc, labelConvertFrom, labelConvertTo, codeConvertFrom))
}

func postalCodeContains(postalCode *string, addresses []*Address) bool {
	for _, address := range addresses {
		if address.PostalCode == nil || postalCode == nil {
			return true
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
