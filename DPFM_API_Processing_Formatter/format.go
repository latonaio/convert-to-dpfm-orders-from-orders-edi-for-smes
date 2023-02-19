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

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		psdc.Header, e = p.Header(sdc, psdc)
		if e != nil {
			err = e
			return
		}
		psdc.ConversionProcessingHeader, e = p.ConversionProcessingHeader(sdc, psdc)
		if e != nil {
			err = e
			return
		}
		psdc.Item, e = p.Item(sdc, psdc)
		if e != nil {
			err = e
			return
		}
		psdc.ConversionProcessingItem, e = p.ConversionProcessingItem(sdc, psdc)
		if e != nil {
			err = e
			return
		}
		psdc.ItemPricingElement, e = p.ItemPricingElement(sdc, psdc)
		if e != nil {
			err = e
			return
		}
		psdc.ItemScheduleLine, e = p.ItemScheduleLine(sdc, psdc)
		if e != nil {
			err = e
			return
		}
		psdc.Address, e = p.Address(sdc, psdc)
		if e != nil {
			err = e
			return
		}
		psdc.Partner, e = p.Partner(sdc, psdc)
		if e != nil {
			err = e
			return
		}

	}(&wg)

	wg.Wait()
	if err != nil {
		return err
	}

	p.l.Info(psdc)

	return nil
}

func (p *ProcessingFormatter) Header(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) (*Header, error) {
	data := sdc.Header
	dataItem := sdc.Header.Item[0]

	systemDate := getSystemDatePtr()

	res := Header{
		ConvertingOrderID:         data.ExchangedOrdersDocumentIdentifier,
		OrderDate:                 data.ExchangedOrdersDocumentIssueDate,
		ConvertingOrderType:       data.ExchangedOrdersDocumentTypeCode,
		ConvertingBuyer:           data.TradeBuyerIdentifier,
		ConvertingSeller:          data.TradeSellerIdentifier,
		CreationDate:              systemDate,
		LastChangeDate:            systemDate,
		TotalNetAmount:            data.TradeOrdersSettlementMonetarySummationNetTotalAmount,
		TotalTaxAmount:            data.TradeSettlementMonetarySummationTotalTaxAmount,
		TotalGrossAmount:          data.TradeOrdersMonetarySummationIncludingTaxesTotalAmount,
		TransactionCurrency:       data.SupplyChainTradeCurrencyCode,
		RequestedDeliveryDate:     dataItem.SupplyChainEventRequirementOccurrenceDate,
		ConvertingPaymentMethod:   data.TradePaymentTermsTypeCode,
		HeaderText:                data.OrdersDocument,
		HeaderBlockStatus:         getBoolPtr(false),
		HeaderBillingBlockStatus:  getBoolPtr(false),
		HeaderDeliveryBlockStatus: getBoolPtr(false),
		IsCancelled:               getBoolPtr(false),
		IsMarkedForDeletion:       getBoolPtr(false),
	}

	return &res, nil
}

func (p *ProcessingFormatter) ConversionProcessingHeader(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) (*ConversionProcessingHeader, error) {
	dataKey := make([]*ConversionProcessingKey, 0)

	dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "ExchangedOrdersDocumentIdentifier", "OrderID", psdc.Header.ConvertingOrderID))
	dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "ExchangedOrdersDocumentTypeCode", "OrderType", psdc.Header.ConvertingOrderType))

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
	pm.ConvertedOrderType = data["OrderType"].CodeConvertFromString
	pm.ConvertingBuyer = data["Buyer"].CodeConvertFromString
	pm.ConvertedBuyer = data["Buyer"].CodeConvertToInt
	pm.ConvertingSeller = data["Seller"].CodeConvertFromString
	pm.ConvertedSeller = data["Seller"].CodeConvertToInt
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
		ConvertingPaymentMethod: pm.ConvertingPaymentMethod,
		ConvertedPaymentMethod:  pm.ConvertedPaymentMethod,
	}

	return res, nil
}

func (p *ProcessingFormatter) Item(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) ([]*Item, error) {
	res := make([]*Item, 0)
	dataHeader := psdc.Header
	data := sdc.Header.Item

	systemDate := getSystemDatePtr()

	for _, data := range data {

		res = append(res, &Item{
			ConvertingOrderID:                      dataHeader.ConvertingOrderID,
			ConvertingOrderItem:                    data.OrdersDocumentItemlineIdentifier,
			OrderItemText:                          data.NoteOrdersItemContentText,
			Product:                                data.TradeProductIdentifier,
			ConvertingProductGroup:                 data.ProductCharacteristicIdentifier,
			BaseUnit:                               data.QuantityUnitCode,
			RequestedDeliveryDate:                  data.SupplyChainEventRequirementOccurrenceDate,
			ConvertingDeliverToParty:               sdc.Header.TradeShipToPartyIdentifier,
			ConvertingDeliverFromParty:             sdc.Header.TradeShipFromPartyIdentifier,
			CreationDate:                           systemDate,
			LastChangeDate:                         systemDate,
			DeliverFromPlant:                       data.LogisticsLocationIdentification,
			DeliverFromPlantBatch:                  data.TradeProductInstanceBatchIdentifier,
			DeliveryUnit:                           data.ReferencedLogisticsPackageQuantityUnitCode,
			StockConfirmationPlant:                 data.LogisticsLocationIdentification,
			StockConfirmationPlantBatch:            data.TradeProductInstanceBatchIdentifier,
			OrderQuantityInBaseUnit:                data.SupplyChainTradeDeliveryRequestedQuantity,
			OrderQuantityInDeliveryUnit:            data.SupplyChainTradeDeliveryPerPackageUnitQuantity,
			NetAmount:                              data.ItemTradeOrdersSettlementMonetarySummationNetTotalAmount,
			GrossAmount:                            data.ItemTradeOrdersSettlementMonetarySummationIncludingTaxesNetTotalAmount,
			ConvertingTransactionTaxClassification: data.ItemTradeTaxCategoryCode,
			ConvertingPaymentMethod:                dataHeader.ConvertingPaymentMethod,
			ConvertingProject:                      sdc.Header.ProjectIdentifier,
			ItemBlockStatus:                        getBoolPtr(false),
			ItemBillingBlockStatus:                 getBoolPtr(false),
			ItemDeliveryBlockStatus:                getBoolPtr(false),
			IsCancelled:                            getBoolPtr(false),
			IsMarkedForDeletion:                    getBoolPtr(false),
		})
	}

	return res, nil
}

func (p *ProcessingFormatter) ConversionProcessingItem(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) ([]*ConversionProcessingItem, error) {
	data := make([]*ConversionProcessingItem, 0)

	for _, item := range psdc.Item {
		dataKey := make([]*ConversionProcessingKey, 0)

		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "OrdersDocumentItemlineIdentifier", "OrderItem", item.ConvertingOrderItem))
		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "MaterialGroup", "ProductGroup", item.ConvertingProductGroup))
		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeShipToPartyIdentifier", "DeliverToParty", item.ConvertingDeliverToParty))
		dataKey = append(dataKey, p.ConversionProcessingKey(sdc, "TradeShipFromPartyIdentifier", "DeliverFromParty", item.ConvertingDeliverFromParty))
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
	pm.ConvertingProductGroup = data["ProductGroup"].CodeConvertFromString
	pm.ConvertedProductGroup = data["ProductGroup"].CodeConvertFromString
	pm.ConvertingDeliverToParty = data["DeliverToParty"].CodeConvertFromString
	pm.ConvertedDeliverToParty = data["DeliverToParty"].CodeConvertToInt
	pm.ConvertingDeliverFromParty = data["DeliverFromParty"].CodeConvertFromString
	pm.ConvertedDeliverFromParty = data["DeliverFromParty"].CodeConvertToInt
	pm.ConvertingTransactionTaxClassification = data["TransactionTaxClassification"].CodeConvertFromString
	pm.ConvertedTransactionTaxClassification = data["TransactionTaxClassification"].CodeConvertFromString
	pm.ConvertingProject = data["Project"].CodeConvertFromString
	pm.ConvertedProject = data["Project"].CodeConvertFromString

	res := &ConversionProcessingItem{
		ConvertingOrderItem:                    pm.ConvertingOrderItem,
		ConvertedOrderItem:                     pm.ConvertedOrderItem,
		ConvertingProductGroup:                 pm.ConvertingProductGroup,
		ConvertedProductGroup:                  pm.ConvertedProductGroup,
		ConvertingDeliverToParty:               pm.ConvertingDeliverToParty,
		ConvertedDeliverToParty:                pm.ConvertedDeliverToParty,
		ConvertingDeliverFromParty:             pm.ConvertingDeliverFromParty,
		ConvertedDeliverFromParty:              pm.ConvertedDeliverFromParty,
		ConvertingTransactionTaxClassification: pm.ConvertingTransactionTaxClassification,
		ConvertedTransactionTaxClassification:  pm.ConvertedTransactionTaxClassification,
		ConvertingProject:                      pm.ConvertingProject,
		ConvertedProject:                       pm.ConvertedProject,
	}

	return res, nil
}

func (p *ProcessingFormatter) ItemPricingElement(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) ([]*ItemPricingElement, error) {
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

	return res, nil
}

func (p *ProcessingFormatter) ItemScheduleLine(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) ([]*ItemScheduleLine, error) {
	res := make([]*ItemScheduleLine, 0)
	dataHeader := psdc.Header
	dataItem := psdc.Item
	data := sdc.Header.Item

	for _, dataItem := range dataItem {
		for _, data := range data {

			res = append(res, &ItemScheduleLine{
				ConvertingOrderID:                 dataHeader.ConvertingOrderID,
				ConvertingOrderItem:               dataItem.ConvertingOrderItem,
				Product:                           data.TradeProductBuyerAssignedIdentifier,
				StockConfirmationBussinessPartner: sdc.BusinessPartnerID,
				StockConfirmationPlantBatch:       data.TradeProductInstanceBatchIdentifier,
				RequestedDeliveryDate:             data.SupplyChainEventRequirementOccurrenceDate,
				OriginalOrderQuantityInBaseUnit:   data.SupplyChainTradeDeliveryRequestedQuantity,
			})
		}
	}

	return res, nil
}

func (p *ProcessingFormatter) Address(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) ([]*Address, error) {
	res := make([]*Address, 0)
	dataHeader := psdc.Header

	res = append(res, &Address{
		ConvertingOrderID: dataHeader.ConvertingOrderID,
		PostalCode:        sdc.Header.BuyerAddressPostalCode,
	})

	return res, nil
}

func (p *ProcessingFormatter) Partner(sdc *dpfm_api_input_reader.SDC, psdc *ProcessingFormatterSDC) ([]*Partner, error) {
	res := make([]*Partner, 0)
	dataHeader := psdc.Header

	for range psdc.Partner {
		res = append(res, &Partner{
			ConvertingOrderID: dataHeader.ConvertingOrderID,
			Currency:          dataHeader.TransactionCurrency,
		})
	}
	return res, nil
}
