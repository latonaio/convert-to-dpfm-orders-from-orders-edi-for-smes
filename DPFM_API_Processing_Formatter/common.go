package dpfm_api_processing_formatter

import (
	"convert-to-dpfm-orders-from-orders-edi-for-smes/DPFM_API_Caller/requests"
	dpfm_api_input_reader "convert-to-dpfm-orders-from-orders-edi-for-smes/DPFM_API_Input_Reader"
	"database/sql"
	"strings"

	"fmt"
)

func (p *ProcessingFormatter) ConversionProcessingKey(sdc *dpfm_api_input_reader.SDC, labelConvertFrom, labelConvertTo string, codeConvertFrom any) *ConversionProcessingKey {
	pm := &requests.ConversionProcessingKey{
		SystemConvertTo:   "DPFM",
		SystemConvertFrom: "EDIForSMEs",
		BusinessPartner:   *sdc.BusinessPartnerID,
	}

	pm.LabelConvertFrom = labelConvertFrom
	pm.LabelConvertTo = labelConvertTo

	switch codeConvertFrom := codeConvertFrom.(type) {
	case int:
		pm.CodeConvertFromInt = &codeConvertFrom
	case float32:
		pm.CodeConvertFromFloat = &codeConvertFrom
	case string:
		pm.CodeConvertFromString = &codeConvertFrom
	case *int:
		if codeConvertFrom != nil {
			pm.CodeConvertFromInt = codeConvertFrom
		}
	case *float32:
		if codeConvertFrom != nil {
			pm.CodeConvertFromFloat = codeConvertFrom
		}
	case *string:
		if codeConvertFrom != nil {
			pm.CodeConvertFromString = codeConvertFrom
		}
	}

	data := pm
	res := ConversionProcessingKey{
		SystemConvertTo:       data.SystemConvertTo,
		SystemConvertFrom:     data.SystemConvertFrom,
		LabelConvertTo:        data.LabelConvertTo,
		LabelConvertFrom:      data.LabelConvertFrom,
		CodeConvertFromInt:    data.CodeConvertFromInt,
		CodeConvertFromFloat:  data.CodeConvertFromFloat,
		CodeConvertFromString: data.CodeConvertFromString,
		BusinessPartner:       data.BusinessPartner,
	}

	return &res
}

func (p *ProcessingFormatter) ConversionProcessingCommonQueryGets(dataKey []*ConversionProcessingKey) ([]*ConversionProcessingCommonQueryGets, error) {
	var res []*ConversionProcessingCommonQueryGets
	var err error

	res, err = p.CodeConversionFromInt(dataKey, res)
	if err != nil {
		return nil, err
	}

	res, err = p.CodeConversionFromFloat(dataKey, res)
	if err != nil {
		return nil, err
	}

	res, err = p.CodeConversionFromString(dataKey, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (p *ProcessingFormatter) CodeConversionFromInt(dataKey []*ConversionProcessingKey, res []*ConversionProcessingCommonQueryGets) ([]*ConversionProcessingCommonQueryGets, error) {
	var args []interface{}

	length := 0
	for _, v := range dataKey {
		if v.CodeConvertFromInt != nil {
			args = append(args, v.SystemConvertTo, v.SystemConvertFrom, v.LabelConvertTo, v.LabelConvertFrom, v.CodeConvertFromInt, v.BusinessPartner)
			length++
		}
	}
	if length == 0 {
		return nil, nil
	}
	repeat := strings.Repeat("(?,?,?,?,?,?),", length-1) + "(?,?,?,?,?,?)"

	rows, err := p.db.Query(
		`SELECT CodeConversionID, SystemConvertTo, SystemConvertFrom, LabelConvertTo, LabelConvertFrom,
		CodeConvertFromInt, CodeConvertFromFloat, CodeConvertFromString, CodeConvertToInt, CodeConvertToFloat, CodeConvertToString, BusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_code_conversion_code_conversion_data
		WHERE (SystemConvertTo, SystemConvertFrom, LabelConvertTo, LabelConvertFrom, CodeConvertFromInt, BusinessPartner) IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res, err = p.ConvertToCodeConversionQueryGets(rows, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (p *ProcessingFormatter) CodeConversionFromFloat(dataKey []*ConversionProcessingKey, res []*ConversionProcessingCommonQueryGets) ([]*ConversionProcessingCommonQueryGets, error) {
	var args []interface{}

	length := 0
	for _, v := range dataKey {
		if v.CodeConvertFromFloat != nil {
			args = append(args, v.SystemConvertTo, v.SystemConvertFrom, v.LabelConvertTo, v.LabelConvertFrom, v.CodeConvertFromFloat, v.BusinessPartner)
			length++
		}
	}
	if length == 0 {
		return nil, nil
	}
	repeat := strings.Repeat("(?,?,?,?,?,?),", length-1) + "(?,?,?,?,?,?)"

	rows, err := p.db.Query(
		`SELECT CodeConversionID, SystemConvertTo, SystemConvertFrom, LabelConvertTo, LabelConvertFrom,
		CodeConvertFromInt, CodeConvertFromFloat, CodeConvertFromString, CodeConvertToInt, CodeConvertToFloat, CodeConvertToString, BusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_code_conversion_code_conversion_data
		WHERE (SystemConvertTo, SystemConvertFrom, LabelConvertTo, LabelConvertFrom, CodeConvertFromFloat, BusinessPartner) IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res, err = p.ConvertToCodeConversionQueryGets(rows, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (p *ProcessingFormatter) CodeConversionFromString(dataKey []*ConversionProcessingKey, res []*ConversionProcessingCommonQueryGets) ([]*ConversionProcessingCommonQueryGets, error) {
	var args []interface{}

	length := 0
	for _, v := range dataKey {
		if v.CodeConvertFromString != nil {
			args = append(args, v.SystemConvertTo, v.SystemConvertFrom, v.LabelConvertTo, v.LabelConvertFrom, v.CodeConvertFromString, v.BusinessPartner)
			length++
		}
	}
	if length == 0 {
		return nil, nil
	}
	repeat := strings.Repeat("(?,?,?,?,?,?),", length-1) + "(?,?,?,?,?,?)"

	rows, err := p.db.Query(
		`SELECT CodeConversionID, SystemConvertTo, SystemConvertFrom, LabelConvertTo, LabelConvertFrom,
		CodeConvertFromInt, CodeConvertFromFloat, CodeConvertFromString, CodeConvertToInt, CodeConvertToFloat, CodeConvertToString, BusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_code_conversion_code_conversion_data
		WHERE (SystemConvertTo, SystemConvertFrom, LabelConvertTo, LabelConvertFrom, CodeConvertFromString, BusinessPartner) IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res, err = p.ConvertToCodeConversionQueryGets(rows, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (p *ProcessingFormatter) ConvertToCodeConversionQueryGets(rows *sql.Rows, res []*ConversionProcessingCommonQueryGets) ([]*ConversionProcessingCommonQueryGets, error) {
	defer rows.Close()

	i := 0
	for rows.Next() {
		i++
		pm := &requests.ConversionProcessingCommonQueryGets{}

		err := rows.Scan(
			&pm.CodeConversionID,
			&pm.SystemConvertTo,
			&pm.SystemConvertFrom,
			&pm.LabelConvertTo,
			&pm.LabelConvertFrom,
			&pm.CodeConvertFromInt,
			&pm.CodeConvertFromFloat,
			&pm.CodeConvertFromString,
			&pm.CodeConvertToInt,
			&pm.CodeConvertToFloat,
			&pm.CodeConvertToString,
			&pm.BusinessPartner,
		)
		if err != nil {
			return nil, err
		}

		data := pm
		res = append(res, &ConversionProcessingCommonQueryGets{
			CodeConversionID:      data.CodeConversionID,
			SystemConvertTo:       data.SystemConvertTo,
			SystemConvertFrom:     data.SystemConvertFrom,
			LabelConvertTo:        data.LabelConvertTo,
			LabelConvertFrom:      data.LabelConvertFrom,
			CodeConvertFromInt:    data.CodeConvertFromInt,
			CodeConvertFromFloat:  data.CodeConvertFromFloat,
			CodeConvertFromString: data.CodeConvertFromString,
			CodeConvertToInt:      data.CodeConvertToInt,
			CodeConvertToFloat:    data.CodeConvertToFloat,
			CodeConvertToString:   data.CodeConvertToString,
			BusinessPartner:       data.BusinessPartner,
		})
	}
	if i == 0 {
		return nil, fmt.Errorf("'data_platform_code_conversion_code_conversion_data'テーブルに対象のレコードが存在しません。")
	}

	return res, nil
}
