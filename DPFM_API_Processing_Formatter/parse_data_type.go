package dpfm_api_processing_formatter

import (
	"strconv"
	"time"
)

func parseInt(s string) (int, error) {
	res, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}

	return res, nil
}

func parseIntPtr(s *string) (*int, error) {
	if s == nil {
		return nil, nil
	} else if *s == "" {
		return nil, nil
	}

	res, err := strconv.Atoi(*s)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

func getIntPtr(i int) *int {
	return &i
}

func parseFloat32Ptr(s *string) (*float32, error) {
	if s == nil {
		return nil, nil
	} else if *s == "" {
		return nil, nil
	}

	f, err := strconv.ParseFloat(*s, 32)
	if err != nil {
		return nil, err
	}

	res := float32(f)

	return &res, nil
}

func getFloat32Ptr(f float32) *float32 {
	return &f
}

func parseBoolPtr(s *string) (*bool, error) {
	if s == nil {
		return nil, nil
	} else if *s == "" {
		return nil, nil
	}

	res, err := strconv.ParseBool(*s)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

func getBoolPtr(b bool) *bool {
	return &b
}

func getSystemDatePtr() *string {
	day := time.Now()
	res := day.Format("2006-01-02")

	return &res
}

func getStringPtr(s string) *string {
	return &s
}
