package dpfm_api_output_formatter

import (
	"encoding/json"
	"fmt"
	"os"
)

func ConvertToOutput(data []byte) Output {
	output := Output{}
	err := json.Unmarshal(data, &output)
	if err != nil {
		fmt.Printf("input data marshal error :%#v", err.Error())
		os.Exit(1)
	}

	return output
}
