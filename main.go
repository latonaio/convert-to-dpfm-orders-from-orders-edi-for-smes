package main

import (
	"context"
	dpfm_api_input_reader "convert-to-dpfm-orders-from-orders-edi-for-smes/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "convert-to-dpfm-orders-from-orders-edi-for-smes/DPFM_API_Output_Formatter"
	dpfm_api_processing_formatter "convert-to-dpfm-orders-from-orders-edi-for-smes/DPFM_API_Processing_Formatter"
	"convert-to-dpfm-orders-from-orders-edi-for-smes/config"
	"fmt"
	"time"

	database "github.com/latonaio/golang-mysql-network-connector"
	"golang.org/x/xerrors"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
)

func main() {
	ctx := context.Background()
	l := logger.NewLogger()
	c := config.NewConf()
	db, err := database.NewMySQL(c.DB)
	if err != nil {
		l.Error(err)
		return
	}
	defer db.Close()

	rmq, err := rabbitmq.NewRabbitmqClient(c.RMQ.URL(), c.RMQ.QueueFrom(), "", c.RMQ.QueueTo(), 0)
	if err != nil {
		l.Fatal(err.Error())
	}
	iter, err := rmq.Iterator()
	if err != nil {
		l.Fatal(err.Error())
	}
	defer rmq.Stop()

	for msg := range iter {
		start := time.Now()
		sdc, err := callProcess(ctx, db, rmq, msg, c)
		if err != nil {
			msg.Fail()
			l.Error(err)
			continue
		}
		l.JsonParseOut(sdc)
		msg.Success()
		l.Info("process time %v\n", time.Since(start).Milliseconds())
	}
}

func getSessionID(data map[string]interface{}) string {
	id := fmt.Sprintf("%v", data["runtime_session_id"])
	return id
}

func callProcess(ctx context.Context, db *database.Mysql, rmq *rabbitmq.RabbitmqClient, msg rabbitmq.RabbitmqMessage, c *config.Conf) (dpfm_api_output_formatter.Output, error) {
	var err error
	l := logger.NewLogger()
	defer func() {
		if e := recover(); e != nil {
			err = xerrors.Errorf("error occurred: %w", e)
			return
		}
	}()
	l.AddHeaderInfo(map[string]interface{}{"runtime_session_id": getSessionID(msg.Data())})

	processingFormatter := dpfm_api_processing_formatter.NewProcessingFormatter(ctx, db, l)

	sdc := dpfm_api_input_reader.ConvertToSDC(msg.Raw())
	psdc := dpfm_api_processing_formatter.ConvertToProcessingFormatter()
	osdc := dpfm_api_output_formatter.ConvertToOutput(msg.Raw())

	err = processingFormatter.ProcessingFormatter(&sdc, &psdc)
	if err != nil {
		osdc.APIProcessingResult = getBoolPtr(false)
		osdc.APIProcessingError = err.Error()
		rmq.Send(c.RMQ.QueueToErrResponse(), osdc)
		return osdc, err
	}

	err = dpfm_api_output_formatter.OutputFormatter(&sdc, &psdc, &osdc)
	if err != nil {
		osdc.APIProcessingResult = getBoolPtr(false)
		osdc.APIProcessingError = err.Error()
		rmq.Send(c.RMQ.QueueToErrResponse(), osdc)
		return osdc, err
	}

	rmq.Send(c.RMQ.QueueTo()[0], osdc)

	return osdc, nil
}

func getBoolPtr(b bool) *bool {
	return &b
}

func getAccepter(input *dpfm_api_input_reader.SDC) []string {
	accepter := input.Accepter
	if len(input.Accepter) == 0 {
		accepter = []string{"All"}
	}

	if accepter[0] == "All" {
		accepter = []string{
			"Header", "Item",
		}
	}
	return accepter
}
