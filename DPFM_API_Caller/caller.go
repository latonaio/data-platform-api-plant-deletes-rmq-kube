package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-plant-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-plant-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-plant-deletes-rmq-kube/config"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type DPFMAPICaller struct {
	ctx  context.Context
	conf *config.Conf
	rmq  *rabbitmq.RabbitmqClient
	db   *database.Mysql
}

func NewDPFMAPICaller(
	conf *config.Conf, rmq *rabbitmq.RabbitmqClient, db *database.Mysql,
) *DPFMAPICaller {
	return &DPFMAPICaller{
		ctx:  context.Background(),
		conf: conf,
		rmq:  rmq,
		db:   db,
	}
}

func (c *DPFMAPICaller) AsyncDeletes(
	accepter []string,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (interface{}, []error) {
	var response interface{}
	if input.APIType == "deletes" {
		response = c.deleteSqlProcess(input, output, accepter, log)
	}

	return response, nil
}

func (c *DPFMAPICaller) deleteSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	log *logger.Logger,
) *dpfm_api_output_formatter.Message {
	var generalData *dpfm_api_output_formatter.General
	storageLocationData := make([]dpfm_api_output_formatter.StorageLocation, 0)
	for _, a := range accepter {
		switch a {
		case "General":
			h, i := c.generalDelete(input, output, log) ///general:g?
			generalData = h
			if h == nil || i == nil {
				continue
			}
			storageLocationData = append(storageLocationData, *i...)
		case "StorageLocation":
			i := c.storageLocationDelete(input, output, log)
			if i == nil {
				continue
			}
			storageLocationData = append(storageLocationData, *i...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		General:         generalData,
		StorageLocation: &storageLocationData,
	}
}

func (c *DPFMAPICaller) generalDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.General {
	sessionID := input.RuntimeSessionID
	general := c.GeneralRead(input, log)
	general.BusinessPartner = input.General.BusinessPartner
	general.Plant = input.General.Plant
	general.IsMarkedForDeletion = input.General.IsMarkedForDeletion
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "PlantGeneral", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "General Data cannot delete"
		return nil
	}

	return general
}

func (c *DPFMAPICaller) storageLocationDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.StorageLocation {
	sessionID := input.RuntimeSessionID
	storageLocation := c.StorageLocationRead(input, log)
	storageLocation.BusinessPartner = input.StorageLocation.BusinessPartner
	storageLocation.Plant = input.StorageLocation.Plant
	storageLocation.StorageLocation = input.StorageLocation.StorageLocation
	storageLocation.IsMarkedForDeletion = input.StorageLocation.IsMarkedForDeletion
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": storageLocation, "function": "PlantStorageLocation", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "StorageLocation Data cannot delete"
		return nil
	}

	return storageLocation
}

func checkResult(msg rabbitmq.RabbitmqMessage) bool {
	data := msg.Data()
	d, ok := data["result"]
	if !ok {
		return false
	}
	result, ok := d.(string)
	if !ok {
		return false
	}
	return result == "success"
}

func getBoolPtr(b bool) *bool {
	return &b
}
