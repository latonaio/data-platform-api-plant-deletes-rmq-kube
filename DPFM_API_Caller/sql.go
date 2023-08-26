package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-plant-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-plant-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"strings"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) GeneralRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.General {

	where := strings.Join([]string{
		fmt.Sprintf("WHERE general.BusinessPartner = %d ", input.General.BusinessPartner),
		fmt.Sprintf("AND general.Plant = \"%s\" ", input.General.Plant),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	general.BusinessPartner
		general.Plant
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_plant_general_data as general 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToGeneral(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) StorageLocation(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.StorageLocation {

	where := strings.Join([]string{
		fmt.Sprintf("WHERE storageLocation.BusinessPartner = %d ", input.StorageLocation.BusinessPartner),
		fmt.Sprintf("AND storageLocation.Plant = \"%s\" ", input.StorageLocation.Plant),
		fmt.Sprintf("AND storageLocation.StorageLocation = \"%s\" ", input.StorageLocation.StorageLocation),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	general.BusinessPartner
		general.Plant
		general.StorageLocation
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_plant_storageLocation_data as storageLocation 
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_plant_general_data as general
		ON general.Plant = storageLocation.Plant ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToStorageLocation(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
