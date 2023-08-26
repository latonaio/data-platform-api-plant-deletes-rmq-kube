package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToGeneral(rows *sql.Rows) (*General, error) {
	defer rows.Close()
	general := General{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&general.BusinessPartner,
			&general.Plant,
			&general.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &general, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &general, nil
	}

	return &general, nil
}

func ConvertToStorageLocation(rows *sql.Rows) (*StorageLocation, error) {
	defer rows.Close()
	storageLocation := StorageLocation{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&storageLocation.BusinessPartner,
			&storageLocation.Plant,
			&storageLocation.StorageLocation,
			&storageLocation.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &storageLocation, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &storageLocation, nil
	}

	return &storageLocation, nil
}
