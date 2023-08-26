package requests

type StorageLocation struct {
	BusinessPartner     int    `json:"BusinessPartner"`
	Plant               string `json:"Plant"`
	StorageLocation     string `json:"StorageLocation"`
	IsMarkedForDeletion *bool  `json:"IsMarkedForDeletion"`
}
