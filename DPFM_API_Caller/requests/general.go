package requests

type General struct {
	BusinessPartner     int    `json:"BusinessPartner"`
	Plant               string `json:"Plant"`
	IsMarkedForDeletion *bool  `json:"IsMarkedForDeletion"`
}
