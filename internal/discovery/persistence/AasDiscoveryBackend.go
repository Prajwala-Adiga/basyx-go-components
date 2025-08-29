package persistence

import model "github.com/eclipse-basyx/basyx-go-sdk/pkg/discoveryapi/go"

type AasDiscoveryBackend interface {
	GetAllAssetAdministrationShellIdsByAssetLink(assetIds []model.SpecificAssetId, limit int32, cursor string) ([]string, model.Message, string)
	GetAllAssetLinksById(aasIdentifier string) ([]model.SpecificAssetId, model.Message)
	PostAllAssetLinksById(aasIdentifier string, specificAssetId []model.SpecificAssetId) ([]model.SpecificAssetId, model.Message)
	DeleteAllAssetLinksById(aasIdentifier string) model.Message
}
