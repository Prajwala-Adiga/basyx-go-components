package persistence_inmemory

import (
	"sort"

	base64url "github.com/eclipse-basyx/basyx-go-sdk/internal/common"
	model "github.com/eclipse-basyx/basyx-go-sdk/pkg/discoveryapi/go"
)

type InMemoryAasDiscoveryBackend struct {
	links map[string][]model.SpecificAssetId
}

func NewInMemoryAasDiscoveryBackend() (*InMemoryAasDiscoveryBackend, error) {
	return &InMemoryAasDiscoveryBackend{
		links: make(map[string][]model.SpecificAssetId),
	}, nil
}

func (db *InMemoryAasDiscoveryBackend) GetAllAssetAdministrationShellIdsByAssetLink(assetIds []model.SpecificAssetId, limit int32, cursor string) ([]string, model.Message, string) {
	// Apply default limit if necessary
	if limit <= 0 {
		limit = 100
	}

	// Validate cursor format if it's not empty
	if cursor != "" {
		_, err := base64url.DecodeString(cursor)
		if err != nil {
			return nil, model.Message{
				Code:        "400",
				Text:        "Invalid cursor format - " + err.Error(),
				MessageType: "Error",
			}, ""
		}
	}

	// Find matching AAS IDs based on provided asset IDs
	matchingAasIds := make(map[string]bool)

	// If assetIds is empty, return all AAS IDs
	if len(assetIds) == 0 {
		for aasId := range db.links {
			matchingAasIds[aasId] = true
		}
	} else {
		// Otherwise filter by the provided asset IDs
		for aasId, specificAssetIds := range db.links {
			for _, linkedAssetId := range specificAssetIds {
				for _, queryAssetId := range assetIds {
					if linkedAssetId.Name == queryAssetId.Name && linkedAssetId.Value == queryAssetId.Value {
						matchingAasIds[aasId] = true
						break
					}
				}
				if matchingAasIds[aasId] {
					break
				}
			}
		}
	}

	// Convert map to sorted slice for consistent ordering
	result := make([]string, 0, len(matchingAasIds))
	for aasId := range matchingAasIds {
		result = append(result, aasId)
	}

	// No results found
	if len(result) == 0 {
		return nil, model.Message{
			Code:        "404",
			Text:        "No Asset Administration Shell Ids found",
			MessageType: "Error",
		}, ""
	}

	// Sort for consistent ordering
	sort.Strings(result)

	// Apply cursor-based pagination
	startIdx := 0
	if cursor != "" {
		cursorData, _ := base64url.DecodeString(cursor) // Already validated above
		for i, id := range result {
			if id > cursorData {
				startIdx = i
				break
			}
		}
	}

	// Apply pagination
	if startIdx < len(result) {
		result = result[startIdx:]
	} else {
		result = []string{}
	}

	// Check if we need to return a cursor for the next page
	var nextCursor string
	if int32(len(result)) > limit {
		// The cursor points to the LAST item on the current page
		nextCursor = base64url.EncodeString(result[limit-1])
		result = result[:limit] // Truncate results to the requested limit
	}

	return result, model.Message{
		Code:        "200",
		Text:        "OK",
		MessageType: "Info",
	}, nextCursor
}

func (db *InMemoryAasDiscoveryBackend) GetAllAssetLinksById(aasIdentifier string) ([]model.SpecificAssetId, model.Message) {
	if aasIdentifier == "" {
		return nil, model.Message{
			Code:        "400",
			Text:        "Bad Request: aasIdentifier cannot be empty",
			MessageType: "Error",
		}
	}

	links, exists := db.links[aasIdentifier]
	if !exists {
		return nil, model.Message{
			Code:        "404",
			Text:        "Asset Link for shell with id " + aasIdentifier + " not found",
			MessageType: "Error",
		}
	}

	return links, model.Message{
		Code:        "200",
		Text:        "OK",
		MessageType: "Info",
	}
}

func (db *InMemoryAasDiscoveryBackend) PostAllAssetLinksById(aasIdentifier string, specificAssetId []model.SpecificAssetId) ([]model.SpecificAssetId, model.Message) {
	if aasIdentifier == "" {
		return nil, model.Message{
			Code:        "400",
			Text:        "Bad Request: AAS identifier cannot be empty",
			MessageType: "Error",
		}
	}

	if specificAssetId == nil {
		return nil, model.Message{
			Code:        "400",
			Text:        "Bad Request: Asset IDs cannot be null",
			MessageType: "Error",
		}
	}

	// Check if entry already exists (optional for conflict handling)
	_, exists := db.links[aasIdentifier]
	if exists {
		return nil, model.Message{
			Code:        "409",
			Text:        "Conflict: Link with the given AAS Id already exists",
			MessageType: "Error",
		}
	}

	db.links[aasIdentifier] = specificAssetId

	return specificAssetId, model.Message{
		Code:        "201",
		Text:        "Created",
		MessageType: "Info",
	}
}

func (db *InMemoryAasDiscoveryBackend) DeleteAllAssetLinksById(aasIdentifier string) model.Message {
	if aasIdentifier == "" {
		return model.Message{
			Code:        "400",
			Text:        "Bad Request: aasIdentifier cannot be empty",
			MessageType: "Error",
		}
	}

	if _, exists := db.links[aasIdentifier]; !exists {
		return model.Message{
			Code:        "404",
			Text:        "Asset Link for shell with id " + aasIdentifier + " not found",
			MessageType: "Error",
		}
	}

	delete(db.links, aasIdentifier)

	return model.Message{
		Code:        "204",
		Text:        "No Content",
		MessageType: "Info",
	}
}
