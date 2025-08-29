package persistence_mongodb

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	base64url "github.com/eclipse-basyx/basyx-go-sdk/internal/common"
	model "github.com/eclipse-basyx/basyx-go-sdk/pkg/discoveryapi/go"
)

type MongoDBAasDiscoveryBackend struct {
	client     *mongo.Client
	collection *mongo.Collection
}

func NewMongoDBAasDiscoveryBackend(ctx context.Context, connectionUri string, dbName string, collectionName string) (*MongoDBAasDiscoveryBackend, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionUri))
	if err != nil {
		return nil, err
	}
	collection := client.Database(dbName).Collection(collectionName)
	return &MongoDBAasDiscoveryBackend{
		client:     client,
		collection: collection,
	}, nil
}

func (db *MongoDBAasDiscoveryBackend) GetAllAssetAdministrationShellIdsByAssetLink(assetIds []model.SpecificAssetId, limit int32, cursor string) ([]string, model.Message, string) {
	if limit <= 0 {
		limit = 100
	}

	var filter map[string]interface{}
	if len(assetIds) > 0 {
		filter = map[string]interface{}{"links": map[string]interface{}{"$in": assetIds}}
	} else {
		filter = map[string]interface{}{}
	}

	if cursor != "" {
		cursorData, err := base64url.DecodeString(cursor)
		if err != nil {
			return nil, model.Message{
				Code:        "400",
				Text:        "Invalid cursor format - " + err.Error(),
				MessageType: "Error",
			}, ""
		}

		if cursorData != "" {
			filter["_id"] = map[string]interface{}{"$gt": cursorData}
		}
	}

	findOptions := options.Find().SetLimit(int64(limit + 1))
	findOptions.SetSort(map[string]interface{}{"_id": 1}) // Ensure consistent ordering

	mongoCursor, err := db.collection.Find(context.Background(), filter, findOptions)
	if err != nil {
		return nil, model.Message{
			Code:        "500",
			Text:        "MongoDB Error - " + err.Error(),
			MessageType: "Error",
		}, ""
	}
	defer mongoCursor.Close(context.Background())

	var aasIds []string
	var nextCursor string
	count := 0
	var lastResult struct {
		ID     string   `bson:"_id"`
		AasIds []string `bson:"aasIds"`
	}

	for mongoCursor.Next(context.Background()) {
		if count >= int(limit) {
			cursorData := lastResult.ID
			nextCursor = base64url.EncodeString(cursorData)
			break
		}

		lastResult = struct {
			ID     string   `bson:"_id"`
			AasIds []string `bson:"aasIds"`
		}{}

		if err := mongoCursor.Decode(&lastResult); err != nil {
			return nil, model.Message{
				Code:        "500",
				Text:        "MongoDB Error while decoding - " + err.Error(),
				MessageType: "Error",
			}, ""
		}

		aasIds = append(aasIds, lastResult.ID)
		count++
	}

	if err := mongoCursor.Err(); err != nil {
		return nil, model.Message{
			Code:        "500",
			Text:        "MongoDB Error during cursor iteration - " + err.Error(),
			MessageType: "Error",
		}, ""
	}

	if len(aasIds) == 0 {
		return nil, model.Message{
			Code:        "404",
			Text:        "No Asset Administration Shell Ids found",
			MessageType: "Error",
		}, ""
	}

	return aasIds, model.Message{
		Code:        "200",
		Text:        "OK",
		MessageType: "Info",
	}, nextCursor
}

func (db *MongoDBAasDiscoveryBackend) GetAllAssetLinksById(aasIdentifier string) ([]model.SpecificAssetId, model.Message) {
	if aasIdentifier == "" {
		return nil, model.Message{
			Code:        "400",
			Text:        "Bad Request: aasIdentifier cannot be empty",
			MessageType: "Error",
		}
	}

	filter := map[string]interface{}{"_id": aasIdentifier}
	var result struct {
		Links []model.SpecificAssetId `json:"links"`
	}
	err := db.collection.FindOne(context.Background(), filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, model.Message{
				Code:        "404",
				Text:        "Asset Link for shell with id " + aasIdentifier + " not found",
				MessageType: "Error",
			}
		}
		return nil, model.Message{
			Code:        "500",
			Text:        "MongoDB Error - " + err.Error(),
			MessageType: "Error",
		}
	}
	return result.Links, model.Message{
		Code:        "200",
		Text:        "OK",
		MessageType: "Info",
	}
}

func (db *MongoDBAasDiscoveryBackend) PostAllAssetLinksById(aasIdentifier string, specificAssetId []model.SpecificAssetId) ([]model.SpecificAssetId, model.Message) {
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

	doc := map[string]interface{}{
		"_id":   aasIdentifier,
		"links": specificAssetId,
	}

	_, err := db.collection.InsertOne(context.Background(), doc)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key error") {
			return nil, model.Message{
				Code:        "409",
				Text:        "Conflict: Link with the given AAS Id already exists",
				MessageType: "Error",
			}
		} else {
			return nil, model.Message{
				Code:        "500",
				Text:        "MongoDB Error - " + err.Error(),
				MessageType: "Error",
			}
		}
	}
	return specificAssetId, model.Message{
		Code:        "201",
		Text:        "Created",
		MessageType: "Info",
	}
}

func (db *MongoDBAasDiscoveryBackend) DeleteAllAssetLinksById(aasIdentifier string) model.Message {
	if aasIdentifier == "" {
		return model.Message{
			Code:        "400",
			Text:        "Bad Request: aasIdentifier cannot be empty",
			MessageType: "Error",
		}
	}

	filter := map[string]interface{}{"_id": aasIdentifier}

	var result struct{}
	err := db.collection.FindOneAndDelete(context.Background(), filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return model.Message{
				Code:        "404",
				Text:        "Asset Link for shell with id " + aasIdentifier + " not found",
				MessageType: "Error",
			}
		}

		return model.Message{
			Code:        "500",
			Text:        "MongoDB Error - " + err.Error(),
			MessageType: "Error",
		}
	}

	return model.Message{
		Code:        "204",
		Text:        "No Content",
		MessageType: "Info",
	}
}

func (db *MongoDBAasDiscoveryBackend) Close() {
	db.client.Disconnect(context.Background())
}

func (db *MongoDBAasDiscoveryBackend) IsMongoDBHealthy() bool {
	err := db.client.Ping(context.Background(), nil)
	return err == nil
}
