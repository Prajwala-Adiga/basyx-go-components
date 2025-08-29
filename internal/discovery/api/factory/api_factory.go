package api

import (
	"context"
	"log"
	"strings"
	"time"

	api "github.com/eclipse-basyx/basyx-go-sdk/internal/discovery/api"
	"github.com/eclipse-basyx/basyx-go-sdk/internal/discovery/config"
	persistence "github.com/eclipse-basyx/basyx-go-sdk/internal/discovery/persistence"
	persistence_inmemory "github.com/eclipse-basyx/basyx-go-sdk/internal/discovery/persistence/inmemory"
	persistence_mongodb "github.com/eclipse-basyx/basyx-go-sdk/internal/discovery/persistence/mongodb"
	persistence_s3 "github.com/eclipse-basyx/basyx-go-sdk/internal/discovery/persistence/s3"
	openapi "github.com/eclipse-basyx/basyx-go-sdk/pkg/discoveryapi/go"
)

type ApiFactory struct {
	config *config.Config
}

func NewApiFactory(config *config.Config) *ApiFactory {
	return &ApiFactory{
		config: config,
	}
}

func (f *ApiFactory) Create() (*api.AssetAdministrationShellBasicDiscoveryAPIAPIService, *openapi.AssetAdministrationShellBasicDiscoveryAPIAPIController, func()) {
	var database persistence.AasDiscoveryBackend
	var err error
	var closeFunc func()
	cfg := f.config
	switch strings.ToLower(cfg.BaSyx.Backend) {
	case "inmemory", "":
		database, err = persistence_inmemory.NewInMemoryAasDiscoveryBackend()
		if err != nil {
			log.Fatalf("Failed to initialize in-memory database: %v", err)
		}
		closeFunc = nil
	case "mongodb":
		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Duration(cfg.MongoDB.Timeout)*time.Second,
		)
		defer cancel()
		log.Printf("Connecting to MongoDB at %s", cfg.MongoDB.URI)
		var mongoDb *persistence_mongodb.MongoDBAasDiscoveryBackend
		mongoDb, err = persistence_mongodb.NewMongoDBAasDiscoveryBackend(
			ctx,
			cfg.MongoDB.URI,
			cfg.MongoDB.Database,
			cfg.MongoDB.Collection,
		)
		if err != nil {
			log.Fatalf("Failed to connect to MongoDB: %v", err)
			//os.Exit(1)
		}

		if !mongoDb.IsMongoDBHealthy() {
			log.Fatalf("Could not connect to MongoDB: %s", cfg.MongoDB.URI)
		}

		database = mongoDb
		closeFunc = mongoDb.Close
		log.Printf("Connected to MongoDB.")
	case "s3":
		if cfg.S3.Bucket == "" {
			log.Fatalf("S3 bucket name is required")
		}

		ctx := context.Background()

		s3Backend, err := persistence_s3.NewS3AasDiscoveryBackend(
			ctx,
			cfg.S3.Region,
			cfg.S3.Endpoint,
			cfg.S3.Bucket,
			cfg.S3.Prefix,
			cfg.S3.AccessKey,
			cfg.S3.SecretKey,
			cfg.S3.CacheMinutes,
		)

		if err != nil {
			log.Fatalf("Failed to initialize S3 backend: %v", err)
		}

		database = s3Backend
		closeFunc = nil
		log.Printf("Connected to S3 bucket %s", cfg.S3.Bucket)
	default:
		log.Fatalf("Unknown backend type: %s - Valid Backend Types are: InMemory | MongoDB", cfg.BaSyx.Backend)
	}
	discSvc := api.NewAssetAdministrationShellBasicDiscoveryAPIAPIService(database)
	discCtrl := openapi.NewAssetAdministrationShellBasicDiscoveryAPIAPIController(discSvc)
	return discSvc, discCtrl, closeFunc
}
