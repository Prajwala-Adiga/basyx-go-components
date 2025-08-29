package persistence_s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyendpoints "github.com/aws/smithy-go/endpoints"

	base64url "github.com/eclipse-basyx/basyx-go-sdk/internal/common"
	model "github.com/eclipse-basyx/basyx-go-sdk/pkg/discoveryapi/go"
)

type S3AasDiscoveryBackend struct {
	client       *s3.Client
	bucket       string
	prefix       string
	cacheMinutes int
	cache        map[string]cacheEntry
}

type cacheEntry struct {
	data      []byte
	timestamp time.Time
}

// NewS3AasDiscoveryBackend creates a new S3 backend for AAS discovery
func NewS3AasDiscoveryBackend(
	ctx context.Context,
	region string,
	endpoint string,
	bucket string,
	prefix string,
	accessKey string,
	secretKey string,
	cacheMinutes int,
) (*S3AasDiscoveryBackend, error) {
	if bucket == "" {
		return nil, errors.New("bucket name cannot be empty")
	}

	// Load AWS S3 configuration
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}

	if accessKey != "" && secretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load S3 configuration: %w", err)
	}

	// Create S3 client with custom endpoint if specified
	clientOpts := []func(*s3.Options){}

	if endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.EndpointResolverV2 = &customS3EndpointResolver{
				endpoint: endpoint,
				region:   region,
			}
		})
	}

	client := s3.NewFromConfig(cfg, clientOpts...)

	// Ensure bucket exists
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		var notFoundErr *types.NotFound
		if errors.As(err, &notFoundErr) {
			// Bucket doesn't exist, create it
			_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(bucket),
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create S3 bucket %s: %w", bucket, err)
			}
		} else {
			return nil, fmt.Errorf("failed to access S3 bucket %s: %w", bucket, err)
		}
	}

	// Ensure prefix ends with a slash
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	return &S3AasDiscoveryBackend{
		client:       client,
		bucket:       bucket,
		prefix:       prefix,
		cacheMinutes: cacheMinutes,
		cache:        make(map[string]cacheEntry),
	}, nil
}

// Custom endpoint resolver for S3
type customS3EndpointResolver struct {
	endpoint string
	region   string
}

// ResolveEndpoint implements the EndpointResolverV2 interface
func (r *customS3EndpointResolver) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	if r.endpoint != "" {
		u, err := url.Parse(r.endpoint)
		if err != nil {
			return smithyendpoints.Endpoint{}, err
		}

		// If bucket name is provided, append it to the path (MinIO style)
		if params.Bucket != nil {
			// Make a copy of the URL to avoid modifying the original
			uCopy := *u
			uCopy.Path = path.Join(uCopy.Path, *params.Bucket)
			return smithyendpoints.Endpoint{
				URI: uCopy,
			}, nil
		}

		// When no bucket is specified (e.g. for ListBuckets)
		return smithyendpoints.Endpoint{
			URI: *u,
		}, nil
	}

	// Fall back to the default resolver if no custom endpoint is specified
	return s3.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, params)
}

// IsS3Healthy checks if the S3 service is healthy
func (db *S3AasDiscoveryBackend) IsS3Healthy(ctx context.Context) bool {
	_, err := db.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(db.bucket),
	})
	return err == nil
}

// encodeAasId encodes an AAS ID to be used as an S3 object key
func (db *S3AasDiscoveryBackend) encodeAasId(aasId string) string {
	return base64url.EncodeString(aasId)
}

// decodeAasId decodes an S3 object key to get the original AAS ID
func (db *S3AasDiscoveryBackend) decodeAasId(encodedAasId string) (string, error) {
	return base64url.DecodeString(encodedAasId)
}

// GetAllAssetAdministrationShellIdsByAssetLink returns all AAS IDs linked to specific asset identifiers
func (db *S3AasDiscoveryBackend) GetAllAssetAdministrationShellIdsByAssetLink(
	assetIds []model.SpecificAssetId,
	limit int32,
	cursor string,
) ([]string, model.Message, string) {
	if limit <= 0 {
		limit = 100
	}

	ctx := context.Background()

	// Create a map to track matching AAS IDs
	matchingAasIds := make(map[string]struct{})

	// Process each provided asset ID
	for _, assetId := range assetIds {
		// List all objects in the bucket with the given prefix
		paginator := s3.NewListObjectsV2Paginator(db.client, &s3.ListObjectsV2Input{
			Bucket: aws.String(db.bucket),
			Prefix: aws.String(db.prefix),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, model.Message{
					Code:        "500",
					Text:        "Error listing S3 objects: " + err.Error(),
					MessageType: "Error",
				}, ""
			}

			// Check each object
			for _, obj := range page.Contents {
				// Skip directories
				if strings.HasSuffix(*obj.Key, "/") {
					continue
				}

				// Extract the encoded AAS ID from the object key
				encodedAasId := strings.TrimPrefix(*obj.Key, db.prefix)

				// Decode the AAS ID from the S3 object key
				aasId, err := db.decodeAasId(encodedAasId)
				if err != nil {
					// Skip invalid encoded IDs
					continue
				}

				// Get the asset links for this AAS
				assetLinks, msg := db.GetAllAssetLinksById(aasId)
				if msg.Code != "200" {
					continue // Skip if we can't get asset links
				}

				// Check if any of the asset links match the requested asset ID
				for _, link := range assetLinks {
					if link.Name == assetId.Name && link.Value == assetId.Value {
						matchingAasIds[aasId] = struct{}{}
						break
					}
				}
			}
		}
	}

	// If no asset IDs were provided, return all AAS IDs
	if len(assetIds) == 0 {
		paginator := s3.NewListObjectsV2Paginator(db.client, &s3.ListObjectsV2Input{
			Bucket: aws.String(db.bucket),
			Prefix: aws.String(db.prefix),
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, model.Message{
					Code:        "500",
					Text:        "Error listing S3 objects: " + err.Error(),
					MessageType: "Error",
				}, ""
			}

			for _, obj := range page.Contents {
				// Skip directories
				if strings.HasSuffix(*obj.Key, "/") {
					continue
				}

				encodedAasId := strings.TrimPrefix(*obj.Key, db.prefix)

				// Skip if this is not an AAS document
				if strings.Contains(encodedAasId, "/") {
					continue
				}

				// Decode the AAS ID
				aasId, err := db.decodeAasId(encodedAasId)
				if err != nil {
					// Skip invalid encoded IDs
					continue
				}

				matchingAasIds[aasId] = struct{}{}
			}
		}
	}

	// Convert map to sorted slice for consistent ordering
	var results []string
	for aasId := range matchingAasIds {
		results = append(results, aasId)
	}

	// Sort for deterministic ordering
	sort.Strings(results)

	// If no matches were found, return 404
	if len(results) == 0 {
		return nil, model.Message{
			Code:        "404",
			Text:        "No Asset Administration Shell Ids found",
			MessageType: "Error",
		}, ""
	}

	// Handle pagination with cursor
	startIndex := 0
	if cursor != "" {
		// The cursor is already encoded aasId
		cursorValue, err := base64url.DecodeString(cursor)
		if err != nil {
			return nil, model.Message{
				Code:        "400",
				Text:        "Invalid cursor format: " + err.Error(),
				MessageType: "Error",
			}, ""
		}

		// Find the position after the cursor
		for i, id := range results {
			if id == cursorValue {
				startIndex = i + 1
				break
			}
		}
	}

	// Apply pagination limits
	endIndex := startIndex + int(limit)
	if endIndex > len(results) {
		endIndex = len(results)
	}

	// Get the subset of results for this page
	pageResults := results[startIndex:endIndex]

	// Create next page cursor if needed
	var nextCursor string
	if endIndex < len(results) {
		// When returning cursor to outside world, it should be base64url encoded
		nextCursor = base64url.EncodeString(results[endIndex-1])
	}

	return pageResults, model.Message{
		Code:        "200",
		Text:        "OK",
		MessageType: "Info",
	}, nextCursor
}

// GetAllAssetLinksById returns all asset links for a specific AAS ID
func (db *S3AasDiscoveryBackend) GetAllAssetLinksById(aasIdentifier string) ([]model.SpecificAssetId, model.Message) {
	if aasIdentifier == "" {
		return nil, model.Message{
			Code:        "400",
			Text:        "Bad Request: aasIdentifier cannot be empty",
			MessageType: "Error",
		}
	}

	ctx := context.Background()

	// Encode the AAS ID for use as S3 object key
	encodedAasId := db.encodeAasId(aasIdentifier)
	objectKey := db.prefix + encodedAasId

	// Check cache first
	if db.cacheMinutes > 0 {
		if entry, found := db.cache[objectKey]; found {
			if time.Since(entry.timestamp).Minutes() < float64(db.cacheMinutes) {
				var links []model.SpecificAssetId
				if err := json.Unmarshal(entry.data, &links); err == nil {
					return links, model.Message{
						Code:        "200",
						Text:        "OK",
						MessageType: "Info",
					}
				}
			}
			// Remove expired/invalid cache entry
			delete(db.cache, objectKey)
		}
	}

	// Get object from S3
	result, err := db.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		var notFound *types.NoSuchKey
		if errors.As(err, &notFound) {
			return nil, model.Message{
				Code:        "404",
				Text:        "Asset Link for shell with id " + aasIdentifier + " not found",
				MessageType: "Error",
			}
		}
		return nil, model.Message{
			Code:        "500",
			Text:        "S3 Error - " + err.Error(),
			MessageType: "Error",
		}
	}
	defer result.Body.Close()

	// Read the object data
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(result.Body); err != nil {
		return nil, model.Message{
			Code:        "500",
			Text:        "Failed to read object data - " + err.Error(),
			MessageType: "Error",
		}
	}
	data := buf.Bytes()

	// Parse the JSON data
	var links []model.SpecificAssetId
	if err := json.Unmarshal(data, &links); err != nil {
		return nil, model.Message{
			Code:        "500",
			Text:        "Failed to parse JSON data - " + err.Error(),
			MessageType: "Error",
		}
	}

	// Update cache
	if db.cacheMinutes > 0 {
		db.cache[objectKey] = cacheEntry{
			data:      data,
			timestamp: time.Now(),
		}
	}

	return links, model.Message{
		Code:        "200",
		Text:        "OK",
		MessageType: "Info",
	}
}

// PostAllAssetLinksById creates asset links for a specific AAS ID
func (db *S3AasDiscoveryBackend) PostAllAssetLinksById(aasIdentifier string, specificAssetId []model.SpecificAssetId) ([]model.SpecificAssetId, model.Message) {
	if aasIdentifier == "" {
		return nil, model.Message{
			Code:        "400",
			Text:        "Bad Request: AAS identifier cannot be empty",
			MessageType: "Error",
		}
	}

	if len(specificAssetId) == 0 {
		return nil, model.Message{
			Code:        "400",
			Text:        "Bad Request: Asset IDs cannot be null or empty",
			MessageType: "Error",
		}
	}

	ctx := context.Background()

	// Encode the AAS ID for use as S3 object key
	encodedAasId := db.encodeAasId(aasIdentifier)
	objectKey := db.prefix + encodedAasId

	// Check if the object already exists
	_, err := db.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(objectKey),
	})
	if err == nil {
		return nil, model.Message{
			Code:        "409",
			Text:        "Conflict: Link with the given AAS Id already exists",
			MessageType: "Error",
		}
	}

	// Convert asset IDs to JSON
	jsonData, err := json.Marshal(specificAssetId)
	if err != nil {
		return nil, model.Message{
			Code:        "500",
			Text:        "Failed to serialize JSON - " + err.Error(),
			MessageType: "Error",
		}
	}

	// Upload to S3
	_, err = db.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(db.bucket),
		Key:         aws.String(objectKey),
		Body:        bytes.NewReader(jsonData),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return nil, model.Message{
			Code:        "500",
			Text:        "S3 Error - " + err.Error(),
			MessageType: "Error",
		}
	}

	// Update cache
	if db.cacheMinutes > 0 {
		db.cache[objectKey] = cacheEntry{
			data:      jsonData,
			timestamp: time.Now(),
		}
	}

	return specificAssetId, model.Message{
		Code:        "201",
		Text:        "Created",
		MessageType: "Info",
	}
}

// DeleteAllAssetLinksById deletes asset links for a specific AAS ID
func (db *S3AasDiscoveryBackend) DeleteAllAssetLinksById(aasIdentifier string) model.Message {
	if aasIdentifier == "" {
		return model.Message{
			Code:        "400",
			Text:        "Bad Request: aasIdentifier cannot be empty",
			MessageType: "Error",
		}
	}

	ctx := context.Background()

	// Encode the AAS ID for use as S3 object key
	encodedAasId := db.encodeAasId(aasIdentifier)
	objectKey := db.prefix + encodedAasId

	// Check if the object exists
	_, err := db.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		// Check specifically for NoSuchKey error
		var notFoundErr *types.NoSuchKey
		if errors.As(err, &notFoundErr) {
			return model.Message{
				Code:        "404",
				Text:        "Asset Link for shell with id " + aasIdentifier + " not found",
				MessageType: "Error",
			}
		}

		// Also check for NotFound error which might be returned by some S3 implementations
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return model.Message{
				Code:        "404",
				Text:        "Asset Link for shell with id " + aasIdentifier + " not found",
				MessageType: "Error",
			}
		}

		return model.Message{
			Code:        "500",
			Text:        "S3 Error - " + err.Error(),
			MessageType: "Error",
		}
	}

	// Delete the object
	_, err = db.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return model.Message{
			Code:        "500",
			Text:        "S3 Error - " + err.Error(),
			MessageType: "Error",
		}
	}

	// Remove from cache if present
	if db.cacheMinutes > 0 {
		delete(db.cache, objectKey)
	}

	return model.Message{
		Code:        "204",
		Text:        "No Content",
		MessageType: "Info",
	}
}
