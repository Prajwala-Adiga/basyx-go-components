//go:build integration
// +build integration

package persistence_s3

import (
	"context"
	"flag"
	"log"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	model "github.com/eclipse-basyx/basyx-go-sdk/pkg/discoveryapi/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testEndpoint  = flag.String("endpoint", "http://localhost:9000", "S3 endpoint")
	testRegion    = flag.String("region", "eu-central-1", "S3 region")
	testBucket    = flag.String("bucket", "basyx-test", "S3 bucket")
	testPrefix    = flag.String("prefix", "aas_discovery", "S3 prefix")
	testAccessKey = flag.String("access-key", "minioadmin", "S3 access key")
	testSecretKey = flag.String("secret-key", "minioadmin", "S3 secret key")
)

func TestS3AasDiscoveryBackend(t *testing.T) {
	flag.Parse()

	// Use the flag values
	endpoint := *testEndpoint
	region := *testRegion
	bucket := *testBucket
	prefix := *testPrefix
	accessKey := *testAccessKey
	secretKey := *testSecretKey

	if endpoint == "" || bucket == "" {
		t.Fatal("Missing required flags for S3 test")
	}

	// Create a test-specific bucket to ensure isolation
	testBucketName := bucket + "-" + randString(8)

	// Create an S3 client for setup/teardown operations
	ctx := context.Background()

	// Load AWS S3 configuration
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	require.NoError(t, err)

	// Create S3 client with custom endpoint
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.EndpointResolverV2 = &customS3EndpointResolver{
			endpoint: endpoint,
			region:   region,
		}
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})

	// Ensure the test bucket doesn't exist already (cleanup from potential previous failed tests)
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(testBucketName),
	})

	if err == nil {
		// If bucket exists, delete it first
		log.Printf("Test bucket %s already exists, deleting it first", testBucketName)
		cleanupBucket(t, client, testBucketName)
	}

	// Create the test bucket
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucketName),
	})
	require.NoError(t, err, "Failed to create test bucket")

	_, err = client.ListBuckets(ctx, &s3.ListBucketsInput{})
	t.Logf("S3 connection test result: %v", err)

	// Cleanup bucket after test completes
	defer func() {
		log.Printf("Cleaning up test bucket %s", testBucketName)
		cleanupBucket(t, client, testBucketName)
	}()

	// Initialize backend using the test bucket
	backend, err := NewS3AasDiscoveryBackend(
		context.Background(),
		region,
		endpoint,
		testBucketName,
		prefix,
		accessKey,
		secretKey,
		1,
	)
	require.NoError(t, err)
	require.NotNil(t, backend)

	// Test health check
	assert.True(t, backend.IsS3Healthy(context.Background()))

	// Clean up any leftover test data before running tests
	cleanupTestData(t, backend, prefix)

	// Clean up all test data after tests complete
	defer cleanupTestData(t, backend, prefix)

	t.Run("PostLinks", func(t *testing.T) {
		links := []model.SpecificAssetId{
			{
				Name:  "test-asset-id",
				Value: "test-asset-value",
			},
		}

		aasId := "https://example.com/ids/aas/test-aas-1"

		// Post links
		returnedLinks, msg := backend.PostAllAssetLinksById(aasId, links)
		assert.Equal(t, "201", msg.Code)
		assert.Equal(t, links, returnedLinks)

		// Try posting again (should fail with conflict)
		_, msg = backend.PostAllAssetLinksById(aasId, links)
		assert.Equal(t, "409", msg.Code)
	})

	t.Run("GetLinks", func(t *testing.T) {
		aasId := "https://example.com/ids/aas/test-aas-1"

		links, msg := backend.GetAllAssetLinksById(aasId)
		assert.Equal(t, "200", msg.Code)
		assert.Equal(t, 1, len(links))
		// Add a check before accessing the array element to avoid panic
		if len(links) > 0 {
			assert.Equal(t, "test-asset-id", links[0].Name)
		}

		// Try getting non-existent links
		_, msg = backend.GetAllAssetLinksById("non-existent-id")
		assert.Equal(t, "404", msg.Code)
	})

	t.Run("GetAasIdsByAssetLink", func(t *testing.T) {
		// Add another AAS with the same asset ID to test multiple results
		links := []model.SpecificAssetId{
			{
				Name:  "test-asset-id",
				Value: "test-asset-value",
			},
		}
		aasId1 := "https://example.com/ids/aas/test-aas-1"
		aasId2 := "https://example.com/ids/aas/test-aas-2"

		// Make sure the second AAS is posted successfully
		returnedLinks, postMsg := backend.PostAllAssetLinksById(aasId2, links)
		require.Equal(t, "201", postMsg.Code)
		require.Equal(t, links, returnedLinks)

		// Test getting by asset ID - check both AAS IDs are returned
		ids, msg, cursor := backend.GetAllAssetAdministrationShellIdsByAssetLink(links, 10, "")
		assert.Equal(t, "200", msg.Code)
		assert.Equal(t, 2, len(ids), "Expected to find exactly 2 AAS IDs")
		assert.Contains(t, ids, aasId1)
		assert.Contains(t, ids, aasId2)
		assert.Equal(t, "", cursor)

		// Test with no matching asset IDs
		nonMatchingIds, msg, _ := backend.GetAllAssetAdministrationShellIdsByAssetLink(
			[]model.SpecificAssetId{{Name: "nonexistent", Value: "value"}},
			10,
			"",
		)
		assert.Equal(t, "404", msg.Code)
		assert.Equal(t, 0, len(nonMatchingIds))

		// Test with pagination (limit 1)
		firstPageIds, msg, cursor := backend.GetAllAssetAdministrationShellIdsByAssetLink(links, 1, "")
		if msg.Code != "200" {
			t.Fatalf("Failed to get first page: %s - %s", msg.Code, msg.Text)
		}
		assert.Equal(t, "200", msg.Code)
		assert.Equal(t, 1, len(firstPageIds), "Expected exactly 1 result in first page")
		assert.NotEqual(t, "", cursor, "Expected cursor to be non-empty for pagination")

		if cursor == "" {
			t.Fatalf("Cursor is empty, cannot test pagination")
		}

		// Test with cursor to get second page
		secondPageIds, msg, nextCursor := backend.GetAllAssetAdministrationShellIdsByAssetLink(links, 10, cursor)
		assert.Equal(t, "200", msg.Code)
		assert.Equal(t, 1, len(secondPageIds), "Expected exactly 1 result in second page")
		assert.Equal(t, "", nextCursor, "Expected no further cursor on final page")

		// Verify first and second page contain different IDs
		assert.NotEqual(t, firstPageIds[0], secondPageIds[0],
			"First and second page should return different AAS IDs")

		// Verify that all results from both pages match our original full results
		combinedResults := append(firstPageIds, secondPageIds...)
		assert.ElementsMatch(t, ids, combinedResults,
			"Combined paginated results should match full results")
	})

	t.Run("DeleteLinks", func(t *testing.T) {
		aasId := "https://example.com/ids/aas/test-aas-1"

		// First, post some links to ensure the object exists
		links := []model.SpecificAssetId{
			{
				Name:  "test-delete-asset-id",
				Value: "test-delete-asset-value",
			},
		}

		// Ensure the links exist before attempting deletion
		_, postMsg := backend.PostAllAssetLinksById(aasId, links)
		if postMsg.Code != "201" && postMsg.Code != "409" {
			t.Fatalf("Failed to create test data for delete test: %s", postMsg.Text)
		}

		// Now delete the links
		msg := backend.DeleteAllAssetLinksById(aasId)
		assert.Equal(t, "204", msg.Code, "Should successfully delete existing links")

		// Verify it's deleted
		_, getMsg := backend.GetAllAssetLinksById(aasId)
		assert.Equal(t, "404", getMsg.Code, "Links should be deleted")

		// Try deleting again (should fail with not found)
		msg = backend.DeleteAllAssetLinksById(aasId)
		assert.Equal(t, "404", msg.Code, "Deleting non-existent links should return 404")
	})

	t.Run("DeleteNonExistentLinks", func(t *testing.T) {
		// Try to delete a link that definitely doesn't exist
		nonExistentId := "non-existent-id-" + randString(8)
		msg := backend.DeleteAllAssetLinksById(nonExistentId)
		assert.Equal(t, "404", msg.Code, "Deleting non-existent links should return 404")
	})

}

// Helper function to clean up all objects in a bucket and then delete the bucket
func cleanupBucket(t *testing.T, client *s3.Client, bucketName string) {
	ctx := context.Background()

	// List and delete all objects in the bucket
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})

	// First delete all objects
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Logf("Error listing objects: %v", err)
			return
		}

		for _, obj := range page.Contents {
			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			})
			if err != nil {
				t.Logf("Error deleting object %s: %v", *obj.Key, err)
			}
		}
	}

	// Then delete the bucket
	_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Logf("Error deleting bucket %s: %v", bucketName, err)
	}
}

// Helper function to clean up test data
func cleanupTestData(t *testing.T, backend *S3AasDiscoveryBackend, prefix string) {
	ctx := context.Background()

	// Create a paginator to list all objects with the given prefix
	paginator := s3.NewListObjectsV2Paginator(backend.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(backend.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		require.NoError(t, err)

		// Delete each object in the current page
		for _, obj := range page.Contents {
			_, err := backend.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(backend.bucket),
				Key:    obj.Key,
			})
			require.NoError(t, err)
		}
	}
}

// Helper function to generate a random string
func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[i%len(letters)]
	}
	return string(b)
}
