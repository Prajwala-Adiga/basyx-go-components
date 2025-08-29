//go:build integration
// +build integration

package persistence_mongodb

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	model "github.com/eclipse-basyx/basyx-go-sdk/pkg/discoveryapi/go"
)

var (
	testMongoURI        = flag.String("mongo-uri", "mongodb://mongoAdmin:mongoPassword@localhost:27017", "MongoDB connection URI")
	testMongoDatabase   = flag.String("mongo-db", "basyx_test", "MongoDB database name")
	testMongoCollection = flag.String("mongo-collection", "aas_discovery", "MongoDB collection name")
)

func TestMongoDBDiscoveryBackend(t *testing.T) {
	flag.Parse()

	// Use the flag values
	mongoURI := *testMongoURI
	database := *testMongoDatabase
	collection := *testMongoCollection

	if mongoURI == "" || database == "" || collection == "" {
		t.Fatal("Missing required flags for MongoDB test")
	}

	// Create a unique collection name for this test to ensure isolation
	testCollectionName := collection + "_" + randString(8)

	// Create a MongoDB client for setup/teardown operations
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer client.Disconnect(ctx)

	// Test connection
	err = client.Ping(ctx, nil)
	require.NoError(t, err, "Failed to ping MongoDB server")

	// Drop the test collection if it exists (cleanup from previous failed runs)
	client.Database(database).Collection(testCollectionName).Drop(ctx)

	// Create the test collection
	err = client.Database(database).CreateCollection(ctx, testCollectionName)
	require.NoError(t, err, "Failed to create test collection")

	// Initialize backend using the test collection
	backend, err := NewMongoDBAasDiscoveryBackend(
		ctx,
		mongoURI,
		database,
		testCollectionName,
	)
	require.NoError(t, err)
	require.NotNil(t, backend)

	// Ensure cleanup after tests
	defer func() {
		client.Database(database).Collection(testCollectionName).Drop(ctx)
		backend.Close()
	}()

	// Test health check
	assert.True(t, backend.IsMongoDBHealthy())

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

		// Verify links exist before deletion
		links, getMsg := backend.GetAllAssetLinksById(aasId)
		require.Equal(t, "200", getMsg.Code, "Links should exist before deletion test")
		require.Greater(t, len(links), 0, "Should have at least one link to test deletion")

		// Now delete the links
		msg := backend.DeleteAllAssetLinksById(aasId)
		assert.Equal(t, "204", msg.Code, "Should successfully delete existing links")

		// Verify it's deleted
		_, getMsg = backend.GetAllAssetLinksById(aasId)
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

	t.Run("BadInputs", func(t *testing.T) {
		// Test empty AAS ID for POST
		_, msg := backend.PostAllAssetLinksById("", []model.SpecificAssetId{})
		assert.Equal(t, "400", msg.Code)

		// Test nil asset IDs for POST
		_, msg = backend.PostAllAssetLinksById("some-id", nil)
		assert.Equal(t, "400", msg.Code)

		// Test empty AAS ID for GET
		_, msg = backend.GetAllAssetLinksById("")
		assert.Equal(t, "400", msg.Code)

		// Test empty AAS ID for DELETE
		msg = backend.DeleteAllAssetLinksById("")
		assert.Equal(t, "400", msg.Code)
	})
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
